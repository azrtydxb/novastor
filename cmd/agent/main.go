package main

import (
	"context"
	"encoding/base64"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/metrics"
	"github.com/piwi3910/novastor/internal/transport"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// logReporter is a simple ScrubReporter that logs corrupt chunk IDs via zap.
type logReporter struct{}

func (logReporter) ReportCorruptChunk(_ context.Context, chunkID chunk.ChunkID) error {
	logging.L.Error("scrub: corrupt chunk detected", zap.String("chunkID", string(chunkID)))
	metrics.ScrubErrors.Inc()
	return nil
}

// diskUsedBytes walks the given directory and returns the total size of all
// regular files found. It is used to compute the disk-used metric from the
// chunk store directory.
func diskUsedBytes(dir string) uint64 {
	var total uint64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	return total
}

// diskStats returns the total and free bytes for the filesystem containing dir
// using syscall.Statfs. On failure it logs a warning and returns zeros.
func diskStats(dir string) (total, free uint64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		logging.L.Warn("failed to stat filesystem", zap.String("dir", dir), zap.Error(err))
		return 0, 0
	}
	// Bsize is the fundamental block size; Blocks/Bfree count in those units.
	total = uint64(stat.Bsize) * stat.Blocks
	free = uint64(stat.Bsize) * stat.Bfree
	return total, free
}

// buildNodeMeta constructs current NodeMeta from the local store state.
func buildNodeMeta(_ context.Context, nodeID, listenAddr, dataDir string, _ chunk.Store, status string) *metadata.NodeMeta {
	total, free := diskStats(dataDir)
	return &metadata.NodeMeta{
		NodeID:            nodeID,
		Address:           listenAddr,
		DiskCount:         1,
		TotalCapacity:     int64(total),
		AvailableCapacity: int64(free),
		LastHeartbeat:     time.Now().Unix(),
		Status:            status,
	}
}

// registerNode sends the initial NodeMeta registration and then periodically
// sends heartbeats until the context is cancelled.
func registerNode(ctx context.Context, client *metadata.GRPCClient, nodeID, listenAddr, dataDir string, store chunk.Store, heartbeatInterval time.Duration) {
	// Initial registration.
	if err := client.PutNodeMeta(ctx, buildNodeMeta(ctx, nodeID, listenAddr, dataDir, store, "ready")); err != nil {
		logging.L.Warn("node registration failed; will retry on next heartbeat", zap.Error(err))
	} else {
		logging.L.Info("node registered with metadata service", zap.String("nodeID", nodeID))
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Mark node as offline on graceful shutdown.
			offlineMeta := buildNodeMeta(context.Background(), nodeID, listenAddr, dataDir, store, "offline")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.PutNodeMeta(shutdownCtx, offlineMeta); err != nil {
				logging.L.Warn("failed to mark node offline on shutdown", zap.Error(err))
			}
			return
		case <-ticker.C:
			if err := client.PutNodeMeta(ctx, buildNodeMeta(ctx, nodeID, listenAddr, dataDir, store, "ready")); err != nil {
				logging.L.Warn("heartbeat failed", zap.String("nodeID", nodeID), zap.Error(err))
			} else {
				logging.L.Debug("heartbeat sent", zap.String("nodeID", nodeID))
			}
		}
	}
}

func main() {
	listenAddr := flag.String("listen", ":9100", "gRPC listen address")
	hostIP := flag.String("host-ip", "", "Host IP address advertised to NVMe-oF initiators (required for NVMe-oF targets)")
	dataDir := flag.String("data-dir", "/var/lib/novastor/chunks", "Chunk storage directory")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	metricsAddr := flag.String("metrics-addr", ":9101", "Prometheus metrics listen address")
	scrubInterval := flag.Duration("scrub-interval", 24*time.Hour, "Interval between scrub runs")
	heartbeatInterval := flag.Duration("heartbeat-interval", 30*time.Second, "Interval between metadata heartbeats")
	nodeID := flag.String("node-id", "", "Unique node ID (defaults to hostname)")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to server certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to server key for mTLS")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	encryptionEnabled := flag.Bool("encryption-enabled", false, "Enable chunk encryption at rest")
	encryptionKeyDir := flag.String("encryption-key-dir", "", "Path to directory containing encryption key files (file-based key management)")
	encryptionMasterKey := flag.String("encryption-master-key", "", "Base64-encoded 32-byte master key for derived key management")
	flag.Parse()

	logging.Init(false)
	defer logging.Sync()

	logging.L.Info("novastor-agent starting",
		zap.String("version", version),
		zap.String("commit", commit),
		zap.String("built", date),
	)

	// Resolve node ID from hostname if not provided.
	if *nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			logging.L.Fatal("failed to determine hostname for node ID", zap.Error(err))
		}
		*nodeID = hostname
	}

	// Register Prometheus metrics.
	metrics.Register()

	// Create chunk local store.
	localStore, err := chunk.NewLocalStore(*dataDir)
	if err != nil {
		logging.L.Fatal("failed to create chunk store", zap.Error(err))
	}

	// Optionally wrap the local store with encryption.
	var store chunk.Store = localStore
	if *encryptionEnabled {
		var km chunk.KeyManager
		switch {
		case *encryptionKeyDir != "":
			km, err = chunk.NewFileKeyManager(*encryptionKeyDir)
			if err != nil {
				logging.L.Fatal("failed to create file key manager", zap.Error(err))
			}
			logging.L.Info("chunk encryption enabled (file-based key management)",
				zap.String("keyDir", *encryptionKeyDir))
		case *encryptionMasterKey != "":
			masterKeyBytes, decodeErr := base64.StdEncoding.DecodeString(*encryptionMasterKey)
			if decodeErr != nil {
				logging.L.Fatal("failed to decode master key", zap.Error(decodeErr))
			}
			km, err = chunk.NewDerivedKeyManager(masterKeyBytes)
			if err != nil {
				logging.L.Fatal("failed to create derived key manager", zap.Error(err))
			}
			logging.L.Info("chunk encryption enabled (derived key management)")
		default:
			logging.L.Fatal("encryption enabled but no key source specified; set --encryption-key-dir or --encryption-master-key")
		}
		store = chunk.NewKeyedEncryptedStore(localStore, km)
	}

	// Wire the AgentCollector to real data sources.
	collector := metrics.NewAgentCollector()
	collector.ChunkCountFn = func() int64 {
		ids, listErr := store.List(context.Background())
		if listErr != nil {
			logging.L.Warn("metrics: listing chunks failed", zap.Error(listErr))
			return 0
		}
		return int64(len(ids))
	}
	collector.DiskStatsFn = func() []metrics.DiskStats {
		used := diskUsedBytes(*dataDir)
		total, free := diskStats(*dataDir)
		return []metrics.DiskStats{
			{
				Device: *dataDir,
				Total:  total,
				Used:   used,
				Free:   free,
			},
		}
	}

	// Main context for the agent lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the periodic metrics collection loop.
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collector.Collect()
			}
		}
	}()

	// Build gRPC server options.
	var serverOpts []grpc.ServerOption
	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		rotator := transport.NewCertRotator(*tlsCert, *tlsKey, *tlsRotationInterval)
		rotator.Start(ctx)
		logging.L.Info("TLS certificate rotation enabled",
			zap.String("certPath", *tlsCert),
			zap.String("keyPath", *tlsKey),
			zap.Duration("interval", *tlsRotationInterval),
		)
		tlsOpt, tlsErr := transport.NewServerTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
		}, rotator)
		if tlsErr != nil {
			logging.L.Fatal("failed to configure TLS", zap.Error(tlsErr))
		}
		serverOpts = append(serverOpts, tlsOpt)
	}

	srv := grpc.NewServer(serverOpts...)

	// Create and register the chunk server.
	chunkServer := agent.NewChunkServer(store)
	chunkServer.Register(srv)

	// Create and register the NVMe target server when a host IP is provided.
	if *hostIP != "" {
		nvmeServer := agent.NewNVMeTargetServer(*hostIP)
		nvmeServer.Register(srv)
		logging.L.Info("NVMe-oF target service registered", zap.String("hostIP", *hostIP))
	} else {
		logging.L.Info("NVMe-oF target service disabled (--host-ip not set)")
	}

	// Start the scrubber.
	scrubber := chunk.NewScrubber(store, logReporter{}, *scrubInterval)
	scrubber.Start(ctx)

	// Connect to the metadata service and register this node.
	metaClient, metaErr := metadata.Dial(*metaAddr)
	if metaErr != nil {
		logging.L.Warn("cannot connect to metadata service; node registration disabled",
			zap.String("metaAddr", *metaAddr),
			zap.Error(metaErr),
		)
	} else {
		defer metaClient.Close()
		// Use the routable host IP for registration so that other components
		// (e.g. the CSI controller) can dial this agent from outside the pod.
		// Fall back to listenAddr when host-ip is not set.
		registrationAddr := *listenAddr
		if *hostIP != "" {
			_, port, splitErr := net.SplitHostPort(*listenAddr)
			if splitErr == nil {
				registrationAddr = net.JoinHostPort(*hostIP, port)
			}
		}
		go registerNode(ctx, metaClient, *nodeID, registrationAddr, *dataDir, store, *heartbeatInterval)
	}

	// Start Prometheus metrics HTTP server.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:         *metricsAddr,
		Handler:      metricsMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		logging.L.Info("metrics server listening", zap.String("addr", *metricsAddr))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.L.Error("metrics server error", zap.Error(err))
		}
	}()

	// Start gRPC listener.
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logging.L.Fatal("failed to listen", zap.String("addr", *listenAddr), zap.Error(err))
	}

	logging.L.Info("agent gRPC server listening", zap.String("addr", *listenAddr))

	// Graceful shutdown on SIGTERM/SIGINT.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		logging.L.Info("shutting down agent")
		cancel() // cancels ctx: stops scrubber loop, heartbeat loop, metrics loop
		scrubber.Stop()
		srv.GracefulStop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		metricsServer.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(listener); err != nil {
		logging.L.Fatal("agent gRPC server failed", zap.Error(err))
	}
	logging.L.Info("agent stopped")
}
