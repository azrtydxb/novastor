// Package main provides the NovaStor storage agent binary.
// The agent runs on each storage node and manages local chunk storage,
// NVMe-oF target services, and coordination with the metadata service.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/piwi3910/novastor/internal/spdk"
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

// buildNodeMeta constructs current NodeMeta from the store state.
// It uses the CapacityStore interface when available to get capacity information.
func buildNodeMeta(ctx context.Context, nodeID, listenAddr string, store chunk.Store, status string) *metadata.NodeMeta {
	meta := &metadata.NodeMeta{
		NodeID:        nodeID,
		Address:       listenAddr,
		DiskCount:     1,
		LastHeartbeat: time.Now().Unix(),
		Status:        status,
	}

	// If the store implements CapacityStore, use it to get capacity information.
	if cs, ok := store.(chunk.CapacityStore); ok {
		stats, err := cs.Stats(ctx)
		if err != nil {
			logging.L.Warn("failed to get store stats", zap.Error(err))
		} else {
			meta.TotalCapacity = stats.TotalBytes
			meta.AvailableCapacity = stats.AvailableBytes
		}
	}

	return meta
}

// registerNode sends the initial NodeMeta registration and then periodically
// sends heartbeats until the context is cancelled.
func registerNode(ctx context.Context, client *metadata.GRPCClient, nodeID, listenAddr string, store chunk.Store, heartbeatInterval time.Duration) {
	// Initial registration.
	if err := client.PutNodeMeta(ctx, buildNodeMeta(ctx, nodeID, listenAddr, store, "ready")); err != nil {
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
			offlineMeta := buildNodeMeta(context.Background(), nodeID, listenAddr, store, "offline")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.PutNodeMeta(shutdownCtx, offlineMeta); err != nil {
				logging.L.Warn("failed to mark node offline on shutdown", zap.Error(err))
			}
			return
		case <-ticker.C:
			if err := client.PutNodeMeta(ctx, buildNodeMeta(ctx, nodeID, listenAddr, store, "ready")); err != nil {
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
	podIP := flag.String("pod-ip", "", "Pod IP address used for gRPC registration (pod-network routable)")
	backendType := flag.String("backend", "local", "Storage backend type (local, memory)")
	dataDir := flag.String("data-dir", "/var/lib/novastor/chunks", "Chunk storage directory (for local backend)")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	metricsAddr := flag.String("metrics-addr", ":9101", "Prometheus metrics listen address")
	scrubInterval := flag.Duration("scrub-interval", 24*time.Hour, "Interval between scrub runs")
	gcInterval := flag.Duration("gc-interval", 1*time.Hour, "Interval between garbage collection runs")
	heartbeatInterval := flag.Duration("heartbeat-interval", 30*time.Second, "Interval between metadata heartbeats")
	nodeID := flag.String("node-id", "", "Unique node ID (defaults to hostname)")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to server certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to server key for mTLS")
	tlsClientCert := flag.String("tls-client-cert", "", "Path to client certificate for outbound mTLS connections to metadata service")
	tlsClientKey := flag.String("tls-client-key", "", "Path to client key for outbound mTLS connections to metadata service")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	encryptionEnabled := flag.Bool("encryption-enabled", false, "Enable chunk encryption at rest")
	encryptionKeyDir := flag.String("encryption-key-dir", "", "Path to directory containing encryption key files (file-based key management)")
	encryptionMasterKey := flag.String("encryption-master-key", "", "Base64-encoded 32-byte master key for derived key management")
	dataPlane := flag.String("data-plane", "legacy", "Data plane mode: 'legacy' (chunk-backed loop) or 'spdk' (SPDK user-space)")
	_ = flag.String("spdk-binary", "/usr/local/bin/novastor-dataplane", "Path to the SPDK data-plane binary (unused: dataplane runs as separate DaemonSet)")
	spdkSocket := flag.String("spdk-socket", "/var/tmp/novastor-spdk.sock", "Path to the SPDK JSON-RPC Unix socket")
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

	// Create chunk store using the backend factory.
	backendConfig := map[string]string{}
	if *dataDir != "" {
		backendConfig["dir"] = *dataDir
	}
	backendStore, err := chunk.CreateBackend(*backendType, backendConfig)
	if err != nil {
		logging.L.Fatal("failed to create chunk store",
			zap.String("backend", *backendType),
			zap.Error(err),
		)
	}

	// Optionally wrap the backend store with encryption.
	store := backendStore
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
		store = chunk.NewKeyedEncryptedStore(backendStore, km)
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
	// Disk stats are only available for local backend.
	collector.DiskStatsFn = func() []metrics.DiskStats {
		// Use CapacityStore interface if available.
		if cs, ok := store.(chunk.CapacityStore); ok {
			stats, err := cs.Stats(context.Background())
			if err != nil {
				logging.L.Warn("metrics: getting store stats failed", zap.Error(err))
				return []metrics.DiskStats{{Device: *dataDir, Total: 0, Used: 0, Free: 0}}
			}
			total := stats.TotalBytes
			if total < 0 {
				total = 0
			}
			used := stats.UsedBytes
			if used < 0 {
				used = 0
			}
			free := stats.AvailableBytes
			if free < 0 {
				free = 0
			}
			return []metrics.DiskStats{
				{
					Device: *dataDir,
					Total:  uint64(total),
					Used:   uint64(used),
					Free:   uint64(free),
				},
			}
		}
		// If CapacityStore is not implemented, return zeros.
		logging.L.Warn("metrics: store does not support capacity stats")
		return []metrics.DiskStats{{Device: *dataDir, Total: 0, Used: 0, Free: 0}}
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

	// Build TLS dial options for outbound metadata connections.
	var dialOpts []grpc.DialOption
	clientCertPath := *tlsClientCert
	clientKeyPath := *tlsClientKey
	if clientCertPath == "" {
		clientCertPath = *tlsCert
	}
	if clientKeyPath == "" {
		clientKeyPath = *tlsKey
	}
	if *tlsCA != "" && clientCertPath != "" && clientKeyPath != "" {
		clientRotator := transport.NewCertRotator(clientCertPath, clientKeyPath, *tlsRotationInterval)
		clientRotator.Start(ctx)
		clientTLSOpt, clientTLSErr := transport.NewClientTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   clientCertPath,
			KeyPath:    clientKeyPath,
		}, clientRotator)
		if clientTLSErr != nil {
			logging.L.Fatal("failed to configure client TLS for metadata connection", zap.Error(clientTLSErr))
		}
		dialOpts = append(dialOpts, clientTLSOpt)
	}

	// Connect to the metadata service early (needed for NVMe-oF target server).
	var metaClient *metadata.GRPCClient
	if *metaAddr != "" {
		var metaErr error
		metaClient, metaErr = metadata.Dial(*metaAddr, dialOpts...)
		if metaErr != nil {
			logging.L.Warn("cannot connect to metadata service; some features disabled",
				zap.String("metaAddr", *metaAddr),
				zap.Error(metaErr),
			)
		}
	}

	// Create and register the NVMe target server when a host IP is provided.
	if *hostIP != "" {
		if metaClient == nil {
			logging.L.Warn("NVMe-oF target service disabled: no metadata connection")
		} else if *dataPlane == "spdk" {
			// SPDK mode: connect to the data-plane socket provided by the
			// dataplane DaemonSet (shared via hostPath volume).
			logging.L.Info("waiting for SPDK data-plane socket", zap.String("socket", *spdkSocket))
			socketTimeout := 120 * time.Second
			socketDeadline := time.Now().Add(socketTimeout)
			for {
				if _, statErr := os.Stat(*spdkSocket); statErr == nil {
					break
				}
				if time.Now().After(socketDeadline) {
					logging.L.Fatal("timed out waiting for SPDK data-plane socket",
						zap.String("socket", *spdkSocket),
						zap.Duration("timeout", socketTimeout))
				}
				time.Sleep(2 * time.Second)
			}
			spdkClient := spdk.NewClient(*spdkSocket)
			if err := spdkClient.Connect(); err != nil {
				logging.L.Fatal("failed to connect to SPDK data-plane", zap.Error(err))
			}
			spdkServer := agent.NewSPDKTargetServer(*hostIP, spdkClient, metaClient)
			spdkServer.Register(srv)
			logging.L.Info("NVMe-oF target service registered (SPDK)", zap.String("hostIP", *hostIP))
		} else {
			// Legacy mode: chunk-backed block devices with loop/nvmet.
			nvmeServer := agent.NewNVMeTargetServer(*hostIP, store, metaClient)
			nvmeServer.Register(srv)
			logging.L.Info("NVMe-oF target service registered (chunk-backed)", zap.String("hostIP", *hostIP))
		}
	} else {
		logging.L.Info("NVMe-oF target service disabled (--host-ip not set)")
	}

	// Start the scrubber.
	scrubber := chunk.NewScrubber(store, logReporter{}, *scrubInterval)
	scrubber.Start(ctx)

	// Start the garbage collector for orphan chunks.
	if metaClient != nil {
		agentGC := agent.NewGarbageCollector(store, metaClient, *gcInterval)
		agentGC.Start(ctx)
		logging.L.Info("agent garbage collector started", zap.Duration("interval", *gcInterval))
	} else {
		logging.L.Info("agent garbage collector disabled: no metadata connection")
	}

	// Register this node with the metadata service.
	if metaClient != nil {
		defer metaClient.Close()
		// Use the pod IP for gRPC registration so that other components
		// (e.g. the CSI controller) can dial this agent via the pod network.
		// Fall back to listenAddr when pod-ip is not set.
		registrationAddr := *listenAddr
		if *podIP != "" {
			_, port, splitErr := net.SplitHostPort(*listenAddr)
			if splitErr == nil {
				registrationAddr = net.JoinHostPort(*podIP, port)
			}
		}
		go registerNode(ctx, metaClient, *nodeID, registrationAddr, store, *heartbeatInterval)
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
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", *listenAddr)
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
