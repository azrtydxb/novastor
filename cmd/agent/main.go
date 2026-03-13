// Package main provides the NovaStor storage agent binary.
// The agent runs on each storage node as a management plane: it coordinates
// with the metadata service, manages NVMe-oF targets via the SPDK data plane,
// and exposes Prometheus metrics. All data-path I/O is handled by the Rust
// SPDK data-plane process.
package main

import (
	"context"
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

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/agent/failover"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
	"github.com/azrtydxb/novastor/internal/spdk"
	"github.com/azrtydxb/novastor/internal/transport"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// buildNodeMeta constructs current NodeMeta from SPDK data-plane stats.
func buildNodeMeta(nodeID, listenAddr, status string) *metadata.NodeMeta {
	return &metadata.NodeMeta{
		NodeID:        nodeID,
		Address:       listenAddr,
		DiskCount:     1,
		LastHeartbeat: time.Now().Unix(),
		Status:        status,
	}
}

// registerNode sends the initial NodeMeta registration and then periodically
// sends heartbeats until the context is cancelled.
func registerNode(ctx context.Context, client *metadata.GRPCClient, nodeID, listenAddr string, heartbeatInterval time.Duration) {
	// Initial registration.
	if err := client.PutNodeMeta(ctx, buildNodeMeta(nodeID, listenAddr, "ready")); err != nil {
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
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := client.PutNodeMeta(shutdownCtx, buildNodeMeta(nodeID, listenAddr, "offline")); err != nil {
				logging.L.Warn("failed to mark node offline on shutdown", zap.Error(err))
			}
			cancel()
			return
		case <-ticker.C:
			if err := client.PutNodeMeta(ctx, buildNodeMeta(nodeID, listenAddr, "ready")); err != nil {
				logging.L.Warn("heartbeat failed", zap.String("nodeID", nodeID), zap.Error(err))
			} else {
				logging.L.Debug("heartbeat sent", zap.String("nodeID", nodeID))
			}
		}
	}
}

func main() {
	listenAddr := flag.String("listen", ":9100", "gRPC listen address")
	hostIP := flag.String("host-ip", "", "Host IP address advertised to NVMe-oF initiators (required)")
	podIP := flag.String("pod-ip", "", "Pod IP address used for gRPC registration (pod-network routable)")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	metricsAddr := flag.String("metrics-addr", ":9101", "Prometheus metrics listen address")
	gcInterval := flag.Duration("gc-interval", 1*time.Hour, "Interval between garbage collection runs")
	heartbeatInterval := flag.Duration("heartbeat-interval", 30*time.Second, "Interval between metadata heartbeats")
	nodeID := flag.String("node-id", "", "Unique node ID (defaults to hostname)")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to server certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to server key for mTLS")
	tlsClientCert := flag.String("tls-client-cert", "", "Path to client certificate for outbound mTLS connections to metadata service")
	tlsClientKey := flag.String("tls-client-key", "", "Path to client key for outbound mTLS connections to metadata service")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
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

	// Main context for the agent lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the SPDK data-plane socket.
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
	logging.L.Info("connected to SPDK data-plane", zap.String("socket", *spdkSocket))

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

	// Connect to the metadata service.
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

	// Compute registrationAddr for metadata and failover.
	registrationAddr := *listenAddr
	if *podIP != "" {
		_, port, splitErr := net.SplitHostPort(*listenAddr)
		if splitErr == nil {
			registrationAddr = net.JoinHostPort(*podIP, port)
		}
	}

	// Register NVMe-oF target service via SPDK data plane.
	if *hostIP != "" {
		if metaClient == nil {
			logging.L.Warn("NVMe-oF target service disabled: no metadata connection")
		} else {
			spdkServer := agent.NewSPDKTargetServer(*hostIP, spdkClient, metaClient)
			spdkServer.Register(srv)
			logging.L.Info("NVMe-oF target service registered (SPDK)", zap.String("hostIP", *hostIP))

			// Start failover controller for ANA state management.
			fc := failover.New(*nodeID, registrationAddr, *hostIP, metaClient, spdkClient, logging.L)
			if err := fc.Start(ctx); err != nil {
				logging.L.Fatal("failover controller start", zap.Error(err))
			}
			defer fc.Stop()
		}
	} else {
		logging.L.Info("NVMe-oF target service disabled (--host-ip not set)")
	}

	// Start the garbage collector for orphan chunks.
	if metaClient != nil {
		// TODO: GC needs to be reimplemented to work via SPDK data plane
		// instead of the chunk.Store interface.
		_ = gcInterval
		logging.L.Info("agent garbage collector: pending SPDK integration")
	}

	// Register this node with the metadata service.
	if metaClient != nil {
		defer metaClient.Close()
		go registerNode(ctx, metaClient, *nodeID, registrationAddr, *heartbeatInterval)
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
		cancel()
		srv.GracefulStop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(listener); err != nil {
		logging.L.Fatal("agent gRPC server failed", zap.Error(err))
	}
	logging.L.Info("agent stopped")
}
