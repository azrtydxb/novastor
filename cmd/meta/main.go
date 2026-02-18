package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

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

func main() {
	nodeID := flag.String("node-id", "", "Raft node ID (defaults to hostname when empty)")
	dataDir := flag.String("data-dir", "/var/lib/novastor/meta", "Raft data directory")
	raftAddr := flag.String("raft-addr", ":7000", "Raft consensus transport listen address")
	grpcAddr := flag.String("grpc-addr", ":7001", "gRPC client API listen address")
	join := flag.String("join", "", "Comma-separated list of existing Raft peer addresses to join (e.g. peer1:7000,peer2:7000). When empty, bootstraps as a single-node cluster.")
	bootstrapExpect := flag.Int("bootstrap-expect", 0, "Number of nodes expected for initial cluster. When > 0 and join fails, the node bootstraps and lets others join.")
	metricsAddr := flag.String("metrics-addr", ":7002", "Prometheus metrics listen address")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to server certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to server key for mTLS")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	flag.Parse()

	logging.Init(false)
	defer logging.Sync()

	log.Printf("novastor-meta %s (commit: %s, built: %s)", version, commit, date)

	// Default node ID to hostname when not explicitly set.
	if *nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("--node-id not set and could not determine hostname: %v", err)
		}
		*nodeID = hostname
		log.Printf("--node-id not set; using hostname %q", *nodeID)
	}

	// Register Prometheus metrics.
	metrics.Register()

	// Create the Raft-backed metadata store.
	store, err := metadata.NewRaftStore(metadata.RaftConfig{
		NodeID:          *nodeID,
		DataDir:         *dataDir,
		RaftAddr:        *raftAddr,
		JoinAddrs:       *join,
		BootstrapExpect: *bootstrapExpect,
	})
	if err != nil {
		log.Fatalf("Failed to create Raft store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Main context for the metadata service lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Raft metrics monitoring (updates every 5 seconds).
	store.StartMetricsMonitor(ctx, 5*time.Second)

	// Build gRPC server options.
	var serverOpts []grpc.ServerOption
	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		rotator := transport.NewCertRotator(*tlsCert, *tlsKey, *tlsRotationInterval)
		rotator.Start(ctx)
		log.Printf("TLS certificate rotation enabled (cert=%s, key=%s, interval=%s)",
			*tlsCert, *tlsKey, *tlsRotationInterval)
		tlsOpt, tlsErr := transport.NewServerTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
		}, rotator)
		if tlsErr != nil {
			log.Fatalf("Failed to configure TLS: %v", tlsErr)
		}
		serverOpts = append(serverOpts, tlsOpt)
	}

	// Create and register the gRPC metadata server.
	grpcServer := grpc.NewServer(serverOpts...)
	metaServer := metadata.NewGRPCServer(store)
	metaServer.Register(grpcServer)

	// Start gRPC listener.
	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *grpcAddr, err)
	}

	log.Printf("Metadata gRPC server listening on %s (Raft addr: %s, node: %s, join: %q)",
		*grpcAddr, *raftAddr, *nodeID, *join)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Metadata gRPC server failed: %v", err)
		}
	}()

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
		log.Printf("Metrics server listening on %s", *metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGTERM/SIGINT.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	log.Println("Shutting down metadata service...")
	cancel() // cancel main context: stops cert rotator
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = metricsServer.Shutdown(shutdownCtx)
	log.Println("Metadata service stopped")
}
