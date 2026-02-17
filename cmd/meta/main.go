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
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	nodeID := flag.String("node-id", "", "Raft node ID (required)")
	dataDir := flag.String("data-dir", "/var/lib/novastor/meta", "Raft data directory")
	bindAddr := flag.String("bind-addr", ":7000", "Raft bind address")
	grpcAddr := flag.String("grpc-addr", ":7001", "gRPC client API address")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a single-node Raft cluster")
	metricsAddr := flag.String("metrics-addr", ":7002", "Prometheus metrics listen address")
	flag.Parse()

	logging.Init(false)
	defer logging.Sync()

	log.Printf("novastor-meta %s (commit: %s, built: %s)", version, commit, date)

	if *nodeID == "" {
		log.Fatal("--node-id is required")
	}

	// Register Prometheus metrics.
	metrics.Register()

	// Create the Raft-backed metadata store.
	store, err := metadata.NewRaftStore(*nodeID, *dataDir, *bindAddr, *bootstrap)
	if err != nil {
		log.Fatalf("Failed to create Raft store: %v", err)
	}
	defer store.Close()

	// Create and register the gRPC metadata server.
	grpcServer := grpc.NewServer()
	metaServer := metadata.NewGRPCServer(store)
	metaServer.Register(grpcServer)

	// Start gRPC listener.
	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *grpcAddr, err)
	}

	log.Printf("Metadata gRPC server listening on %s (Raft bind: %s, node: %s)", *grpcAddr, *bindAddr, *nodeID)

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
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	metricsServer.Shutdown(shutdownCtx)
	log.Println("Metadata service stopped")
}
