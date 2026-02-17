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

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/metrics"
	"github.com/piwi3910/novastor/internal/transport"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// logReporter is a simple ScrubReporter that logs corrupt chunk IDs.
type logReporter struct{}

func (logReporter) ReportCorruptChunk(_ context.Context, chunkID chunk.ChunkID) error {
	log.Printf("scrub: corrupt chunk detected: %s", chunkID)
	metrics.ScrubErrors.Inc()
	return nil
}

func main() {
	listenAddr := flag.String("listen", ":9100", "gRPC listen address")
	dataDir := flag.String("data-dir", "/var/lib/novastor/chunks", "Chunk storage directory")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	metricsAddr := flag.String("metrics-addr", ":9101", "Prometheus metrics listen address")
	scrubInterval := flag.Duration("scrub-interval", 24*time.Hour, "Interval between scrub runs")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to server certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to server key for mTLS")
	flag.Parse()

	logging.Init(false)
	defer logging.Sync()

	log.Printf("novastor-agent %s (commit: %s, built: %s)", version, commit, date)

	// Reserve meta-addr for future use (heartbeats, registration).
	_ = *metaAddr

	// Register Prometheus metrics.
	metrics.Register()

	// Create chunk local store.
	store, err := chunk.NewLocalStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create chunk store: %v", err)
	}

	// Build gRPC server options.
	var serverOpts []grpc.ServerOption
	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		tlsOpt, tlsErr := transport.NewServerTLS(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
		})
		if tlsErr != nil {
			log.Fatalf("Failed to configure TLS: %v", tlsErr)
		}
		serverOpts = append(serverOpts, tlsOpt)
	}

	srv := grpc.NewServer(serverOpts...)

	// Create and register the chunk server.
	chunkServer := agent.NewChunkServer(store)
	chunkServer.Register(srv)

	// Start the scrubber.
	scrubber := chunk.NewScrubber(store, logReporter{}, *scrubInterval)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scrubber.Start(ctx)

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

	// Start gRPC listener.
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *listenAddr, err)
	}

	log.Printf("Agent gRPC server listening on %s", *listenAddr)

	// Graceful shutdown on SIGTERM/SIGINT.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		log.Println("Shutting down agent...")
		scrubber.Stop()
		srv.GracefulStop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		metricsServer.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(listener); err != nil {
		log.Fatalf("Agent gRPC server failed: %v", err)
	}
	log.Println("Agent stopped")
}
