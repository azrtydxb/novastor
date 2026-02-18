package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/metrics"
	s3gw "github.com/piwi3910/novastor/internal/s3"
	"github.com/piwi3910/novastor/internal/transport"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func init() {
	// Register Prometheus metrics.
	metrics.Register()
}

func main() {
	listenAddr := flag.String("listen", ":9000", "S3 HTTP listen address")
	metricsAddr := flag.String("metrics-addr", ":8081", "Metrics HTTP listen address")
	accessKey := flag.String("access-key", "", "S3 access key")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	agentAddr := flag.String("agent-addr", "localhost:9100", "Chunk agent address")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to client certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to client key for mTLS")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	flag.Parse()

	log.Printf("novastor-s3gw %s (commit: %s, built: %s)", version, commit, date)

	if *accessKey == "" || *secretKey == "" {
		fmt.Fprintln(os.Stderr, "Warning: --access-key and --secret-key not set, S3 auth will reject all requests")
	}

	// Main context for the S3 gateway lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build TLS dial option when TLS flags are provided.
	var dialOpts []grpc.DialOption
	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		rotator := transport.NewCertRotator(*tlsCert, *tlsKey, *tlsRotationInterval)
		rotator.Start(ctx)
		log.Printf("TLS certificate rotation enabled (cert=%s, key=%s, interval=%s)",
			*tlsCert, *tlsKey, *tlsRotationInterval)
		tlsOpt, tlsErr := transport.NewClientTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
		}, rotator)
		if tlsErr != nil {
			log.Fatalf("Failed to configure TLS: %v", tlsErr)
		}
		dialOpts = append(dialOpts, tlsOpt)
	}

	// Connect to the metadata service.
	metaClient, err := metadata.Dial(*metaAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to metadata service at %s: %v", *metaAddr, err)
	}
	defer func() { _ = metaClient.Close() }()

	// Connect to the chunk agent.
	chunkClient, err := agent.Dial(*agentAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to chunk agent at %s: %v", *agentAddr, err)
	}
	defer func() { _ = chunkClient.Close() }()

	// Create adapters from metadata client to S3 interfaces.
	adapter := s3gw.NewMetadataAdapter(metaClient)
	chunkStore := agent.NewLocalChunkStore(chunkClient)

	// Wire the S3 gateway with real dependencies.
	gateway := s3gw.NewGateway(adapter, adapter, chunkStore, adapter, nil, *accessKey, *secretKey)

	// Create a handler that serves both S3 API and metrics.
	mux := http.NewServeMux()
	mux.Handle("/", gateway)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:         *listenAddr,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start metrics server on a separate port.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	metricsSrv := &http.Server{
		Addr:         *metricsAddr,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		logging.L.Info("S3 gateway listening", zap.String("addr", *listenAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.L.Error("S3 gateway failed", zap.Error(err))
		}
	}()

	go func() {
		logging.L.Info("metrics server listening", zap.String("addr", *metricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.L.Error("metrics server failed", zap.Error(err))
		}
	}()

	<-stop
	logging.L.Info("Shutting down S3 gateway...")
	cancel() // cancel main context: stops cert rotator

	// Shutdown metrics server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logging.L.Error("metrics server shutdown failed", zap.Error(err))
	}

	// Shutdown S3 API server.
	s3ShutdownCtx, s3ShutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer s3ShutdownCancel()
	if err := srv.Shutdown(s3ShutdownCtx); err != nil {
		logging.L.Error("S3 gateway shutdown failed", zap.Error(err))
	}
	logging.L.Info("S3 gateway stopped")
}
