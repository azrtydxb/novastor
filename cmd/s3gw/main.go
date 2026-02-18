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

	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/metadata"
	s3gw "github.com/piwi3910/novastor/internal/s3"
	"github.com/piwi3910/novastor/internal/transport"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	listenAddr := flag.String("listen", ":9000", "HTTP listen address")
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
	defer metaClient.Close()

	// Connect to the chunk agent.
	chunkClient, err := agent.Dial(*agentAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to chunk agent at %s: %v", *agentAddr, err)
	}
	defer chunkClient.Close()

	// Create adapters from metadata client to S3 interfaces.
	adapter := s3gw.NewMetadataAdapter(metaClient)
	chunkStore := agent.NewLocalChunkStore(chunkClient)

	// Wire the S3 gateway with real dependencies.
	gateway := s3gw.NewGateway(adapter, adapter, chunkStore, adapter, *accessKey, *secretKey)

	srv := &http.Server{
		Addr:         *listenAddr,
		Handler:      gateway,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("S3 gateway listening on %s", *listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("S3 gateway failed: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down S3 gateway...")
	cancel() // cancel main context: stops cert rotator

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("S3 gateway shutdown failed: %v", err)
	}
	log.Println("S3 gateway stopped")
}
