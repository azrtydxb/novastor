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

	s3gw "github.com/piwi3910/novastor/internal/s3"
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
	metaAddr := flag.String("meta-addr", "localhost:7000", "Metadata service address")
	flag.Parse()

	log.Printf("novastor-s3gw %s (commit: %s, built: %s)", version, commit, date)

	if *accessKey == "" || *secretKey == "" {
		fmt.Fprintln(os.Stderr, "Warning: --access-key and --secret-key not set, S3 auth will reject all requests")
	}

	// Phase 3 stub: metadata and chunk backends are nil until full cluster wiring.
	_ = *metaAddr

	gateway := s3gw.NewGateway(nil, nil, nil, nil, *accessKey, *secretKey)

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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("S3 gateway shutdown failed: %v", err)
	}
	log.Println("S3 gateway stopped")
}
