package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/filer"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/metrics"
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
	// Operation mode flags.
	mode := flag.String("mode", "nfs", "Server mode: 'nfs' or 'fuse'")

	// NFS-specific flags.
	nfsListenAddr := flag.String("nfs-listen", ":2049", "NFS listen address")

	// FUSE-specific flags.
	fuseMountPoint := flag.String("fuse-mount", "/mnt/novastor", "FUSE mount point")
	fuseAllowOther := flag.Bool("fuse-allow-other", false, "Allow other users to access FUSE mount")
	fuseFsName := flag.String("fuse-fsname", "novastor", "Filesystem name for FUSE")

	// Common flags.
	metricsAddr := flag.String("metrics-addr", ":8080", "Metrics HTTP listen address")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	agentAddr := flag.String("agent-addr", "localhost:9100", "Chunk agent address")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to client certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to client key for mTLS")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	flag.Parse()

	log.Printf("novastor-filer %s (commit: %s, built: %s) mode=%s", version, commit, date, *mode)

	// Main context for the filer lifetime.
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

	// Create adapters from gRPC clients to filer interfaces.
	metaAdapter := filer.NewMetadataAdapter(metaClient)
	chunkAdapter := filer.NewChunkAdapter(chunkClient)

	// Create the filesystem.
	fs := filer.NewFileSystem(metaAdapter, chunkAdapter)

	// Start metrics server.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	metricsSrv := &http.Server{
		Addr:         *metricsAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	// Start metrics server.
	go func() {
		logging.L.Info("metrics server listening", zap.String("addr", *metricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.L.Error("metrics server failed", zap.Error(err))
		}
	}()

	// Start the appropriate server based on mode.
	go func() {
		switch *mode {
		case "nfs":
			runNFSServer(ctx, fs, *nfsListenAddr, stop)
		case "fuse":
			fuseConfig := &filer.FUSEConfig{
				MountPoint: *fuseMountPoint,
				AllowOther: *fuseAllowOther,
				FsName:     *fuseFsName,
				Debug:      false,
			}
			runFUSEServer(ctx, fs, fuseConfig, stop)
		default:
			log.Fatalf("Unknown mode: %s (valid: nfs, fuse)", *mode)
		}
	}()

	<-stop
	logging.L.Info("Shutting down...")
	cancel() // cancel main context: stops cert rotator

	// Shutdown metrics server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logging.L.Error("metrics server shutdown failed", zap.Error(err))
	}
}

// runNFSServer starts and manages the NFS server.
func runNFSServer(ctx context.Context, fs *filer.FileSystem, listenAddr string, stop chan os.Signal) {
	locker := filer.NewLockManager()
	nfsSrv := filer.NewNFSServer(fs, locker)

	go func() {
		if err := nfsSrv.Serve(listenAddr); err != nil {
			log.Printf("NFS server failed: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down NFS server...")
	if err := nfsSrv.Stop(); err != nil {
		log.Printf("Error stopping NFS server: %v", err)
	}
	log.Println("NFS server stopped")
}

// runFUSEServer starts and manages the FUSE server.
func runFUSEServer(ctx context.Context, fs *filer.FileSystem, config *filer.FUSEConfig, stop chan os.Signal) {
	fuseSrv := filer.NewFUSEServer(fs, config)

	// Run FUSE server in a goroutine.
	serverErr := make(chan error, 1)
	go func() {
		if err := fuseSrv.Serve(); err != nil {
			serverErr <- err
		}
		close(serverErr)
	}()

	// Wait for shutdown signal or server error.
	select {
	case <-stop:
		log.Println("Shutting down FUSE server...")
	case err := <-serverErr:
		if err != nil {
			log.Printf("FUSE server error: %v", err)
		}
	}

	if err := fuseSrv.Stop(); err != nil {
		log.Printf("Error stopping FUSE server: %v", err)
	}
	log.Println("FUSE server stopped")
}
