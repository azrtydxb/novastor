package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/filer"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/transport"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	listenAddr := flag.String("listen", ":2049", "NFS listen address")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	agentAddr := flag.String("agent-addr", "localhost:9100", "Chunk agent address")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to client certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to client key for mTLS")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	flag.Parse()

	log.Printf("novastor-filer %s (commit: %s, built: %s)", version, commit, date)

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
	defer metaClient.Close()

	// Connect to the chunk agent.
	chunkClient, err := agent.Dial(*agentAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to chunk agent at %s: %v", *agentAddr, err)
	}
	defer chunkClient.Close()

	// Create adapters from gRPC clients to filer interfaces.
	metaAdapter := filer.NewMetadataAdapter(metaClient)
	chunkAdapter := filer.NewChunkAdapter(chunkClient)

	locker := filer.NewLockManager()
	fs := filer.NewFileSystem(metaAdapter, chunkAdapter)
	nfsSrv := filer.NewNFSServer(fs, locker)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := nfsSrv.Serve(*listenAddr); err != nil {
			log.Fatalf("NFS server failed: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down NFS server...")
	cancel() // cancel main context: stops cert rotator
	if err := nfsSrv.Stop(); err != nil {
		log.Printf("Error stopping NFS server: %v", err)
	}
	log.Println("NFS server stopped")
}
