package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/filer"
	"github.com/piwi3910/novastor/internal/metadata"
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
	flag.Parse()

	log.Printf("novastor-filer %s (commit: %s, built: %s)", version, commit, date)

	// Connect to the metadata service.
	metaClient, err := metadata.Dial(*metaAddr)
	if err != nil {
		log.Fatalf("Failed to connect to metadata service at %s: %v", *metaAddr, err)
	}
	defer metaClient.Close()

	// Connect to the chunk agent.
	chunkClient, err := agent.Dial(*agentAddr)
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
	if err := nfsSrv.Stop(); err != nil {
		log.Printf("Error stopping NFS server: %v", err)
	}
	log.Println("NFS server stopped")
}
