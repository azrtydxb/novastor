package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/piwi3910/novastor/internal/filer"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	listenAddr := flag.String("listen", ":2049", "NFS listen address")
	metaAddr := flag.String("meta-addr", "localhost:7000", "Metadata service address")
	flag.Parse()

	log.Printf("novastor-filer %s (commit: %s, built: %s)", version, commit, date)

	// Phase 4 stub: metadata and chunk backends are nil until full cluster wiring.
	_ = *metaAddr

	locker := filer.NewLockManager()
	fs := filer.NewFileSystem(nil, nil)
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
