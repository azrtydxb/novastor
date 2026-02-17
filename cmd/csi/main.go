package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	novcsi "github.com/piwi3910/novastor/internal/csi"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	endpoint := flag.String("endpoint", "unix:///var/lib/kubelet/plugins/novastor.csi.novastor.io/csi.sock", "CSI endpoint")
	nodeID := flag.String("node-id", "", "Node ID for this CSI node")
	metaAddr := flag.String("meta-addr", "localhost:7000", "Metadata service address")
	flag.Parse()

	log.Printf("novastor-csi %s (commit: %s, built: %s)", version, commit, date)

	// For Phase 2, metadata and placement are stubbed — the controller runs
	// with nil dependencies until the full cluster wiring is implemented.
	// Only the identity and node services are fully functional at this stage.
	_ = *metaAddr

	// Parse endpoint scheme and address.
	scheme, addr, err := parseEndpoint(*endpoint)
	if err != nil {
		log.Fatalf("Invalid endpoint %q: %v", *endpoint, err)
	}

	if scheme == "unix" {
		// Remove stale socket file.
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			log.Fatalf("Failed to remove socket %s: %v", addr, err)
		}
	}

	listener, err := net.Listen(scheme, addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s://%s: %v", scheme, addr, err)
	}
	defer listener.Close()

	srv := grpc.NewServer()

	// Register CSI Identity service.
	identity := novcsi.NewIdentityServer()
	csi.RegisterIdentityServer(srv, identity)

	// Register CSI Node service (requires node ID).
	if *nodeID != "" {
		node := novcsi.NewNodeService(*nodeID, nil, nil)
		csi.RegisterNodeServer(srv, node)
	}

	// Register CSI Controller service (nil deps until wired to real metadata/placement).
	controller := novcsi.NewControllerServer(nil, nil)
	csi.RegisterControllerServer(srv, controller)

	log.Printf("CSI driver listening on %s://%s", scheme, addr)

	// Graceful shutdown on SIGTERM/SIGINT.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		log.Println("Shutting down CSI driver...")
		srv.GracefulStop()
	}()

	if err := srv.Serve(listener); err != nil {
		log.Fatalf("CSI gRPC server failed: %v", err)
	}
}

// parseEndpoint extracts scheme and address from a CSI endpoint string.
func parseEndpoint(ep string) (string, string, error) {
	if len(ep) > 7 && ep[:7] == "unix://" {
		return "unix", ep[7:], nil
	}
	if len(ep) > 6 && ep[:6] == "tcp://" {
		return "tcp", ep[6:], nil
	}
	return "", "", fmt.Errorf("unsupported endpoint scheme in %q", ep)
}
