package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	"github.com/piwi3910/novastor/internal/agent"
	novcsi "github.com/piwi3910/novastor/internal/csi"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/placement"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	endpoint := flag.String("endpoint", "unix:///var/lib/kubelet/plugins/novastor.csi.novastor.io/csi.sock", "CSI endpoint")
	nodeID := flag.String("node-id", "", "Node ID for this CSI node")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	agentAddrs := flag.String("agent-addrs", "", "Comma-separated list of agent addresses (nodeID=addr,...)")
	flag.Parse()

	log.Printf("novastor-csi %s (commit: %s, built: %s)", version, commit, date)

	// Connect to the metadata service.
	metaClient, err := metadata.Dial(*metaAddr)
	if err != nil {
		log.Fatalf("Failed to connect to metadata service at %s: %v", *metaAddr, err)
	}
	defer metaClient.Close()

	// Build the NodeChunkClient from agent addresses.
	nodeChunkClient := agent.NewNodeChunkClient()
	var nodeIDs []string
	if *agentAddrs != "" {
		for _, entry := range strings.Split(*agentAddrs, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			parts := strings.SplitN(entry, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("Invalid agent address format %q, expected nodeID=addr", entry)
			}
			nID, addr := parts[0], parts[1]
			if err := nodeChunkClient.AddNode(nID, addr); err != nil {
				log.Fatalf("Failed to connect to agent node %s at %s: %v", nID, addr, err)
			}
			nodeIDs = append(nodeIDs, nID)
			log.Printf("Connected to agent node %s at %s", nID, addr)
		}
	}

	// Create placement engine with known nodes.
	placer := placement.NewRoundRobin(nodeIDs)

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
		mounter := &novcsi.RealMounter{}
		node := novcsi.NewNodeService(*nodeID, nodeChunkClient, mounter)
		csi.RegisterNodeServer(srv, node)
	}

	// Register CSI Controller service with real metadata + placement.
	controller := novcsi.NewControllerServer(metaClient, placer)
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
