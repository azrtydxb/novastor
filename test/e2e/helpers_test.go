//go:build e2e

package e2e

import (
	"fmt"
	"hash/crc32"
	"net"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testCluster sets up a minimal NovaStor cluster for testing.
// It bootstraps a single-node Raft metadata store, starts a chunk agent
// backed by a LocalStore, and provides gRPC clients for both.
type testCluster struct {
	metaStore   *metadata.RaftStore
	metaServer  *grpc.Server
	metaClient  *metadata.GRPCClient
	metaAddr    string
	chunkStore  chunk.Store
	chunkServer *grpc.Server
	agentClient *agent.Client
	chunkAddr   string
	tmpDirs     []string
	cleanups    []func()
}

// newTestCluster creates and starts a fully wired NovaStor test cluster.
// It creates temp dirs, bootstraps a single-node Raft store, starts metadata
// and chunk gRPC servers on random ports, and creates clients for both.
//
// The Raft transport requires its own TCP port (separate from the gRPC metadata
// server), so we allocate three random ports: one for Raft, one for metadata
// gRPC, and one for chunk gRPC.
func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	tc := &testCluster{}

	// 1. Create temp dirs for raft data and chunk storage.
	raftDir := t.TempDir()
	chunkDir := t.TempDir()
	tc.tmpDirs = append(tc.tmpDirs, raftDir, chunkDir)

	// 2. Allocate a random port for the Raft transport (it binds its own listener).
	raftBindAddr := allocAddr(t)

	// 3. Bootstrap a single-node RaftStore on the Raft-specific port.
	raftStore, err := metadata.NewRaftStore("node-1", raftDir, raftBindAddr, true)
	if err != nil {
		t.Fatalf("Failed to create RaftStore: %v", err)
	}
	tc.metaStore = raftStore
	tc.cleanups = append(tc.cleanups, func() {
		raftStore.Close()
	})

	// Wait for the Raft leader to be elected (single-node cluster).
	if !waitForLeader(raftStore, 10*time.Second) {
		tc.Close()
		t.Fatalf("Raft leader was not elected within timeout")
	}

	// 4. Allocate random ports for metadata gRPC and chunk gRPC servers.
	metaLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tc.Close()
		t.Fatalf("Failed to listen for metadata server: %v", err)
	}
	metaAddr := metaLis.Addr().String()
	tc.metaAddr = metaAddr

	chunkLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		metaLis.Close()
		tc.Close()
		t.Fatalf("Failed to listen for chunk server: %v", err)
	}
	chunkAddr := chunkLis.Addr().String()
	tc.chunkAddr = chunkAddr

	// 5. Create and start metadata gRPC server.
	metaSrv := grpc.NewServer()
	metaGRPC := metadata.NewGRPCServer(raftStore)
	metaGRPC.Register(metaSrv)
	tc.metaServer = metaSrv
	tc.cleanups = append(tc.cleanups, func() {
		metaSrv.GracefulStop()
	})
	go func() {
		if err := metaSrv.Serve(metaLis); err != nil {
			// Server stopped, expected during cleanup.
		}
	}()

	// 6. Create and start chunk agent gRPC server.
	localStore, err := chunk.NewLocalStore(chunkDir)
	if err != nil {
		tc.Close()
		t.Fatalf("Failed to create LocalStore: %v", err)
	}
	tc.chunkStore = localStore

	chunkSrv := grpc.NewServer()
	chunkServer := agent.NewChunkServer(localStore)
	chunkServer.Register(chunkSrv)
	tc.chunkServer = chunkSrv
	tc.cleanups = append(tc.cleanups, func() {
		chunkSrv.GracefulStop()
	})
	go func() {
		if err := chunkSrv.Serve(chunkLis); err != nil {
			// Server stopped, expected during cleanup.
		}
	}()

	// 7. Create metadata gRPC client.
	metaClient, err := metadata.Dial(
		metaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		tc.Close()
		t.Fatalf("Failed to dial metadata server: %v", err)
	}
	tc.metaClient = metaClient
	tc.cleanups = append(tc.cleanups, func() {
		metaClient.Close()
	})

	// 8. Create chunk agent gRPC client.
	agentClient, err := agent.Dial(
		chunkAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		tc.Close()
		t.Fatalf("Failed to dial chunk server: %v", err)
	}
	tc.agentClient = agentClient
	tc.cleanups = append(tc.cleanups, func() {
		agentClient.Close()
	})

	return tc
}

// Close tears down all cluster components in reverse order.
func (tc *testCluster) Close() {
	for i := len(tc.cleanups) - 1; i >= 0; i-- {
		tc.cleanups[i]()
	}
}

// waitForLeader polls the RaftStore until it becomes leader or the timeout expires.
func waitForLeader(store *metadata.RaftStore, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// allocAddr allocates a random TCP port on localhost and returns the address
// string. The port is released immediately so it can be reused by the caller.
func allocAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to allocate address: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// freePort returns an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// formatAddr returns a "host:port" address string.
func formatAddr(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// computeCRC32C computes the CRC-32C (Castagnoli) checksum of data.
func computeCRC32C(data []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(data, table)
}
