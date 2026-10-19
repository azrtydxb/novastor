//go:build integration

package integration

import (
	"context"
	"net"
	"testing"

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/test/internal/memchunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startChunkAgent serves an in-memory ChunkService (see memchunk) on a loopback
// port and returns an agent.Client connected to it. Cleanup is registered with
// t.Cleanup.
func startChunkAgent(t *testing.T) *agent.Client {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for chunk server: %v", err)
	}
	srv := grpc.NewServer()
	memchunk.New().Register(srv)
	t.Cleanup(srv.GracefulStop)
	go func() { _ = srv.Serve(lis) }()

	client, err := agent.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial chunk server: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}
