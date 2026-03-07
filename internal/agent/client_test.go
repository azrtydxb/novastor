package agent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash/crc32"
	"net"
	"testing"

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/chunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startServer creates a local gRPC chunk server and returns its listener
// address and a cleanup function.
func startServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()

	dir := t.TempDir()
	store, err := chunk.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("creating local store: %v", err)
	}

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	srv := grpc.NewServer()
	chunkSrv := agent.NewChunkServer(store)
	chunkSrv.Register(srv)

	go func() {
		if serveErr := srv.Serve(lis); serveErr != nil {
			return
		}
	}()

	return lis.Addr().String(), func() {
		srv.GracefulStop()
	}
}

func TestClient_PutGetChunk(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := agent.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	data := []byte("hello, chunk client")
	ch := makeChunk(data)
	ctx := context.Background()

	if err := c.PutChunk(ctx, string(ch.ID), ch.Data, ch.Checksum); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	got, checksum, err := c.GetChunk(ctx, string(ch.ID))
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %d bytes, want %d", len(got), len(data))
	}
	if checksum != ch.Checksum {
		t.Errorf("checksum = %d, want %d", checksum, ch.Checksum)
	}
}

func TestClient_DeleteChunk(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := agent.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	data := []byte("delete-via-client")
	ch := makeChunk(data)
	ctx := context.Background()

	if err := c.PutChunk(ctx, string(ch.ID), ch.Data, ch.Checksum); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	exists, err := c.HasChunk(ctx, string(ch.ID))
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if !exists {
		t.Fatal("expected chunk to exist after put")
	}

	if err := c.DeleteChunk(ctx, string(ch.ID)); err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}

	exists, err = c.HasChunk(ctx, string(ch.ID))
	if err != nil {
		t.Fatalf("HasChunk after delete: %v", err)
	}
	if exists {
		t.Error("expected chunk to not exist after delete")
	}
}

func TestClient_HasChunk(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := agent.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()

	exists, err := c.HasChunk(ctx, "nonexistent-chunk-id")
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if exists {
		t.Error("expected false for missing chunk")
	}

	data := []byte("has-chunk-client-test")
	ch := makeChunk(data)

	if err := c.PutChunk(ctx, string(ch.ID), ch.Data, ch.Checksum); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	exists, err = c.HasChunk(ctx, string(ch.ID))
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if !exists {
		t.Error("expected true after putting chunk")
	}
}

func TestClient_NodeChunkClient(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	nc := agent.NewNodeChunkClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err := nc.AddNode("node-1", addr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	defer nc.RemoveNode("node-1")

	data := []byte("node-chunk-client-data")
	ch := makeChunk(data)
	ctx := context.Background()

	if err := nc.PutChunk(ctx, "node-1", string(ch.ID), ch.Data); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	got, err := nc.GetChunk(ctx, "node-1", string(ch.ID))
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// Verify unknown node returns an error.
	_, err = nc.GetChunk(ctx, "unknown-node", string(ch.ID))
	if err == nil {
		t.Error("expected error for unknown node, got nil")
	}
}

func TestClient_LocalChunkStore(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c, err := agent.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	store := agent.NewLocalChunkStore(c)
	ctx := context.Background()

	data := []byte("s3-chunk-store-test-data")

	// PutChunkData should return the SHA-256 hex chunk ID.
	chunkID, err := store.PutChunkData(ctx, data)
	if err != nil {
		t.Fatalf("PutChunkData: %v", err)
	}

	h := sha256.Sum256(data)
	expectedID := hex.EncodeToString(h[:])
	if chunkID != expectedID {
		t.Errorf("chunkID = %q, want %q", chunkID, expectedID)
	}

	// GetChunkData should return the original data.
	got, err := store.GetChunkData(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunkData: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// Verify CRC-32C checksum was stored correctly by reading via the raw client.
	_, checksum, err := c.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	expectedChecksum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if checksum != expectedChecksum {
		t.Errorf("checksum = %d, want %d", checksum, expectedChecksum)
	}

	// DeleteChunkData should remove the chunk.
	if err := store.DeleteChunkData(ctx, chunkID); err != nil {
		t.Fatalf("DeleteChunkData: %v", err)
	}

	// Verify deletion via the raw client.
	exists, err := c.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if exists {
		t.Error("expected chunk to not exist after DeleteChunkData")
	}
}
