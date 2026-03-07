package csi

import (
	"bytes"
	"context"
	"testing"

	"github.com/azrtydxb/novastor/internal/chunk"
)

// seedShards encodes data and writes shards to the mock client.
// Returns the shard node IDs used.
func seedShards(t *testing.T, client *ecMockChunkClient, ec *chunk.ErasureCoder, chunkID string, data []byte, nodes []string) {
	t.Helper()
	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for i, nodeID := range nodes {
		shardID := string(chunk.ShardID(chunkID, i))
		if err := client.PutChunk(context.Background(), nodeID, shardID, shards[i]); err != nil {
			t.Fatalf("seeding shard %d: %v", i, err)
		}
	}
}

func TestECReader_AllShardsAvailable(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	client := newECMockChunkClient()
	reader := NewECReader(client)

	chunkID := "vol-test-chunk-0000"
	data := make([]byte, chunk.ChunkSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	seedShards(t, client, ec, chunkID, data, nodes)

	result, err := reader.ReadChunk(context.Background(), ec, chunkID, nodes)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Error("reconstructed data does not match original")
	}
}

func TestECReader_DegradedRead(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	client := newECMockChunkClient()
	reader := NewECReader(client)

	chunkID := "vol-test-chunk-0001"
	data := make([]byte, chunk.ChunkSize)
	for i := range data {
		data[i] = byte(i % 173)
	}

	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	seedShards(t, client, ec, chunkID, data, nodes)

	// Mark 2 nodes as unavailable (within parity tolerance).
	degradedNodes := []string{"n1", "", "n3", "n4", "", "n6"}

	result, err := reader.ReadChunk(context.Background(), ec, chunkID, degradedNodes)
	if err != nil {
		t.Fatalf("degraded ReadChunk: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Error("degraded read: reconstructed data does not match original")
	}
}

func TestECReader_TooManyMissing(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	client := newECMockChunkClient()
	reader := NewECReader(client)

	chunkID := "vol-test-chunk-0002"
	data := make([]byte, chunk.ChunkSize)

	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	seedShards(t, client, ec, chunkID, data, nodes)

	// Mark 3 nodes as unavailable (exceeds parity=2).
	degradedNodes := []string{"", "", "", "n4", "n5", "n6"}

	_, err := reader.ReadChunk(context.Background(), ec, chunkID, degradedNodes)
	if err == nil {
		t.Error("expected error when too many shards are missing")
	}
}

func TestECReader_WrongNodeCount(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	reader := NewECReader(newECMockChunkClient())

	_, err := reader.ReadChunk(context.Background(), ec, "c1", []string{"a", "b", "c"})
	if err == nil {
		t.Error("expected error for wrong shard node count")
	}
}

func TestECReader_NodeReadFailure(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	client := newECMockChunkClient()
	reader := NewECReader(client)

	chunkID := "vol-test-chunk-0003"
	data := make([]byte, chunk.ChunkSize)
	for i := range data {
		data[i] = byte(i % 97)
	}

	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	seedShards(t, client, ec, chunkID, data, nodes)

	// Delete 2 shards from the client to simulate node failures.
	client.mu.Lock()
	delete(client.chunks, client.chunkKey("n2", string(chunk.ShardID(chunkID, 1))))
	delete(client.chunks, client.chunkKey("n5", string(chunk.ShardID(chunkID, 4))))
	client.mu.Unlock()

	// Should still succeed — 4 of 6 shards available (exactly dataShards).
	result, err := reader.ReadChunk(context.Background(), ec, chunkID, nodes)
	if err != nil {
		t.Fatalf("ReadChunk with 2 failed nodes: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Error("reconstructed data does not match original after node failures")
	}
}
