package csi

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/piwi3910/novastor/internal/chunk"
)

// ecMockChunkClient is an in-memory ChunkClient for EC tests.
type ecMockChunkClient struct {
	mu     sync.Mutex
	chunks map[string][]byte // key: "nodeID/chunkID"
}

func newECMockChunkClient() *ecMockChunkClient {
	return &ecMockChunkClient{chunks: make(map[string][]byte)}
}

func (m *ecMockChunkClient) chunkKey(nodeID, chunkID string) string {
	return nodeID + "/" + chunkID
}

func (m *ecMockChunkClient) PutChunk(_ context.Context, nodeID, chunkID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks[m.chunkKey(nodeID, chunkID)] = append([]byte(nil), data...)
	return nil
}

func (m *ecMockChunkClient) GetChunk(_ context.Context, nodeID, chunkID string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.chunks[m.chunkKey(nodeID, chunkID)]
	if !ok {
		return nil, fmt.Errorf("chunk %s not found on node %s", chunkID, nodeID)
	}
	return data, nil
}

func TestECDistributor_DistributeChunk(t *testing.T) {
	ec, err := chunk.NewErasureCoder(4, 2)
	if err != nil {
		t.Fatalf("NewErasureCoder: %v", err)
	}

	client := newECMockChunkClient()
	store := newMockMetadataStore()
	dist := NewECDistributor(client, store)

	// Place a 4MB chunk on the source node.
	chunkData := make([]byte, chunk.ChunkSize)
	for i := range chunkData {
		chunkData[i] = byte(i % 251)
	}
	sourceNode := "node-0"
	chunkID := "vol-test-chunk-0000"
	if err := client.PutChunk(context.Background(), sourceNode, chunkID, chunkData); err != nil {
		t.Fatalf("seeding chunk: %v", err)
	}

	shardNodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"}

	err = dist.DistributeChunk(context.Background(), ec, chunkID, sourceNode, shardNodes, "vol-test")
	if err != nil {
		t.Fatalf("DistributeChunk: %v", err)
	}

	// Verify each shard was written to the correct node.
	for i, nodeID := range shardNodes {
		shardID := string(chunk.ShardID(chunkID, i))
		data, err := client.GetChunk(context.Background(), nodeID, shardID)
		if err != nil {
			t.Errorf("shard %d not found on node %s: %v", i, nodeID, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("shard %d on node %s has empty data", i, nodeID)
		}
	}

	// Verify shard placement metadata was written.
	for i, nodeID := range shardNodes {
		sps, err := store.GetShardPlacements(context.Background(), chunkID)
		if err != nil {
			t.Fatalf("GetShardPlacements: %v", err)
		}
		found := false
		for _, sp := range sps {
			if sp.ShardIndex == i && sp.NodeID == nodeID && sp.VolumeID == "vol-test" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("shard placement not found for shard %d on node %s", i, nodeID)
		}
	}

	// Verify shards can be decoded back to original data.
	shards := make([][]byte, 6)
	for i, nodeID := range shardNodes {
		shardID := string(chunk.ShardID(chunkID, i))
		shards[i], _ = client.GetChunk(context.Background(), nodeID, shardID)
	}
	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(decoded, chunkData) {
		t.Error("decoded data does not match original")
	}
}

func TestECDistributor_WrongShardCount(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	client := newECMockChunkClient()
	store := newMockMetadataStore()
	dist := NewECDistributor(client, store)

	// Seed a chunk.
	_ = client.PutChunk(context.Background(), "src", "c1", make([]byte, 64))

	// Wrong number of shard nodes (5 instead of 6).
	err := dist.DistributeChunk(context.Background(), ec, "c1", "src", []string{"a", "b", "c", "d", "e"}, "vol")
	if err == nil {
		t.Error("expected error for wrong shard node count")
	}
}

func TestECDistributor_SourceChunkMissing(t *testing.T) {
	ec, _ := chunk.NewErasureCoder(4, 2)
	client := newECMockChunkClient()
	store := newMockMetadataStore()
	dist := NewECDistributor(client, store)

	// Don't seed any chunk — source is missing.
	err := dist.DistributeChunk(context.Background(), ec, "missing", "src", []string{"a", "b", "c", "d", "e", "f"}, "vol")
	if err == nil {
		t.Error("expected error for missing source chunk")
	}
}
