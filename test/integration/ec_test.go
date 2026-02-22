//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/csi"
	"github.com/piwi3910/novastor/internal/metadata"
)

// ecTestChunkClient is an in-memory ChunkClient that simulates multiple
// storage nodes for EC integration testing.
type ecTestChunkClient struct {
	mu     sync.Mutex
	chunks map[string][]byte // key: "nodeID/chunkID"
}

func newECTestChunkClient() *ecTestChunkClient {
	return &ecTestChunkClient{chunks: make(map[string][]byte)}
}

func (c *ecTestChunkClient) key(nodeID, chunkID string) string {
	return nodeID + "/" + chunkID
}

func (c *ecTestChunkClient) PutChunk(_ context.Context, nodeID, chunkID string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chunks[c.key(nodeID, chunkID)] = append([]byte(nil), data...)
	return nil
}

func (c *ecTestChunkClient) GetChunk(_ context.Context, nodeID, chunkID string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.chunks[c.key(nodeID, chunkID)]
	if !ok {
		return nil, fmt.Errorf("chunk %s not found on node %s", chunkID, nodeID)
	}
	return data, nil
}

func (c *ecTestChunkClient) deleteShard(nodeID, chunkID string, shardIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	shardID := string(chunk.ShardID(chunkID, shardIndex))
	delete(c.chunks, c.key(nodeID, shardID))
}

// ecTestMetaStore implements csi.MetadataStore in-memory for integration testing.
type ecTestMetaStore struct {
	mu              sync.Mutex
	volumes         map[string]*metadata.VolumeMeta
	placements      map[string]*metadata.PlacementMap
	shardPlacements map[string][]*metadata.ShardPlacement
}

func newECTestMetaStore() *ecTestMetaStore {
	return &ecTestMetaStore{
		volumes:         make(map[string]*metadata.VolumeMeta),
		placements:      make(map[string]*metadata.PlacementMap),
		shardPlacements: make(map[string][]*metadata.ShardPlacement),
	}
}

func (s *ecTestMetaStore) PutVolumeMeta(_ context.Context, m *metadata.VolumeMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.volumes[m.VolumeID] = m
	return nil
}

func (s *ecTestMetaStore) GetVolumeMeta(_ context.Context, id string) (*metadata.VolumeMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.volumes[id]
	if !ok {
		return nil, fmt.Errorf("volume %s not found", id)
	}
	return v, nil
}

func (s *ecTestMetaStore) DeleteVolumeMeta(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.volumes, id)
	return nil
}

func (s *ecTestMetaStore) ListVolumesMeta(_ context.Context) ([]*metadata.VolumeMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*metadata.VolumeMeta, 0, len(s.volumes))
	for _, v := range s.volumes {
		result = append(result, v)
	}
	return result, nil
}

func (s *ecTestMetaStore) PutPlacementMap(_ context.Context, pm *metadata.PlacementMap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.placements[pm.ChunkID] = pm
	return nil
}

func (s *ecTestMetaStore) GetPlacementMap(_ context.Context, chunkID string) (*metadata.PlacementMap, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pm, ok := s.placements[chunkID]
	if !ok {
		return nil, fmt.Errorf("placement map for %s not found", chunkID)
	}
	return pm, nil
}

func (s *ecTestMetaStore) DeletePlacementMap(_ context.Context, chunkID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.placements, chunkID)
	return nil
}

func (s *ecTestMetaStore) PutShardPlacement(_ context.Context, sp *metadata.ShardPlacement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shardPlacements[sp.ChunkID] = append(s.shardPlacements[sp.ChunkID], sp)
	return nil
}

func (s *ecTestMetaStore) GetShardPlacements(_ context.Context, chunkID string) ([]*metadata.ShardPlacement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shardPlacements[chunkID], nil
}

func (s *ecTestMetaStore) DeleteShardPlacement(_ context.Context, chunkID string, shardIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sps := s.shardPlacements[chunkID]
	for i, sp := range sps {
		if sp.ShardIndex == shardIndex {
			s.shardPlacements[chunkID] = append(sps[:i], sps[i+1:]...)
			return nil
		}
	}
	return nil
}

// TestErasureCodingEndToEnd tests the full EC pipeline:
// write chunk → encode into shards → distribute → read back → degrade → read degraded → fail.
func TestErasureCodingEndToEnd(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
		totalShards  = dataShards + parityShards
	)

	ctx := context.Background()
	client := newECTestChunkClient()
	store := newECTestMetaStore()

	distributor := csi.NewECDistributor(client, store)
	reader := csi.NewECReader(client)

	ec, err := chunk.NewErasureCoder(dataShards, parityShards)
	if err != nil {
		t.Fatalf("NewErasureCoder: %v", err)
	}

	// --- Setup: write a 4MB chunk to the "primary" node ---

	chunkID := "vol-ec-test-chunk-0000"
	volumeID := "vol-ec-test"
	primaryNode := "primary-node"
	shardNodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4", "node-5"}

	originalData := make([]byte, chunk.ChunkSize)
	for i := range originalData {
		originalData[i] = byte((i * 7 + 13) % 256)
	}

	if err := client.PutChunk(ctx, primaryNode, chunkID, originalData); err != nil {
		t.Fatalf("seeding primary chunk: %v", err)
	}

	// --- Step 1: Distribute chunk into EC shards ---

	err = distributor.DistributeChunk(ctx, ec, chunkID, primaryNode, shardNodes, volumeID)
	if err != nil {
		t.Fatalf("DistributeChunk: %v", err)
	}

	// Verify each shard exists on the expected node.
	for i, nodeID := range shardNodes {
		shardID := string(chunk.ShardID(chunkID, i))
		data, err := client.GetChunk(ctx, nodeID, shardID)
		if err != nil {
			t.Errorf("shard %d missing on %s: %v", i, nodeID, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("shard %d on %s is empty", i, nodeID)
		}
	}

	// Verify shard placement metadata.
	sps, err := store.GetShardPlacements(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetShardPlacements: %v", err)
	}
	if len(sps) != totalShards {
		t.Fatalf("expected %d shard placements, got %d", totalShards, len(sps))
	}

	// --- Step 2: Read chunk from all shards (healthy) ---

	reconstructed, err := reader.ReadChunk(ctx, ec, chunkID, shardNodes)
	if err != nil {
		t.Fatalf("healthy ReadChunk: %v", err)
	}
	if !bytes.Equal(reconstructed, originalData) {
		t.Fatal("healthy read: reconstructed data does not match original")
	}

	// --- Step 3: Degraded read (lose 1 parity shard) ---

	client.deleteShard("node-5", chunkID, 5)
	degraded1 := []string{"node-0", "node-1", "node-2", "node-3", "node-4", ""}

	reconstructed, err = reader.ReadChunk(ctx, ec, chunkID, degraded1)
	if err != nil {
		t.Fatalf("degraded-1 ReadChunk: %v", err)
	}
	if !bytes.Equal(reconstructed, originalData) {
		t.Fatal("degraded-1 read: reconstructed data does not match original")
	}

	// --- Step 4: Degraded read (lose 2 shards — max parity tolerance) ---

	client.deleteShard("node-1", chunkID, 1)
	degraded2 := []string{"node-0", "", "node-2", "node-3", "node-4", ""}

	reconstructed, err = reader.ReadChunk(ctx, ec, chunkID, degraded2)
	if err != nil {
		t.Fatalf("degraded-2 ReadChunk: %v", err)
	}
	if !bytes.Equal(reconstructed, originalData) {
		t.Fatal("degraded-2 read: reconstructed data does not match original")
	}

	// --- Step 5: Too many shards lost (3 > parityShards=2) → must fail ---

	client.deleteShard("node-3", chunkID, 3)
	degraded3 := []string{"node-0", "", "node-2", "", "node-4", ""}

	_, err = reader.ReadChunk(ctx, ec, chunkID, degraded3)
	if err == nil {
		t.Fatal("expected error when 3 shards are lost (parity=2)")
	}
}

// TestErasureCodingMultipleChunks verifies EC works across multiple chunks
// in a single volume, simulating a multi-chunk volume provisioning scenario.
func TestErasureCodingMultipleChunks(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
		totalShards  = dataShards + parityShards
		numChunks    = 4
	)

	ctx := context.Background()
	client := newECTestChunkClient()
	store := newECTestMetaStore()

	distributor := csi.NewECDistributor(client, store)
	reader := csi.NewECReader(client)

	ec, err := chunk.NewErasureCoder(dataShards, parityShards)
	if err != nil {
		t.Fatalf("NewErasureCoder: %v", err)
	}

	volumeID := "vol-multi-chunk"
	primaryNode := "primary"
	nodes := []string{"n0", "n1", "n2", "n3", "n4", "n5"}

	// Write and distribute multiple chunks.
	chunkDatas := make([][]byte, numChunks)
	for c := range numChunks {
		chunkID := fmt.Sprintf("%s-chunk-%04d", volumeID, c)
		data := make([]byte, chunk.ChunkSize)
		for i := range data {
			data[i] = byte((i + c*37) % 256)
		}
		chunkDatas[c] = data

		if err := client.PutChunk(ctx, primaryNode, chunkID, data); err != nil {
			t.Fatalf("seeding chunk %d: %v", c, err)
		}

		// Rotate nodes per chunk (simulates round-robin placement).
		rotatedNodes := make([]string, totalShards)
		for r := range totalShards {
			rotatedNodes[r] = nodes[(c+r)%len(nodes)]
		}

		if err := distributor.DistributeChunk(ctx, ec, chunkID, primaryNode, rotatedNodes, volumeID); err != nil {
			t.Fatalf("DistributeChunk %d: %v", c, err)
		}

		// Read back and verify.
		result, err := reader.ReadChunk(ctx, ec, chunkID, rotatedNodes)
		if err != nil {
			t.Fatalf("ReadChunk %d: %v", c, err)
		}
		if !bytes.Equal(result, data) {
			t.Fatalf("chunk %d: reconstructed data does not match original", c)
		}
	}

	// Verify total shard placements.
	totalPlacements := 0
	for c := range numChunks {
		chunkID := fmt.Sprintf("%s-chunk-%04d", volumeID, c)
		sps, _ := store.GetShardPlacements(ctx, chunkID)
		totalPlacements += len(sps)
	}
	if totalPlacements != numChunks*totalShards {
		t.Errorf("expected %d total shard placements, got %d", numChunks*totalShards, totalPlacements)
	}
}
