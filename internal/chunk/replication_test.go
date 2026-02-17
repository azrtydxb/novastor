package chunk

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func setupReplicationTest(t *testing.T, nodeCount int) ([]*LocalStore, *ReplicationManager, func()) {
	t.Helper()
	dirs := make([]string, nodeCount)
	stores := make([]*LocalStore, nodeCount)
	nodeStores := make(map[string]Store)
	for i := 0; i < nodeCount; i++ {
		dir, err := os.MkdirTemp("", "novastor-repl-test-*")
		if err != nil {
			t.Fatal(err)
		}
		dirs[i] = dir
		store, err := NewLocalStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
		nodeStores[fmt.Sprintf("node-%d", i)] = store
	}
	rm := NewReplicationManager(nodeStores)
	cleanup := func() {
		for _, d := range dirs {
			os.RemoveAll(d)
		}
	}
	return stores, rm, cleanup
}

func TestReplicationManager_WriteQuorum(t *testing.T) {
	_, rm, cleanup := setupReplicationTest(t, 3)
	defer cleanup()
	ctx := context.Background()
	data := []byte("replicated data")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	nodes := []string{"node-0", "node-1", "node-2"}
	if err := rm.Replicate(ctx, c, nodes, 2); err != nil {
		t.Fatalf("Replicate failed: %v", err)
	}
	for _, node := range nodes {
		has, _ := rm.stores[node].Has(ctx, c.ID)
		if !has {
			t.Errorf("chunk should exist on %s", node)
		}
	}
}

func TestReplicationManager_ReadFromAny(t *testing.T) {
	_, rm, cleanup := setupReplicationTest(t, 3)
	defer cleanup()
	ctx := context.Background()
	data := []byte("read from any replica")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	nodes := []string{"node-0", "node-1", "node-2"}
	_ = rm.Replicate(ctx, c, nodes, 2)
	got, err := rm.ReadFromAny(ctx, c.ID, nodes)
	if err != nil {
		t.Fatalf("ReadFromAny failed: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", got.Data, data)
	}
}

func TestReplicationManager_ReadFromAny_SkipsCorrupted(t *testing.T) {
	stores, rm, cleanup := setupReplicationTest(t, 3)
	defer cleanup()
	ctx := context.Background()
	data := []byte("only one good copy")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	nodes := []string{"node-0", "node-1", "node-2"}
	_ = rm.Replicate(ctx, c, nodes, 2)
	_ = os.WriteFile(stores[0].chunkPath(c.ID), []byte("bad"), 0o644)
	_ = os.WriteFile(stores[1].chunkPath(c.ID), []byte("bad"), 0o644)
	got, err := rm.ReadFromAny(ctx, c.ID, nodes)
	if err != nil {
		t.Fatalf("ReadFromAny should succeed from node-2: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("data mismatch from healthy replica")
	}
}

func TestReplicationManager_ReadFromAny_AllCorrupted(t *testing.T) {
	stores, rm, cleanup := setupReplicationTest(t, 2)
	defer cleanup()
	ctx := context.Background()
	data := []byte("all bad")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	nodes := []string{"node-0", "node-1"}
	_ = rm.Replicate(ctx, c, nodes, 2)
	_ = os.WriteFile(stores[0].chunkPath(c.ID), []byte("bad"), 0o644)
	_ = os.WriteFile(stores[1].chunkPath(c.ID), []byte("bad"), 0o644)
	_, err := rm.ReadFromAny(ctx, c.ID, nodes)
	if err == nil {
		t.Error("ReadFromAny should fail when all replicas are corrupted")
	}
}

func TestReplicationManager_QuorumNotMet(t *testing.T) {
	_, rm, cleanup := setupReplicationTest(t, 1)
	defer cleanup()
	ctx := context.Background()
	data := []byte("need more nodes")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	err := rm.Replicate(ctx, c, []string{"node-0"}, 2)
	if err == nil {
		t.Error("Replicate should fail when quorum cannot be met")
	}
}
