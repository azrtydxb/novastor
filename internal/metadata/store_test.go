package metadata

import (
	"context"
	"os"
	"testing"
	"time"
)

func setupTestStore(t *testing.T) (*RaftStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "novastor-meta-test-*")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewRaftStore(RaftConfig{
		NodeID:   "test-node",
		DataDir:  dir,
		RaftAddr: "127.0.0.1:0",
		Backend:  "memory",
	})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewRaftStore failed: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsLeader() {
		store.Close()
		os.RemoveAll(dir)
		t.Fatal("node did not become leader")
	}
	return store, func() {
		store.Close()
		os.RemoveAll(dir)
	}
}

func TestRaftStore_PutGetVolumeMeta(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	meta := &VolumeMeta{
		VolumeID:  "vol-1",
		Pool:      "fast-pool",
		SizeBytes: 1024 * 1024 * 1024,
		ChunkIDs:  []string{"chunk-a", "chunk-b"},
	}
	if err := store.PutVolumeMeta(ctx, meta); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}
	got, err := store.GetVolumeMeta(ctx, "vol-1")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}
	if got.VolumeID != "vol-1" || got.Pool != "fast-pool" || len(got.ChunkIDs) != 2 {
		t.Errorf("unexpected volume meta: %+v", got)
	}
}

func TestRaftStore_GetVolumeMetaNotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	_, err := store.GetVolumeMeta(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent volume")
	}
}

func TestRaftStore_DeleteVolumeMeta(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	meta := &VolumeMeta{VolumeID: "vol-del", Pool: "pool"}
	_ = store.PutVolumeMeta(ctx, meta)
	if err := store.DeleteVolumeMeta(ctx, "vol-del"); err != nil {
		t.Fatalf("DeleteVolumeMeta failed: %v", err)
	}
	_, err := store.GetVolumeMeta(ctx, "vol-del")
	if err == nil {
		t.Error("volume should not exist after delete")
	}
}

func TestRaftStore_PutGetPlacementMap(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	pm := &PlacementMap{
		ChunkID: "chunk-1",
		Nodes:   []string{"node-a", "node-b", "node-c"},
	}
	if err := store.PutPlacementMap(ctx, pm); err != nil {
		t.Fatalf("PutPlacementMap failed: %v", err)
	}
	got, err := store.GetPlacementMap(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("GetPlacementMap failed: %v", err)
	}
	if len(got.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(got.Nodes))
	}
}
