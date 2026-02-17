package chunk

import (
	"context"
	"os"
	"testing"
)

func setupLocalStore(t *testing.T) (*LocalStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "novastor-chunk-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	store, err := NewLocalStore(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create local store: %v", err)
	}
	return store, func() { os.RemoveAll(dir) }
}

func TestLocalStore_PutGet(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	ctx := context.Background()
	data := []byte("hello chunk world")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("Get data = %q, want %q", got.Data, data)
	}
}

func TestLocalStore_GetNotFound(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	_, err := store.Get(context.Background(), ChunkID("nonexistent"))
	if err == nil {
		t.Error("Get should fail for nonexistent chunk")
	}
}

func TestLocalStore_Delete(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	ctx := context.Background()
	data := []byte("delete me")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)
	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	has, _ := store.Has(ctx, c.ID)
	if has {
		t.Error("chunk should not exist after delete")
	}
}

func TestLocalStore_DeleteNonexistent(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	if err := store.Delete(context.Background(), ChunkID("nonexistent")); err != nil {
		t.Errorf("Delete nonexistent should not error, got: %v", err)
	}
}

func TestLocalStore_Has(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	ctx := context.Background()
	data := []byte("exists")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	has, _ := store.Has(ctx, c.ID)
	if has {
		t.Error("Has should return false before Put")
	}
	_ = store.Put(ctx, c)
	has, _ = store.Has(ctx, c.ID)
	if !has {
		t.Error("Has should return true after Put")
	}
}

func TestLocalStore_List(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	ctx := context.Background()
	ids, _ := store.List(ctx)
	if len(ids) != 0 {
		t.Errorf("List empty store = %d, want 0", len(ids))
	}
	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		c := &Chunk{ID: NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		_ = store.Put(ctx, c)
	}
	ids, _ = store.List(ctx)
	if len(ids) != 3 {
		t.Errorf("List = %d, want 3", len(ids))
	}
}

func TestLocalStore_ChecksumVerifiedOnGet(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()
	ctx := context.Background()
	data := []byte("integrity check")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)
	path := store.chunkPath(c.ID)
	_ = os.WriteFile(path, []byte("corrupted"), 0o644)
	_, err := store.Get(ctx, c.ID)
	if err == nil {
		t.Error("Get should fail on corrupted chunk")
	}
}
