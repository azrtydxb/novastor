package chunk

import (
	"context"
	"testing"
)

func TestMemoryStore_PutGet(t *testing.T) {
	store := NewMemoryStore()
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

func TestMemoryStore_GetNotFound(t *testing.T) {
	store := NewMemoryStore()
	_, err := store.Get(context.Background(), ChunkID("nonexistent"))
	if err == nil {
		t.Error("Get should fail for nonexistent chunk")
	}
}

func TestMemoryStore_Delete(t *testing.T) {
	store := NewMemoryStore()
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

func TestMemoryStore_DeleteNonexistent(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Delete(context.Background(), ChunkID("nonexistent")); err != nil {
		t.Errorf("Delete nonexistent should not error, got: %v", err)
	}
}

func TestMemoryStore_Has(t *testing.T) {
	store := NewMemoryStore()
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

func TestMemoryStore_List(t *testing.T) {
	store := NewMemoryStore()
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

func TestMemoryStore_Close(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	data := []byte("close test")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)

	if err := store.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// After close, store should be empty.
	ids, _ := store.List(ctx)
	if len(ids) != 0 {
		t.Errorf("List after Close = %d, want 0", len(ids))
	}
}

func TestMemoryStore_ImplementsCapacityStore(t *testing.T) {
	var _ CapacityStore = &MemoryStore{}
}

func TestMemoryStore_Stats(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Stats on empty store.
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.TotalBytes <= 0 {
		t.Errorf("TotalBytes = %d, want > 0", stats.TotalBytes)
	}
	if stats.ChunkCount != 0 {
		t.Errorf("ChunkCount = %d, want 0", stats.ChunkCount)
	}

	// Add some chunks and verify stats update.
	for i := 0; i < 5; i++ {
		data := make([]byte, 1024*1024) // 1MB chunks
		for j := range data {
			data[j] = byte(i + j)
		}
		c := &Chunk{ID: NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	stats, err = store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after adding chunks failed: %v", err)
	}
	if stats.ChunkCount != 5 {
		t.Errorf("ChunkCount = %d, want 5", stats.ChunkCount)
	}
	if stats.UsedBytes < 5*1024*1024 {
		t.Errorf("UsedBytes = %d, want >= %d", stats.UsedBytes, 5*1024*1024)
	}
	if stats.AvailableBytes != stats.TotalBytes-stats.UsedBytes {
		t.Errorf("AvailableBytes = %d, TotalBytes-UsedBytes = %d", stats.AvailableBytes, stats.TotalBytes-stats.UsedBytes)
	}
}

func TestMemoryStore_ImplementsChunkMetaStore(t *testing.T) {
	var _ ChunkMetaStore = &MemoryStore{}
}

func TestMemoryStore_GetMeta(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	data := []byte("test metadata without loading full chunk")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	meta, err := store.GetMeta(ctx, c.ID)
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if meta.ID != c.ID {
		t.Errorf("GetMeta ID = %s, want %s", meta.ID, c.ID)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("GetMeta Size = %d, want %d", meta.Size, len(data))
	}
	if meta.Checksum != c.Checksum {
		t.Errorf("GetMeta Checksum = %d, want %d", meta.Checksum, c.Checksum)
	}
}

func TestMemoryStore_GetMetaNotFound(t *testing.T) {
	store := NewMemoryStore()
	_, err := store.GetMeta(context.Background(), ChunkID("nonexistent"))
	if err == nil {
		t.Error("GetMeta should fail for nonexistent chunk")
	}
}

func TestMemoryStore_ImplementsHealthCheckStore(t *testing.T) {
	var _ HealthCheckStore = &MemoryStore{}
}

func TestMemoryStore_HealthCheck(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	if err := store.HealthCheck(ctx); err != nil {
		t.Errorf("HealthCheck failed: %v", err)
	}
}

func TestMemoryStore_DeleteUpdatesStats(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Add a chunk.
	data := make([]byte, 1024*1024)
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)

	stats, _ := store.Stats(ctx)
	if stats.UsedBytes == 0 {
		t.Error("UsedBytes should be > 0 after Put")
	}

	// Delete the chunk.
	_ = store.Delete(ctx, c.ID)

	stats, _ = store.Stats(ctx)
	if stats.UsedBytes != 0 {
		t.Errorf("UsedBytes = %d, want 0 after Delete", stats.UsedBytes)
	}
}

func TestMemoryStore_PutReplacesExisting(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	data1 := make([]byte, 1024)
	c1 := &Chunk{ID: "test-chunk", Data: data1}
	c1.Checksum = c1.ComputeChecksum()
	_ = store.Put(ctx, c1)

	stats, _ := store.Stats(ctx)
	usedAfterFirstPut := stats.UsedBytes

	// Replace with larger data.
	data2 := make([]byte, 2048)
	c2 := &Chunk{ID: "test-chunk", Data: data2}
	c2.Checksum = c2.ComputeChecksum()
	_ = store.Put(ctx, c2)

	stats, _ = store.Stats(ctx)
	if stats.UsedBytes != usedAfterFirstPut+1024 {
		t.Errorf("UsedBytes = %d, want %d (increase by size of new chunk)", stats.UsedBytes, usedAfterFirstPut+1024)
	}
}
