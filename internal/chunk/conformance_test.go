package chunk

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// StoreConformanceTest runs a comprehensive suite of tests against a Store implementation.
// Any backend implementation should pass this test suite to be considered conformant.
// Note: This test will modify the store. For each test run, create a new store instance.
func StoreConformanceTest(store Store, t *testing.T) {
	t.Run("PutGet", func(t *testing.T) { testPutGet(store, t) })
	t.Run("GetNotFound", func(t *testing.T) { testGetNotFound(store, t) })
	t.Run("Delete", func(t *testing.T) { testDelete(store, t) })
	t.Run("DeleteNonexistent", func(t *testing.T) { testDeleteNonexistent(store, t) })
	t.Run("Has", func(t *testing.T) { testHas(store, t) })
	t.Run("List", func(t *testing.T) { testList(store, t) })
	t.Run("ChecksumVerification", func(t *testing.T) { testChecksumVerification(store, t) })
	t.Run("LargeChunk", func(t *testing.T) { testLargeChunk(store, t) })
	t.Run("EmptyChunk", func(t *testing.T) { testEmptyChunk(store, t) })
	t.Run("DuplicatePut", func(t *testing.T) { testDuplicatePut(store, t) })
	t.Run("ConcurrentOperations", func(t *testing.T) { testConcurrentOperations(store, t) })
	t.Run("ThreadSafety", func(t *testing.T) { testThreadSafety(store, t) })
}

func testPutGet(store Store, t *testing.T) {
	ctx := context.Background()
	data := []byte("test data for put/get")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ID != c.ID {
		t.Errorf("ID mismatch: got %q, want %q", got.ID, c.ID)
	}
	if string(got.Data) != string(c.Data) {
		t.Errorf("Data mismatch: got %q, want %q", got.Data, c.Data)
	}
	if got.Checksum != c.Checksum {
		t.Errorf("Checksum mismatch: got %d, want %d", got.Checksum, c.Checksum)
	}
}

func testGetNotFound(store Store, t *testing.T) {
	ctx := context.Background()
	_, err := store.Get(ctx, ChunkID("nonexistent"))
	if err == nil {
		t.Error("expected error for nonexistent chunk, got nil")
	}
}

func testDelete(store Store, t *testing.T) {
	ctx := context.Background()
	data := []byte("test data for delete")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify the chunk is gone.
	if _, err := store.Get(ctx, c.ID); err == nil {
		t.Error("expected error getting deleted chunk, got nil")
	}
}

func testDeleteNonexistent(store Store, t *testing.T) {
	ctx := context.Background()
	// Deleting a nonexistent chunk should not error.
	if err := store.Delete(ctx, ChunkID("nonexistent")); err != nil {
		t.Errorf("Delete of nonexistent chunk failed: %v", err)
	}
}

func testHas(store Store, t *testing.T) {
	ctx := context.Background()
	data := []byte("test data for has")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]

	// Chunk should not exist initially.
	has, err := store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if has {
		t.Error("expected Has to return false for nonexistent chunk")
	}

	// Put the chunk.
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Chunk should now exist.
	has, err = store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if !has {
		t.Error("expected Has to return true after Put")
	}
}

func testList(store Store, t *testing.T) {
	ctx := context.Background()

	// Get the initial count (store may not be empty from previous tests).
	initialIDs, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	initialCount := len(initialIDs)

	// Add some chunks.
	var expectedIDs []ChunkID
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		c := &Chunk{ID: NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		expectedIDs = append(expectedIDs, c.ID)
	}

	// List should return all chunks.
	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	expectedCount := initialCount + len(expectedIDs)
	if len(ids) != expectedCount {
		t.Errorf("expected %d IDs, got %d", expectedCount, len(ids))
	}

	// Verify all expected IDs are present.
	idMap := make(map[ChunkID]bool)
	for _, id := range ids {
		idMap[id] = true
	}
	for _, expectedID := range expectedIDs {
		if !idMap[expectedID] {
			t.Errorf("expected ID %q not found in list", expectedID)
		}
	}
}

func testChecksumVerification(store Store, t *testing.T) {
	ctx := context.Background()
	data := []byte("test data for checksum")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the chunk and verify checksum.
	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if err := got.VerifyChecksum(); err != nil {
		t.Errorf("checksum verification failed: %v", err)
	}
}

func testLargeChunk(store Store, t *testing.T) {
	ctx := context.Background()

	// Create a chunk larger than ChunkSize.
	// Some store implementations (e.g. BlockStore) reject oversized chunks,
	// which is valid behavior. The test verifies that either:
	// 1. The store accepts and correctly round-trips the data, or
	// 2. The store rejects the oversized chunk with an error.
	data := make([]byte, ChunkSize+1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Logf("store rejected oversized chunk (valid behavior): %v", err)
		return
	}

	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(got.Data) != len(data) {
		t.Errorf("data size mismatch: got %d, want %d", len(got.Data), len(data))
	}

	if err := got.VerifyChecksum(); err != nil {
		t.Errorf("checksum verification failed: %v", err)
	}
}

func testEmptyChunk(store Store, t *testing.T) {
	ctx := context.Background()
	data := []byte{}

	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(got.Data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(got.Data))
	}
}

func testDuplicatePut(store Store, t *testing.T) {
	ctx := context.Background()
	data := []byte("test data for duplicate put")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]

	// Put the same chunk twice.
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	// Verify the chunk is still accessible.
	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got.Data) != string(c.Data) {
		t.Errorf("Data mismatch after duplicate Put")
	}
}

func testConcurrentOperations(store Store, t *testing.T) {
	ctx := context.Background()
	const numGoroutines = 10
	const numChunksPerGoroutine = 10

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*numChunksPerGoroutine)

	// Concurrent writes. Note: some stores have limited capacity (e.g.
	// BlockStore allocates a full chunk slot per write regardless of data
	// size), so "no free space" errors are expected and not failures.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numChunksPerGoroutine; j++ {
				data := []byte{byte(goroutineID), byte(j)}
				c := &Chunk{
					ID:   NewChunkID(data),
					Data: data,
				}
				c.Checksum = c.ComputeChecksum()
				if err := store.Put(ctx, c); err != nil {
					if strings.Contains(err.Error(), "no free space") {
						continue
					}
					errCh <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent Put failed: %v", err)
	}
}

func testThreadSafety(store Store, t *testing.T) {
	ctx := context.Background()
	const numOperations = 100
	done := make(chan bool)

	// Start multiple goroutines performing various operations.
	for i := 0; i < 5; i++ {
		go func(goroutineID int) {
			for j := 0; j < numOperations; j++ {
				data := []byte{byte(goroutineID), byte(j)}
				id := NewChunkID(data)

				switch j % 4 {
				case 0:
					c := &Chunk{ID: id, Data: data}
					c.Checksum = c.ComputeChecksum()
					store.Put(ctx, c)
				case 1:
					store.Get(ctx, id)
				case 2:
					store.Has(ctx, id)
				case 3:
					store.List(ctx)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete.
	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("thread safety test timed out")
		}
	}
}
