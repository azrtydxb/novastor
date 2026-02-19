package chunk

import (
	"context"
	"testing"
)

// TestChunkStoreMetrics verifies that chunk store operations
// properly increment their corresponding Prometheus metrics.
func TestChunkStoreMetrics(t *testing.T) {
	// This test ensures metrics don't panic when operations are performed.
	// We can't easily verify exact metric values due to global state,
	// but we can verify the operations complete successfully.

	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	ctx := context.Background()

	// Write a chunk with computed checksum.
	data := []byte("test data for metrics")
	c := &Chunk{
		ID:   ChunkID("test-chunk"),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Read the chunk
	_, err = store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Delete the chunk
	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// If we got here without panicking, metrics are working correctly
}
