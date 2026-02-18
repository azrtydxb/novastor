package chunk

import (
	"os"
	"testing"
)

func setupRefCounter(t *testing.T) (*ChunkRefCounter, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "novastor-refcounter-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	rc, err := NewChunkRefCounter(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to create refcounter: %v", err)
	}
	return rc, func() { _ = os.RemoveAll(dir) }
}

func TestChunkRefCounter_Increment(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("abc123")

	// First increment should set count to 1
	if err := rc.Increment(id); err != nil {
		t.Fatalf("first Increment failed: %v", err)
	}
	if got := rc.Get(id); got != 1 {
		t.Errorf("after first Increment, count = %d, want 1", got)
	}

	// Second increment should set count to 2
	if err := rc.Increment(id); err != nil {
		t.Fatalf("second Increment failed: %v", err)
	}
	if got := rc.Get(id); got != 2 {
		t.Errorf("after second Increment, count = %d, want 2", got)
	}
}

func TestChunkRefCounter_Decrement(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("xyz789")

	// Increment twice
	if err := rc.Increment(id); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	// Decrement once
	newCount, err := rc.Decrement(id)
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if newCount != 1 {
		t.Errorf("after Decrement, count = %d, want 1", newCount)
	}
	if got := rc.Get(id); got != 1 {
		t.Errorf("Get after Decrement = %d, want 1", got)
	}

	// Decrement again - should reach zero
	newCount, err = rc.Decrement(id)
	if err != nil {
		t.Fatalf("second Decrement failed: %v", err)
	}
	if newCount != 0 {
		t.Errorf("after final Decrement, count = %d, want 0", newCount)
	}
	if got := rc.Get(id); got != 0 {
		t.Errorf("Get after final Decrement = %d, want 0", got)
	}
}

func TestChunkRefCounter_DecrementNonExistent(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("nonexistent")
	_, err := rc.Decrement(id)
	if err == nil {
		t.Error("Decrement on non-existent chunk should fail")
	}
}

func TestChunkRefCounter_DecrementZero(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("once")
	if err := rc.Increment(id); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if _, err := rc.Decrement(id); err != nil {
		t.Fatalf("Decrement failed: %v", err)
	} // Now at 0, removed from map

	// Decrement again should fail
	_, err := rc.Decrement(id)
	if err == nil {
		t.Error("Decrement on zero-count chunk should fail")
	}
}

func TestChunkRefCounter_Add(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("addtest")

	// Add 5
	if err := rc.Add(id, 5); err != nil {
		t.Fatalf("Add 5 failed: %v", err)
	}
	if got := rc.Get(id); got != 5 {
		t.Errorf("after Add 5, count = %d, want 5", got)
	}

	// Subtract 2
	if err := rc.Add(id, -2); err != nil {
		t.Fatalf("Add -2 failed: %v", err)
	}
	if got := rc.Get(id); got != 3 {
		t.Errorf("after Add -2, count = %d, want 3", got)
	}
}

func TestChunkRefCounter_AddNegativeNotAllowed(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("negtest")
	if err := rc.Increment(id); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	// Try to subtract more than current count
	err := rc.Add(id, -5)
	if err == nil {
		t.Error("Add with negative delta that would go below zero should fail")
	}
	// Count should remain at 1
	if got := rc.Get(id); got != 1 {
		t.Errorf("after failed Add, count = %d, want 1", got)
	}
}

func TestChunkRefCounter_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "novastor-refcounter-persist-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Create first refcounter, add some counts
	id1 := ChunkID("persist1")
	id2 := ChunkID("persist2")

	rc1, err := NewChunkRefCounter(dir)
	if err != nil {
		t.Fatalf("failed to create first refcounter: %v", err)
	}
	if err := rc1.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc1.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc1.Increment(id2); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	// Create second refcounter from same directory - should load persisted counts
	rc2, err := NewChunkRefCounter(dir)
	if err != nil {
		t.Fatalf("failed to create second refcounter: %v", err)
	}

	if got := rc2.Get(id1); got != 2 {
		t.Errorf("persisted count for id1 = %d, want 2", got)
	}
	if got := rc2.Get(id2); got != 1 {
		t.Errorf("persisted count for id2 = %d, want 1", got)
	}
}

func TestChunkRefCounter_Count(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	if got := rc.Count(); got != 0 {
		t.Errorf("initial Count = %d, want 0", got)
	}

	if err := rc.Increment(ChunkID("a")); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(ChunkID("b")); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(ChunkID("c")); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(ChunkID("a")); err != nil {
		t.Fatalf("Increment failed: %v", err)
	} // Duplicate, shouldn't increase Count

	if got := rc.Count(); got != 3 {
		t.Errorf("Count after 3 unique chunks = %d, want 3", got)
	}
}

func TestChunkRefCounter_TotalReferences(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	if got := rc.TotalReferences(); got != 0 {
		t.Errorf("initial TotalReferences = %d, want 0", got)
	}

	id1 := ChunkID("total1")
	id2 := ChunkID("total2")

	if err := rc.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id2); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	if got := rc.TotalReferences(); got != 3 {
		t.Errorf("TotalReferences = %d, want 3", got)
	}

	if _, err := rc.Decrement(id1); err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}

	if got := rc.TotalReferences(); got != 2 {
		t.Errorf("TotalReferences after Decrement = %d, want 2", got)
	}
}

func TestChunkRefCounter_ForEach(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id1 := ChunkID("each1")
	id2 := ChunkID("each2")
	id3 := ChunkID("each3")

	if err := rc.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id2); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id3); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	visited := make(map[ChunkID]bool)
	err := rc.ForEach(func(id ChunkID, count uint32) error {
		visited[id] = true
		if count != rc.Get(id) {
			t.Errorf("ForEach count mismatch for %s: got %d, want %d", id, count, rc.Get(id))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}

	if !visited[id1] || !visited[id2] || !visited[id3] {
		t.Error("ForEach did not visit all expected IDs")
	}
}

func TestChunkRefCounter_SnapshotRestore(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id1 := ChunkID("snap1")
	id2 := ChunkID("snap2")

	if err := rc.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id1); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if err := rc.Increment(id2); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	snapshot, err := rc.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Create new refcounter and restore
	dir, err := os.MkdirTemp("", "novastor-refcounter-snap-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	rc2, err := NewChunkRefCounter(dir)
	if err != nil {
		t.Fatalf("failed to create second refcounter: %v", err)
	}

	if err := rc2.Restore(snapshot); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if got := rc2.Get(id1); got != 2 {
		t.Errorf("restored count for id1 = %d, want 2", got)
	}
	if got := rc2.Get(id2); got != 1 {
		t.Errorf("restored count for id2 = %d, want 1", got)
	}
	if got := rc2.Count(); got != 2 {
		t.Errorf("restored Count = %d, want 2", got)
	}
}

func TestChunkRefCounter_ConcurrentAccess(t *testing.T) {
	rc, cleanup := setupRefCounter(t)
	defer cleanup()

	id := ChunkID("concurrent")
	done := make(chan bool)

	// Run multiple goroutines incrementing
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = rc.Increment(id) //nolint:errcheck // Errors in concurrent test are acceptable
			}
			done <- true
		}()
	}

	// Wait for all to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	if got := rc.Get(id); got != 1000 {
		t.Errorf("concurrent increments resulted in count = %d, want 1000", got)
	}
}
