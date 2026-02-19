package agent

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/chunk"
)

// mockMetadataClient mocks the metadata client for testing.
type mockMetadataClient struct {
	referencedChunks map[string]struct{}
}

func (m *mockMetadataClient) GetAllReferencedChunks(_ context.Context) (map[string]struct{}, error) {
	return m.referencedChunks, nil
}

// mockStore mocks the chunk store for testing.
type mockStore struct {
	chunks map[string]*chunk.Chunk
}

func (m *mockStore) Put(_ context.Context, c *chunk.Chunk) error {
	m.chunks[string(c.ID)] = c
	return nil
}

func (m *mockStore) Get(_ context.Context, id chunk.ChunkID) (*chunk.Chunk, error) {
	c, ok := m.chunks[string(id)]
	if !ok {
		return nil, fmt.Errorf("chunk %s not found", id)
	}
	return c, nil
}

func (m *mockStore) Delete(_ context.Context, id chunk.ChunkID) error {
	delete(m.chunks, string(id))
	return nil
}

func (m *mockStore) Has(_ context.Context, id chunk.ChunkID) (bool, error) {
	_, ok := m.chunks[string(id)]
	return ok, nil
}

func (m *mockStore) List(_ context.Context) ([]chunk.ChunkID, error) {
	var ids []chunk.ChunkID
	for id := range m.chunks {
		ids = append(ids, chunk.ChunkID(id))
	}
	return ids, nil
}

func TestGarbageCollector_RunOnce(t *testing.T) {
	ctx := context.Background()

	// Create a mock store with local chunks.
	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	localChunks := []string{"chunk-1", "chunk-2", "chunk-3", "chunk-orphan-1", "chunk-orphan-2"}
	for _, id := range localChunks {
		store.chunks[id] = &chunk.Chunk{
			ID:       chunk.ChunkID(id),
			Data:     []byte("test data"),
			Checksum: 0,
		}
	}

	// Create a mock metadata client with referenced chunks.
	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{
			"chunk-1": {},
			"chunk-2": {},
			"chunk-3": {},
			// "chunk-orphan-1" and "chunk-orphan-2" are not referenced
		},
	}

	// Create a GC with a mock adapter.
	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
		batchSize:  100,
	}

	// Run GC once.
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify that orphan chunks were deleted.
	if result.OrphanChunksDeleted != 2 {
		t.Errorf("expected 2 orphan chunks deleted, got %d", result.OrphanChunksDeleted)
	}

	// Verify that orphan chunks are gone from the store.
	for _, orphanID := range []string{"chunk-orphan-1", "chunk-orphan-2"} {
		if _, ok := store.chunks[orphanID]; ok {
			t.Errorf("orphan chunk %s should have been deleted", orphanID)
		}
	}

	// Verify that referenced chunks still exist.
	for _, refID := range []string{"chunk-1", "chunk-2", "chunk-3"} {
		if _, ok := store.chunks[refID]; !ok {
			t.Errorf("referenced chunk %s should still exist", refID)
		}
	}
}

func TestGarbageCollector_NoOrphans(t *testing.T) {
	ctx := context.Background()

	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	localChunks := []string{"chunk-1", "chunk-2"}
	for _, id := range localChunks {
		store.chunks[id] = &chunk.Chunk{
			ID:       chunk.ChunkID(id),
			Data:     []byte("test data"),
			Checksum: 0,
		}
	}

	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{
			"chunk-1": {},
			"chunk-2": {},
		},
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
	}

	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	if result.OrphanChunksDeleted != 0 {
		t.Errorf("expected 0 orphan chunks deleted, got %d", result.OrphanChunksDeleted)
	}

	// Verify all chunks still exist.
	for _, id := range localChunks {
		if _, ok := store.chunks[id]; !ok {
			t.Errorf("chunk %s should still exist", id)
		}
	}
}

func TestGarbageCollector_AllOrphans(t *testing.T) {
	ctx := context.Background()

	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	localChunks := []string{"chunk-orphan-1", "chunk-orphan-2", "chunk-orphan-3"}
	for _, id := range localChunks {
		store.chunks[id] = &chunk.Chunk{
			ID:       chunk.ChunkID(id),
			Data:     []byte("test data"),
			Checksum: 0,
		}
	}

	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{}, // No referenced chunks
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
	}

	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	if result.OrphanChunksDeleted != 3 {
		t.Errorf("expected 3 orphan chunks deleted, got %d", result.OrphanChunksDeleted)
	}

	// Verify all chunks were deleted.
	for _, id := range localChunks {
		if _, ok := store.chunks[id]; ok {
			t.Errorf("orphan chunk %s should have been deleted", id)
		}
	}
}

func TestGarbageCollector_EmptyStore(t *testing.T) {
	ctx := context.Background()

	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{},
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
	}

	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	if result.OrphanChunksDeleted != 0 {
		t.Errorf("expected 0 orphan chunks deleted, got %d", result.OrphanChunksDeleted)
	}
}

func TestGarbageCollector_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	// Create many chunks to test cancellation.
	for i := 0; i < 1000; i++ {
		id := chunk.ChunkID(strconv.Itoa(i))
		store.chunks[string(id)] = &chunk.Chunk{
			ID:       id,
			Data:     []byte("test data"),
			Checksum: 0,
		}
	}

	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{},
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
	}

	// Cancel context immediately.
	cancel()

	// Run GC - should handle cancellation gracefully.
	_, err := gc.RunOnce(ctx)
	if err != nil && err != context.Canceled {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}
}

func TestGarbageCollector_LastResult(t *testing.T) {
	ctx := context.Background()

	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	store.chunks["chunk-1"] = &chunk.Chunk{
		ID:       chunk.ChunkID("chunk-1"),
		Data:     []byte("test data"),
		Checksum: 0,
	}

	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{}, // All orphans
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
	}

	// Initially, no last result.
	if result := gc.LastResult(); result != nil {
		t.Error("expected no last result initially, got non-nil")
	}

	// Run GC and verify result is stored.
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	lastResult := gc.LastResult()
	if lastResult == nil {
		t.Error("expected last result to be non-nil after GC run")
	} else if lastResult.OrphanChunksDeleted != result.OrphanChunksDeleted {
		t.Errorf("last result mismatch: got %d, want %d",
			lastResult.OrphanChunksDeleted, result.OrphanChunksDeleted)
	}
}

func TestGarbageCollector_StartStop(_ *testing.T) {
	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	metaClient := &mockMetadataClient{
		referencedChunks: map[string]struct{}{},
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start GC in background.
	gc.Start(ctx)

	// Wait a bit then stop.
	time.Sleep(150 * time.Millisecond)
	gc.Stop()
	cancel()

	// Verify GC stopped without errors.
	// (If there's a panic or deadlock, the test will fail)
}

// BenchmarkGarbageCollector measures the performance of agent GC operations.
func BenchmarkGarbageCollector(b *testing.B) {
	ctx := context.Background()

	store := &mockStore{
		chunks: make(map[string]*chunk.Chunk),
	}

	// Create a large dataset: 10000 local chunks, 1000 orphans.
	const totalChunks = 10000
	const orphanChunks = 1000

	for i := 0; i < totalChunks; i++ {
		id := chunk.ChunkID(strconv.Itoa(i))
		store.chunks[string(id)] = &chunk.Chunk{
			ID:       id,
			Data:     make([]byte, 4*1024*1024), // 4MB chunks
			Checksum: 0,
		}
	}

	metaClient := &mockMetadataClient{
		referencedChunks: make(map[string]struct{}),
	}
	// Mark all but orphan chunks as referenced.
	for i := 0; i < totalChunks-orphanChunks; i++ {
		metaClient.referencedChunks[strconv.Itoa(i)] = struct{}{}
	}

	gc := &GarbageCollector{
		store:      store,
		metaClient: metaClient,
		interval:   1 * time.Hour,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := gc.RunOnce(ctx)
		if err != nil {
			b.Fatalf("GC failed: %v", err)
		}
	}
}
