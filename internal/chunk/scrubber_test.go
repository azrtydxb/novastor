package chunk

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- In-memory store for scrubber tests ---

type memStore struct {
	mu     sync.Mutex
	chunks map[ChunkID]*Chunk
}

func newMemStore() *memStore {
	return &memStore{chunks: make(map[ChunkID]*Chunk)}
}

func (m *memStore) Put(_ context.Context, c *Chunk) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Store a copy to allow independent tampering.
	cp := &Chunk{
		ID:       c.ID,
		Data:     make([]byte, len(c.Data)),
		Checksum: c.Checksum,
	}
	copy(cp.Data, c.Data)
	m.chunks[c.ID] = cp
	return nil
}

func (m *memStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.chunks[id]
	if !ok {
		return nil, &chunkNotFoundError{id: id}
	}
	// Return a copy so callers can't mutate internal state.
	cp := &Chunk{
		ID:       c.ID,
		Data:     make([]byte, len(c.Data)),
		Checksum: c.Checksum,
	}
	copy(cp.Data, c.Data)
	return cp, nil
}

func (m *memStore) Delete(_ context.Context, id ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.chunks, id)
	return nil
}

func (m *memStore) Has(_ context.Context, id ChunkID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.chunks[id]
	return ok, nil
}

func (m *memStore) List(_ context.Context) ([]ChunkID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]ChunkID, 0, len(m.chunks))
	for id := range m.chunks {
		ids = append(ids, id)
	}
	return ids, nil
}

// corrupt tampers with a chunk's data in-place so VerifyChecksum will fail.
func (m *memStore) corrupt(id ChunkID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.chunks[id]
	if ok && len(c.Data) > 0 {
		c.Data[0] ^= 0xFF
	}
}

type chunkNotFoundError struct {
	id ChunkID
}

func (e *chunkNotFoundError) Error() string {
	return "chunk not found: " + string(e.id)
}

// --- Mock reporter ---

type mockReporter struct {
	mu      sync.Mutex
	reports []ChunkID
}

func (r *mockReporter) ReportCorruptChunk(_ context.Context, chunkID ChunkID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, chunkID)
	return nil
}

func (r *mockReporter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reports)
}

// --- Helper ---

func makeTestChunk(data []byte) *Chunk {
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: make([]byte, len(data)),
	}
	copy(c.Data, data)
	c.Checksum = c.ComputeChecksum()
	return c
}

// --- Tests ---

func TestScrubOnce_AllHealthy(t *testing.T) {
	store := newMemStore()
	reporter := &mockReporter{}
	ctx := context.Background()

	c1 := makeTestChunk([]byte("chunk-one"))
	c2 := makeTestChunk([]byte("chunk-two"))
	c3 := makeTestChunk([]byte("chunk-three"))
	for _, c := range []*Chunk{c1, c2, c3} {
		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	scrubber := NewScrubber(store, reporter, time.Hour)
	ok, bad, err := scrubber.ScrubOnce(ctx)
	if err != nil {
		t.Fatalf("ScrubOnce failed: %v", err)
	}
	if ok != 3 {
		t.Errorf("expected 3 ok, got %d", ok)
	}
	if bad != 0 {
		t.Errorf("expected 0 bad, got %d", bad)
	}
	if reporter.count() != 0 {
		t.Error("reporter should not have been called")
	}
}

func TestScrubOnce_CorruptChunk(t *testing.T) {
	store := newMemStore()
	reporter := &mockReporter{}
	ctx := context.Background()

	c1 := makeTestChunk([]byte("chunk-one"))
	c2 := makeTestChunk([]byte("chunk-two"))
	c3 := makeTestChunk([]byte("chunk-three"))
	for _, c := range []*Chunk{c1, c2, c3} {
		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Corrupt one chunk's data directly in the store.
	store.corrupt(c2.ID)

	scrubber := NewScrubber(store, reporter, time.Hour)
	ok, bad, err := scrubber.ScrubOnce(ctx)
	if err != nil {
		t.Fatalf("ScrubOnce failed: %v", err)
	}
	if ok != 2 {
		t.Errorf("expected 2 ok, got %d", ok)
	}
	if bad != 1 {
		t.Errorf("expected 1 bad, got %d", bad)
	}
	if reporter.count() != 1 {
		t.Errorf("expected 1 report, got %d", reporter.count())
	}
}

func TestScrubberStartStop(t *testing.T) {
	store := newMemStore()
	reporter := &mockReporter{}
	ctx := context.Background()

	c := makeTestChunk([]byte("test-data"))
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	scrubber := NewScrubber(store, reporter, 50*time.Millisecond)
	scrubber.Start(ctx)

	// Wait enough time for at least one scrub to complete.
	time.Sleep(200 * time.Millisecond)

	scrubber.Stop()

	_, chunksOK, _ := scrubber.Stats()
	if chunksOK < 1 {
		t.Errorf("expected at least 1 chunksOK after scrub, got %d", chunksOK)
	}
}

func TestScrubberStats(t *testing.T) {
	store := newMemStore()
	reporter := &mockReporter{}
	ctx := context.Background()

	c1 := makeTestChunk([]byte("alpha"))
	c2 := makeTestChunk([]byte("beta"))
	for _, c := range []*Chunk{c1, c2} {
		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Corrupt one.
	store.corrupt(c1.ID)

	scrubber := NewScrubber(store, reporter, time.Hour)

	// Before any scrub, stats should be zero.
	lastScrub, chunksOK, chunksBad := scrubber.Stats()
	if !lastScrub.IsZero() {
		t.Error("expected zero lastScrub before any scrub")
	}

	_, _, err := scrubber.ScrubOnce(ctx)
	if err != nil {
		t.Fatalf("ScrubOnce failed: %v", err)
	}

	lastScrub, chunksOK, chunksBad = scrubber.Stats()
	if lastScrub.IsZero() {
		t.Error("expected non-zero lastScrub after scrub")
	}
	if chunksOK != 1 {
		t.Errorf("expected 1 ok, got %d", chunksOK)
	}
	if chunksBad != 1 {
		t.Errorf("expected 1 bad, got %d", chunksBad)
	}
}
