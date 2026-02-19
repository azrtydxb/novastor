package chunk

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore implements the Store interface in memory.
// It is primarily useful for testing and development.
type MemoryStore struct {
	mu         sync.RWMutex
	chunks     map[ChunkID]*Chunk
	totalBytes int64
	maxBytes   int64
}

// NewMemoryStore creates a new in-memory chunk store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		chunks:   make(map[ChunkID]*Chunk),
		maxBytes: 1 << 40, // Default 1TB limit (effectively unlimited for tests)
	}
}

// Put stores a chunk in memory.
func (s *MemoryStore) Put(_ context.Context, c *Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if replacing an existing chunk.
	if existing, exists := s.chunks[c.ID]; exists {
		s.totalBytes -= int64(len(existing.Data))
	}

	// Store a copy to prevent external mutation.
	chunkCopy := &Chunk{
		ID:       c.ID,
		Data:     make([]byte, len(c.Data)),
		Checksum: c.Checksum,
	}
	copy(chunkCopy.Data, c.Data)

	s.chunks[c.ID] = chunkCopy
	s.totalBytes += int64(len(c.Data))
	return nil
}

// Get retrieves a chunk from memory.
func (s *MemoryStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, exists := s.chunks[id]
	if !exists {
		return nil, fmt.Errorf("chunk %s not found", id)
	}

	// Return a copy to prevent external mutation.
	chunkCopy := &Chunk{
		ID:       c.ID,
		Data:     make([]byte, len(c.Data)),
		Checksum: c.Checksum,
	}
	copy(chunkCopy.Data, c.Data)

	return chunkCopy, nil
}

// Delete removes a chunk from memory.
func (s *MemoryStore) Delete(_ context.Context, id ChunkID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.chunks[id]; exists {
		s.totalBytes -= int64(len(existing.Data))
	}
	delete(s.chunks, id)
	return nil
}

// Has checks if a chunk exists in memory.
func (s *MemoryStore) Has(_ context.Context, id ChunkID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.chunks[id]
	return exists, nil
}

// List returns all chunk IDs in memory.
func (s *MemoryStore) List(_ context.Context) ([]ChunkID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]ChunkID, 0, len(s.chunks))
	for id := range s.chunks {
		ids = append(ids, id)
	}
	return ids, nil
}

// Close clears all stored chunks.
// MemoryStore does not hold external resources, so this is a no-op.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = make(map[ChunkID]*Chunk)
	s.totalBytes = 0
	return nil
}

// Stats returns storage capacity and usage statistics for the MemoryStore.
// Since memory is dynamically allocated, total is set to a configured maximum.
func (s *MemoryStore) Stats(_ context.Context) (*StoreStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &StoreStats{
		TotalBytes:     s.maxBytes,
		UsedBytes:      s.totalBytes,
		AvailableBytes: s.maxBytes - s.totalBytes,
		ChunkCount:     int64(len(s.chunks)),
	}, nil
}

// GetMeta returns chunk metadata without loading the full data payload.
func (s *MemoryStore) GetMeta(_ context.Context, id ChunkID) (*ChunkMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, exists := s.chunks[id]
	if !exists {
		return nil, fmt.Errorf("chunk %s not found", id)
	}

	return &ChunkMeta{
		ID:       c.ID,
		Size:     int64(len(c.Data)),
		Checksum: c.Checksum,
	}, nil
}

// HealthCheck verifies that the memory store is operational.
// For an in-memory store, this is essentially a no-op unless corrupted.
func (s *MemoryStore) HealthCheck(_ context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Basic check: ensure the map is initialized.
	if s.chunks == nil {
		return fmt.Errorf("memory store not initialized")
	}
	return nil
}
