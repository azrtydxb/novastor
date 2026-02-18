package chunk

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore implements the Store interface in memory.
// It is primarily useful for testing and development.
type MemoryStore struct {
	mu     sync.RWMutex
	chunks map[ChunkID]*Chunk
}

// NewMemoryStore creates a new in-memory chunk store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		chunks: make(map[ChunkID]*Chunk),
	}
}

// Put stores a chunk in memory.
func (s *MemoryStore) Put(_ context.Context, c *Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store a copy to prevent external mutation.
	chunkCopy := &Chunk{
		ID:       c.ID,
		Data:     make([]byte, len(c.Data)),
		Checksum: c.Checksum,
	}
	copy(chunkCopy.Data, c.Data)

	s.chunks[c.ID] = chunkCopy
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
	return nil
}
