// Package chunk provides chunk-level storage with deduplication support.
//
// The deduplication system uses content-addressed chunks (SHA-256) to
// automatically deduplicate data across multiple volumes. Chunks with
// identical content are stored only once, with reference counting tracking
// how many entities reference each chunk.
package chunk

import (
	"context"
	"fmt"
	"sync"

	"github.com/piwi3910/novastor/internal/metrics"
)

// DedupStore wraps a Store to provide chunk-level deduplication.
// Chunks are content-addressed (SHA-256), so identical data produces
// the same chunk ID. DedupStore tracks reference counts and only
// deletes chunks when all references are released.
type DedupStore struct {
	backend      Store
	refCounter   *ChunkRefCounter
	mu           sync.RWMutex
	logicalSize  int64
	physicalSize int64
}

// NewDedupStore creates a new deduplicating store wrapper.
// The backend store handles actual chunk data persistence.
// The refCounter tracks how many entities reference each chunk.
func NewDedupStore(backend Store, refCounter *ChunkRefCounter) *DedupStore {
	return &DedupStore{
		backend:     backend,
		refCounter:  refCounter,
		logicalSize: 0,
	}
}

// Put stores a chunk if it doesn't already exist, and increments
// its reference count. If the chunk already exists, only the reference
// count is incremented (deduplication).
func (ds *DedupStore) Put(ctx context.Context, c *Chunk) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	exists, err := ds.backend.Has(ctx, c.ID)
	if err != nil {
		return fmt.Errorf("checking chunk existence: %w", err)
	}

	chunkSize := int64(len(c.Data))

	if exists {
		// Chunk already exists - deduplication!
		metrics.DedupChunksSaved.Inc()
		metrics.DedupBytesSaved.Add(float64(chunkSize))

		// Just increment reference count, don't write data
		if err := ds.refCounter.Increment(c.ID); err != nil {
			return fmt.Errorf("incrementing reference for existing chunk %s: %w", c.ID, err)
		}
	} else {
		// New chunk - write to backend and set initial reference
		if err := ds.backend.Put(ctx, c); err != nil {
			return fmt.Errorf("writing chunk %s to backend: %w", c.ID, err)
		}
		if err := ds.refCounter.Increment(c.ID); err != nil {
			// Rollback: try to delete the chunk we just wrote
			_ = ds.backend.Delete(ctx, c.ID)
			return fmt.Errorf("incrementing reference for new chunk %s: %w", c.ID, err)
		}
		// Update physical bytes for new chunks only
		ds.physicalSize += chunkSize
		metrics.DedupPhysicalBytes.Add(float64(chunkSize))
	}

	// Always track logical bytes
	ds.logicalSize += chunkSize
	metrics.DedupLogicalBytes.Set(float64(ds.logicalSize))

	// Update dedup ratio
	ds.updateRatio()

	return nil
}

// Get retrieves a chunk by ID. It doesn't change reference counts.
func (ds *DedupStore) Get(ctx context.Context, id ChunkID) (*Chunk, error) {
	return ds.backend.Get(ctx, id)
}

// Delete decrements the reference count for a chunk.
// The chunk is only actually deleted when the reference count reaches zero.
func (ds *DedupStore) Delete(ctx context.Context, id ChunkID) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Get current reference count
	refCount := ds.refCounter.Get(id)
	if refCount == 0 {
		// No references - may have already been deleted
		return fmt.Errorf("chunk %s has no references", id)
	}

	// Decrement reference count
	newCount, err := ds.refCounter.Decrement(id)
	if err != nil {
		return fmt.Errorf("decrementing reference for chunk %s: %w", id, err)
	}

	// If reference count reached zero, delete from backend
	if newCount == 0 {
		// Get chunk size before deletion for metrics
		chunk, err := ds.backend.Get(ctx, id)
		chunkSize := int64(0)
		if err == nil && chunk != nil {
			chunkSize = int64(len(chunk.Data))
		}

		if err := ds.backend.Delete(ctx, id); err != nil {
			// Rollback: restore reference count
			_ = ds.refCounter.Increment(id)
			return fmt.Errorf("deleting chunk %s from backend: %w", id, err)
		}

		// Update physical bytes
		ds.physicalSize -= chunkSize
		metrics.DedupPhysicalBytes.Sub(float64(chunkSize))
	}

	// Reduce logical bytes - estimate using chunk size if available
	// Since we don't have the data here, we'll need to track it differently
	// For now, we'll recalculate from reference counter on demand
	ds.recalculateLogicalBytes()
	ds.updateRatio()

	return nil
}

// Has checks if a chunk exists in the backend.
func (ds *DedupStore) Has(ctx context.Context, id ChunkID) (bool, error) {
	return ds.backend.Has(ctx, id)
}

// List returns all chunk IDs stored in the backend.
func (ds *DedupStore) List(ctx context.Context) ([]ChunkID, error) {
	return ds.backend.List(ctx)
}

// GetRefCount returns the current reference count for a chunk.
func (ds *DedupStore) GetRefCount(id ChunkID) uint32 {
	return ds.refCounter.Get(id)
}

// Stats returns statistics about deduplication effectiveness.
func (ds *DedupStore) Stats(ctx context.Context) (*DedupStats, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	physicalChunks := 0
	var physicalBytes int64

	list, err := ds.backend.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing chunks: %w", err)
	}

	for _, id := range list {
		chunk, err := ds.backend.Get(ctx, id)
		if err != nil {
			continue
		}
		physicalChunks++
		physicalBytes += int64(len(chunk.Data))
	}

	totalRefs := ds.refCounter.TotalReferences()
	logicalBytes := ds.logicalSize
	if logicalBytes == 0 {
		// Fallback: recalculate from reference counter
		_ = ds.refCounter.ForEach(func(id ChunkID, count uint32) error {
			chunk, gerr := ds.backend.Get(ctx, id)
			// nolint:nilerr // Skip chunks that can't be read - continue iteration
			if gerr != nil {
				// Skip chunks that can't be read - continue iteration
				return nil
			}
			logicalBytes += int64(len(chunk.Data)) * int64(count)
			return nil
		})
	}

	ratio := float64(0)
	if physicalBytes > 0 {
		ratio = float64(logicalBytes) / float64(physicalBytes)
	}

	return &DedupStats{
		LogicalBytes:    logicalBytes,
		PhysicalBytes:   physicalBytes,
		PhysicalChunks:  physicalChunks,
		TotalReferences: totalRefs,
		DedupRatio:      ratio,
		SavedBytes:      logicalBytes - physicalBytes,
		UniqueChunks:    uint32(uint64(physicalChunks)), //nolint:gosec // physicalChunks is always small
	}, nil
}

// recalculateLogicalBytes rebuilds the logical size counter from the
// reference counter and backend chunks. Use this if counters get out of sync.
func (ds *DedupStore) recalculateLogicalBytes() {
	var total int64
	_ = ds.refCounter.ForEach(func(id ChunkID, count uint32) error {
		// We can't easily get chunk size without context here
		// This is a limitation - in production, we'd cache sizes
		total += int64(count) * ChunkSize // Approximate
		return nil
	})
	ds.logicalSize = total
	metrics.DedupLogicalBytes.Set(float64(ds.logicalSize))
	ds.updatePhysicalBytesMetric()
}

// updatePhysicalBytesMetric syncs the physical bytes metric with our internal counter.
func (ds *DedupStore) updatePhysicalBytesMetric() {
	metrics.DedupPhysicalBytes.Set(float64(ds.physicalSize))
}

// updateRatio recalculates and updates the deduplication ratio metric.
// Must be called while holding ds.mu.
func (ds *DedupStore) updateRatio() {
	// Use the internal counters directly to avoid deadlock
	// since we're already holding the lock
	if ds.physicalSize > 0 {
		metrics.DedupRatio.Set(float64(ds.logicalSize) / float64(ds.physicalSize))
	}
}

// DedupStats contains deduplication statistics.
type DedupStats struct {
	LogicalBytes    int64
	PhysicalBytes   int64
	PhysicalChunks  int
	TotalReferences uint64
	DedupRatio      float64
	SavedBytes      int64
	UniqueChunks    uint32
}

// Compact ensures the reference counter and backend are in sync.
// It removes reference entries for chunks that no longer exist.
func (ds *DedupStore) Compact(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Get all chunks in backend
	backendChunks, err := ds.backend.List(ctx)
	if err != nil {
		return fmt.Errorf("listing backend chunks: %w", err)
	}

	// Build a set of existing chunk IDs
	existing := make(map[ChunkID]bool)
	for _, id := range backendChunks {
		existing[id] = true
	}

	// Remove references for non-existent chunks
	var toRemove []ChunkID
	_ = ds.refCounter.ForEach(func(id ChunkID, count uint32) error {
		if !existing[id] {
			toRemove = append(toRemove, id)
		}
		return nil
	})

	for _, id := range toRemove {
		// Remove the reference entry (orphaned)
		_, _ = ds.refCounter.Decrement(id)
	}

	return nil
}

// RebuildFromBackend reconstructs reference counts by scanning the backend
// and a provided list of chunk IDs that should have references.
// This is a recovery operation for crash consistency.
func (ds *DedupStore) RebuildFromBackend(ctx context.Context, referencedChunks []ChunkID) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Clear existing reference counts - collect IDs first to avoid deadlock
	var toClear []ChunkID
	_ = ds.refCounter.ForEach(func(id ChunkID, count uint32) error {
		toClear = append(toClear, id)
		return nil
	})

	// Now decrement them outside the ForEach lock
	for _, id := range toClear {
		for {
			_, err := ds.refCounter.Decrement(id)
			if err != nil {
				break // Count reached zero
			}
		}
	}

	// Count references
	refCounts := make(map[ChunkID]uint32)
	for _, id := range referencedChunks {
		refCounts[id]++
	}

	// Set reference counts for chunks that exist in backend
	backendChunks, err := ds.backend.List(ctx)
	if err != nil {
		return fmt.Errorf("listing backend chunks: %w", err)
	}

	existing := make(map[ChunkID]bool)
	for _, id := range backendChunks {
		existing[id] = true
	}

	var logicalSize int64
	var physicalSize int64
	for id, count := range refCounts {
		if existing[id] {
			// Set the reference count directly
			for i := uint32(0); i < count; i++ {
				_ = ds.refCounter.Increment(id)
			}
			// Track logical bytes (approximate)
			chunk, err := ds.backend.Get(ctx, id)
			if err == nil {
				logicalSize += int64(len(chunk.Data)) * int64(count)
				physicalSize += int64(len(chunk.Data))
			}
		}
	}

	ds.logicalSize = logicalSize
	ds.physicalSize = physicalSize
	metrics.DedupLogicalBytes.Set(float64(ds.logicalSize))
	metrics.DedupPhysicalBytes.Set(float64(ds.physicalSize))
	ds.updateRatio()

	return nil
}

// Verify checks that all chunks with non-zero reference counts exist in the backend.
// It returns a list of missing chunk IDs.
func (ds *DedupStore) Verify(ctx context.Context) ([]ChunkID, error) {
	var missing []ChunkID

	err := ds.refCounter.ForEach(func(id ChunkID, count uint32) error {
		exists, err := ds.backend.Has(ctx, id)
		if err != nil {
			return fmt.Errorf("checking chunk %s: %w", id, err)
		}
		if !exists {
			missing = append(missing, id)
		}
		return nil
	})

	return missing, err
}
