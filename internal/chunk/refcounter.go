package chunk

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/piwi3910/novastor/internal/metrics"
)

const uint32Max = uint32(1<<32 - 1)

// ChunkRefCounter tracks reference counts for chunks.
// A reference represents a volume or other entity using the chunk.
// Chunks are only deleted when their reference count reaches zero.
type ChunkRefCounter struct {
	dir    string
	mu     sync.RWMutex
	refMap map[ChunkID]uint32
}

// NewChunkRefCounter creates a new reference counter backed by a directory.
// The reference counts are persisted to disk for crash recovery.
func NewChunkRefCounter(dir string) (*ChunkRefCounter, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating refcounter directory: %w", err)
	}
	rc := &ChunkRefCounter{
		dir:    dir,
		refMap: make(map[ChunkID]uint32),
	}
	if err := rc.load(); err != nil {
		return nil, fmt.Errorf("loading reference counts: %w", err)
	}
	return rc, nil
}

// refPath returns the file path for a chunk's reference count.
func (rc *ChunkRefCounter) refPath(id ChunkID) string {
	idStr := string(id)
	if len(idStr) < 2 {
		return filepath.Join(rc.dir, idStr+".ref")
	}
	return filepath.Join(rc.dir, idStr[:2], idStr+".ref")
}

// load reads all reference counts from disk into memory.
func (rc *ChunkRefCounter) load() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	entries, err := os.ReadDir(rc.dir)
	if err != nil {
		return fmt.Errorf("reading refcounter directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subDir := filepath.Join(rc.dir, entry.Name())
		files, err := os.ReadDir(subDir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if filepath.Ext(f.Name()) != ".ref" {
				continue
			}
			path := filepath.Join(subDir, f.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			if len(data) < 4 {
				continue
			}
			count := binary.BigEndian.Uint32(data[:4])
			chunkID := ChunkID(f.Name()[:len(f.Name())-4])
			rc.refMap[chunkID] = count
		}
	}
	return nil
}

// Get returns the current reference count for a chunk.
func (rc *ChunkRefCounter) Get(id ChunkID) uint32 {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.refMap[id]
}

// Increment increases the reference count for a chunk by one.
// If the chunk is not tracked, it starts at 1.
func (rc *ChunkRefCounter) Increment(id ChunkID) error {
	return rc.Add(id, 1)
}

// Add adds delta to the reference count for a chunk.
func (rc *ChunkRefCounter) Add(id ChunkID, delta int32) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	current := rc.refMap[id]
	newCount := int64(current) + int64(delta) //nolint:gosec // Use int64 to avoid overflow
	if newCount < 0 {
		return fmt.Errorf("reference count cannot be negative for chunk %s", id)
	}
	if newCount > int64(uint32Max) {
		return fmt.Errorf("reference count overflow for chunk %s", id)
	}

	rc.refMap[id] = uint32(newCount)

	// Persist to disk
	if err := rc.persist(id, uint32(newCount)); err != nil {
		delete(rc.refMap, id)
		return fmt.Errorf("persisting reference count for %s: %w", id, err)
	}

	// Update metrics
	updateRefCountMetrics(int64(delta))

	return nil
}

// Decrement decreases the reference count for a chunk by one.
// It returns the new reference count. If the count reaches zero,
// the caller should delete the chunk data.
func (rc *ChunkRefCounter) Decrement(id ChunkID) (uint32, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	current, exists := rc.refMap[id]
	if !exists {
		return 0, fmt.Errorf("chunk %s has no references to decrement", id)
	}

	if current == 0 {
		return 0, fmt.Errorf("chunk %s already has zero references", id)
	}

	newCount := current - 1
	if newCount == 0 {
		delete(rc.refMap, id)
		if err := rc.remove(id); err != nil {
			return 0, fmt.Errorf("removing reference file for %s: %w", id, err)
		}
	} else {
		rc.refMap[id] = newCount
		if err := rc.persist(id, newCount); err != nil {
			rc.refMap[id] = current
			return 0, fmt.Errorf("persisting reference count for %s: %w", id, err)
		}
	}

	// Update metrics
	updateRefCountMetrics(-1)

	return newCount, nil
}

// persist writes the reference count to disk.
func (rc *ChunkRefCounter) persist(id ChunkID, count uint32) error {
	path := rc.refPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, count)
	return os.WriteFile(path, data, 0o600)
}

// remove deletes the reference count file from disk.
func (rc *ChunkRefCounter) remove(id ChunkID) error {
	path := rc.refPath(id)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Try to remove empty parent directory
	parentDir := filepath.Dir(path)
	if entries, _ := os.ReadDir(parentDir); len(entries) == 0 {
		_ = os.Remove(parentDir)
	}
	return nil
}

// ForEach calls fn for each chunk with a non-zero reference count.
func (rc *ChunkRefCounter) ForEach(fn func(id ChunkID, count uint32) error) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	for id, count := range rc.refMap {
		if err := fn(id, count); err != nil {
			return err
		}
	}
	return nil
}

// Count returns the total number of chunks with non-zero reference counts.
func (rc *ChunkRefCounter) Count() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return len(rc.refMap)
}

// TotalReferences returns the sum of all reference counts.
func (rc *ChunkRefCounter) TotalReferences() uint64 {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	var total uint64
	for _, count := range rc.refMap {
		total += uint64(count)
	}
	return total
}

// Snapshot returns a JSON snapshot of the reference map.
func (rc *ChunkRefCounter) Snapshot() ([]byte, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return json.Marshal(rc.refMap)
}

// Restore restores the reference map from a JSON snapshot.
func (rc *ChunkRefCounter) Restore(data []byte) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	var m map[ChunkID]uint32
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("unmarshaling snapshot: %w", err)
	}

	rc.refMap = m
	return nil
}

func updateRefCountMetrics(delta int64) {
	metrics.DedupReferences.Add(float64(delta))
	if delta > 0 {
		metrics.DedupRefOpsTotal.WithLabelValues("increment").Inc()
	} else {
		metrics.DedupRefOpsTotal.WithLabelValues("decrement").Inc()
	}
}
