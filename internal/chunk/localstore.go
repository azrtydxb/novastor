package chunk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/piwi3910/novastor/internal/metrics"
)

// LocalStore implements the Store interface using the local filesystem.
// Chunks are stored in a two-level directory hierarchy for efficient access.
type LocalStore struct {
	dir string
}

// NewLocalStore creates a new local filesystem-backed chunk store.
// The directory is created if it does not exist.
func NewLocalStore(dir string) (*LocalStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating chunk store directory: %w", err)
	}
	return &LocalStore{dir: dir}, nil
}

func (s *LocalStore) chunkPath(id ChunkID) string {
	idStr := string(id)
	if len(idStr) < 2 {
		return filepath.Join(s.dir, idStr)
	}
	return filepath.Join(s.dir, idStr[:2], idStr)
}

// Put stores a chunk on the local filesystem.
// The chunk data is prefixed with a 4-byte checksum for integrity verification.
func (s *LocalStore) Put(_ context.Context, c *Chunk) error {
	path := s.chunkPath(c.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating chunk subdirectory: %w", err)
	}
	buf := make([]byte, 4+len(c.Data))
	binary.BigEndian.PutUint32(buf[:4], c.Checksum)
	copy(buf[4:], c.Data)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return fmt.Errorf("writing chunk %s: %w", c.ID, err)
	}
	metrics.ChunkOpsTotal.WithLabelValues("write").Inc()
	metrics.ChunkBytesTotal.WithLabelValues("write").Add(float64(len(c.Data)))
	return nil
}

// Get retrieves a chunk from the local filesystem.
// Returns an error if the chunk is not found or fails checksum verification.
func (s *LocalStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	path := s.chunkPath(id)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk %s not found", id)
		}
		return nil, fmt.Errorf("reading chunk %s: %w", id, err)
	}
	if len(raw) < 4 {
		return nil, fmt.Errorf("chunk %s: file too small", id)
	}
	checksum := binary.BigEndian.Uint32(raw[:4])
	data := raw[4:]
	c := &Chunk{ID: id, Data: data, Checksum: checksum}
	if err := c.VerifyChecksum(); err != nil {
		return nil, fmt.Errorf("chunk %s integrity check failed: %w", id, err)
	}
	metrics.ChunkOpsTotal.WithLabelValues("read").Inc()
	metrics.ChunkBytesTotal.WithLabelValues("read").Add(float64(len(data)))
	return c, nil
}

// Delete removes a chunk from the local filesystem.
// No error is returned if the chunk does not exist.
func (s *LocalStore) Delete(_ context.Context, id ChunkID) error {
	path := s.chunkPath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting chunk %s: %w", id, err)
	}
	metrics.ChunkOpsTotal.WithLabelValues("delete").Inc()
	return nil
}

// Has checks whether a chunk exists in the store.
func (s *LocalStore) Has(_ context.Context, id ChunkID) (bool, error) {
	path := s.chunkPath(id)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking chunk %s: %w", id, err)
	}
	return true, nil
}

// List returns all chunk IDs currently stored in the local store.
func (s *LocalStore) List(_ context.Context) ([]ChunkID, error) {
	var ids []ChunkID
	err := filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) == 2 {
			ids = append(ids, ChunkID(parts[1]))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing chunks: %w", err)
	}
	return ids, nil
}

// diskUsedBytes walks the store directory and returns the total size of all
// regular files found. It is used to compute the actual disk usage for the Stats method.
func (s *LocalStore) diskUsedBytes() uint64 {
	var total uint64
	_ = filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		// nolint:nilerr // Skip files we can't stat - return nil to continue walking
		if err != nil {
			// Skip files we can't stat
			return nil
		}
		if info.IsDir() {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	return total
}

// Stats returns storage capacity and usage statistics for the LocalStore.
// It uses syscall.Statfs to query filesystem-level capacity.
func (s *LocalStore) Stats(_ context.Context) (*StoreStats, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &stat); err != nil {
		return nil, fmt.Errorf("statfs failed: %w", err)
	}

	total := int64(stat.Bsize) * int64(stat.Blocks)
	avail := int64(stat.Bsize) * int64(stat.Bfree)
	used := int64(s.diskUsedBytes())

	ids, err := s.List(context.Background())
	if err != nil {
		return nil, fmt.Errorf("listing chunks for stats: %w", err)
	}

	return &StoreStats{
		TotalBytes:     total,
		UsedBytes:      used,
		AvailableBytes: avail,
		ChunkCount:     int64(len(ids)),
	}, nil
}

// GetMeta returns chunk metadata without loading the full data payload.
// This is more efficient for operations like scrubbing that only need checksums.
func (s *LocalStore) GetMeta(_ context.Context, id ChunkID) (*ChunkMeta, error) {
	path := s.chunkPath(id)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk %s not found", id)
		}
		return nil, fmt.Errorf("getting chunk metadata %s: %w", id, err)
	}

	// Read only the first 4 bytes to get the checksum.
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening chunk for metadata %s: %w", id, err)
	}
	defer func() { _ = file.Close() }()

	var checksumBuf [4]byte
	if _, err := file.Read(checksumBuf[:]); err != nil {
		return nil, fmt.Errorf("reading checksum from chunk %s: %w", id, err)
	}
	checksum := binary.BigEndian.Uint32(checksumBuf[:])

	// The file size includes the 4-byte checksum header.
	// The actual data size is file size - 4.
	dataSize := info.Size() - 4
	if dataSize < 0 {
		dataSize = 0
	}

	return &ChunkMeta{
		ID:       id,
		Size:     dataSize,
		Checksum: checksum,
	}, nil
}

// HealthCheck verifies that the storage directory is accessible and writable.
// It performs a minimal filesystem operation to confirm health.
func (s *LocalStore) HealthCheck(_ context.Context) error {
	// Check if directory exists and is readable.
	info, err := os.Stat(s.dir)
	if err != nil {
		return fmt.Errorf("store directory inaccessible: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("store path is not a directory")
	}

	// Try to create a temporary file to verify write permissions.
	// Use a unique name to avoid collisions.
	probePath := filepath.Join(s.dir, ".healthcheck_probe")
	file, err := os.Create(probePath)
	if err != nil {
		return fmt.Errorf("write permission check failed: %w", err)
	}
	_ = file.Close()

	// Clean up the probe file.
	if removeErr := os.Remove(probePath); removeErr != nil {
		// Log but don't fail the health check if cleanup fails.
		fmt.Printf("warning: failed to remove healthcheck probe file: %v\n", removeErr)
	}

	return nil
}
