package chunk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/piwi3910/novastor/internal/metrics"
)

type LocalStore struct {
	dir string
}

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

func (s *LocalStore) Delete(_ context.Context, id ChunkID) error {
	path := s.chunkPath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting chunk %s: %w", id, err)
	}
	metrics.ChunkOpsTotal.WithLabelValues("delete").Inc()
	return nil
}

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
