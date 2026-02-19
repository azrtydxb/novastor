package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	bucketInodes  = "inodes"
	bucketDirents = "dirents"
)

// InodeType represents the type of a filesystem inode.
type InodeType string

const (
	// InodeTypeFile represents a regular file.
	InodeTypeFile InodeType = "file"
	// InodeTypeDir represents a directory.
	InodeTypeDir InodeType = "dir"
	// InodeTypeSymlink represents a symbolic link.
	InodeTypeSymlink InodeType = "symlink"
)

// InodeMeta holds POSIX-like metadata for a single inode.
type InodeMeta struct {
	Ino       uint64            `json:"ino"`
	Type      InodeType         `json:"type"`
	Size      int64             `json:"size"`
	Mode      uint32            `json:"mode"`
	UID       uint32            `json:"uid"`
	GID       uint32            `json:"gid"`
	LinkCount uint32            `json:"linkCount"`
	ChunkIDs  []string          `json:"chunkIDs,omitempty"`
	Target    string            `json:"target,omitempty"` // symlink target
	Xattrs    map[string]string `json:"xattrs,omitempty"`
	ATime     int64             `json:"atime"` // Unix nanoseconds
	MTime     int64             `json:"mtime"`
	CTime     int64             `json:"ctime"`
}

// DirEntry represents a single directory entry mapping a name to an inode.
type DirEntry struct {
	Name string    `json:"name"`
	Ino  uint64    `json:"ino"`
	Type InodeType `json:"type"`
}

// inodeKey returns the FSM key for an inode.
func inodeKey(ino uint64) string {
	return fmt.Sprintf("%d", ino)
}

// direntKey returns the FSM key for a directory entry.
func direntKey(parentIno uint64, name string) string {
	return fmt.Sprintf("%d/%s", parentIno, name)
}

// direntPrefix returns the prefix used to list all entries in a directory.
func direntPrefix(parentIno uint64) string {
	return fmt.Sprintf("%d/", parentIno)
}

// CreateInode stores inode metadata via Raft consensus.
func (s *RaftStore) CreateInode(_ context.Context, meta *InodeMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling inode meta: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketInodes, Key: inodeKey(meta.Ino), Value: data})
}

// GetInode retrieves inode metadata by inode number.
func (s *RaftStore) GetInode(_ context.Context, ino uint64) (*InodeMeta, error) {
	data, err := s.fsm.Get(bucketInodes, inodeKey(ino))
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling inode meta: %w", err)
	}
	return &meta, nil
}

// UpdateInode updates inode metadata via Raft consensus.
func (s *RaftStore) UpdateInode(_ context.Context, meta *InodeMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling inode meta: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketInodes, Key: inodeKey(meta.Ino), Value: data})
}

// DeleteInode removes inode metadata via Raft consensus.
func (s *RaftStore) DeleteInode(_ context.Context, ino uint64) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketInodes, Key: inodeKey(ino)})
}

// CreateDirEntry stores a directory entry via Raft consensus.
func (s *RaftStore) CreateDirEntry(_ context.Context, parentIno uint64, entry *DirEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling dir entry: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketDirents, Key: direntKey(parentIno, entry.Name), Value: data})
}

// DeleteDirEntry removes a directory entry via Raft consensus.
func (s *RaftStore) DeleteDirEntry(_ context.Context, parentIno uint64, name string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketDirents, Key: direntKey(parentIno, name)})
}

// LookupDirEntry retrieves a directory entry by parent inode and name.
func (s *RaftStore) LookupDirEntry(_ context.Context, parentIno uint64, name string) (*DirEntry, error) {
	data, err := s.fsm.Get(bucketDirents, direntKey(parentIno, name))
	if err != nil {
		return nil, err
	}
	var entry DirEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("unmarshaling dir entry: %w", err)
	}
	return &entry, nil
}

// ListDirectory returns all directory entries for the given parent inode.
func (s *RaftStore) ListDirectory(_ context.Context, parentIno uint64) ([]*DirEntry, error) {
	all, err := s.fsm.GetAll(bucketDirents)
	if err != nil {
		return nil, err
	}
	prefix := direntPrefix(parentIno)
	var result []*DirEntry
	for k, v := range all {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var entry DirEntry
		if err := json.Unmarshal(v, &entry); err != nil {
			return nil, fmt.Errorf("unmarshaling dir entry for key %s: %w", k, err)
		}
		result = append(result, &entry)
	}
	return result, nil
}
