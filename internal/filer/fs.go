package filer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// InodeType represents the type of a filesystem inode.
type InodeType string

const (
	TypeFile    InodeType = "file"
	TypeDir     InodeType = "dir"
	TypeSymlink InodeType = "symlink"
)

// InodeMeta holds POSIX-like metadata for a single inode.
type InodeMeta struct {
	Ino       uint64
	Type      InodeType
	Size      int64
	Mode      uint32
	UID       uint32
	GID       uint32
	LinkCount uint32
	ChunkIDs  []string
	Target    string
	Xattrs    map[string]string
	ATime     int64
	MTime     int64
	CTime     int64
}

// DirEntry represents a single directory entry mapping a name to an inode.
type DirEntry struct {
	Name string
	Ino  uint64
	Type InodeType
}

// MetadataClient defines the interface for inode and directory entry operations.
type MetadataClient interface {
	CreateInode(ctx context.Context, meta *InodeMeta) error
	GetInode(ctx context.Context, ino uint64) (*InodeMeta, error)
	UpdateInode(ctx context.Context, meta *InodeMeta) error
	DeleteInode(ctx context.Context, ino uint64) error
	CreateDirEntry(ctx context.Context, parentIno uint64, entry *DirEntry) error
	DeleteDirEntry(ctx context.Context, parentIno uint64, name string) error
	LookupDirEntry(ctx context.Context, parentIno uint64, name string) (*DirEntry, error)
	ListDirectory(ctx context.Context, parentIno uint64) ([]*DirEntry, error)
}

// ChunkClient defines the interface for reading and writing data chunks.
type ChunkClient interface {
	ReadChunks(ctx context.Context, chunkIDs []string, offset, length int64) ([]byte, error)
	WriteChunks(ctx context.Context, data []byte) (chunkIDs []string, err error)
}

// FileSystem provides a POSIX-like virtual filesystem layer on top of metadata
// and chunk storage.
type FileSystem struct {
	meta    MetadataClient
	chunks  ChunkClient
	nextIno atomic.Uint64
}

// RootIno is the inode number reserved for the root directory.
const RootIno uint64 = 1

// NewFileSystem creates a new FileSystem, initializing the root directory inode
// if it does not already exist.
func NewFileSystem(meta MetadataClient, chunks ChunkClient) *FileSystem {
	fs := &FileSystem{
		meta:   meta,
		chunks: chunks,
	}
	// Reserve inode 1 for root; next allocatable inode starts at 2.
	fs.nextIno.Store(2)

	// Initialize root inode if it does not exist.
	ctx := context.Background()
	if _, err := meta.GetInode(ctx, RootIno); err != nil {
		now := time.Now().UnixNano()
		root := &InodeMeta{
			Ino:       RootIno,
			Type:      TypeDir,
			Mode:      0755,
			UID:       0,
			GID:       0,
			LinkCount: 2,
			ATime:     now,
			MTime:     now,
			CTime:     now,
		}
		// Best effort — if this fails, subsequent operations will surface the error.
		_ = meta.CreateInode(ctx, root)
	}

	return fs
}

// allocIno returns the next available inode number.
func (fs *FileSystem) allocIno() uint64 {
	return fs.nextIno.Add(1) - 1
}

// Stat returns the inode metadata for the given inode number.
func (fs *FileSystem) Stat(ctx context.Context, ino uint64) (*InodeMeta, error) {
	return fs.meta.GetInode(ctx, ino)
}

// Lookup resolves a name within a directory and returns the target inode metadata.
func (fs *FileSystem) Lookup(ctx context.Context, parentIno uint64, name string) (*InodeMeta, error) {
	entry, err := fs.meta.LookupDirEntry(ctx, parentIno, name)
	if err != nil {
		return nil, fmt.Errorf("lookup %q in inode %d: %w", name, parentIno, err)
	}
	return fs.meta.GetInode(ctx, entry.Ino)
}

// Mkdir creates a new directory within the specified parent.
func (fs *FileSystem) Mkdir(ctx context.Context, parentIno uint64, name string, mode uint32) (*InodeMeta, error) {
	ino := fs.allocIno()
	now := time.Now().UnixNano()

	meta := &InodeMeta{
		Ino:       ino,
		Type:      TypeDir,
		Mode:      mode,
		LinkCount: 2,
		ATime:     now,
		MTime:     now,
		CTime:     now,
	}
	if err := fs.meta.CreateInode(ctx, meta); err != nil {
		return nil, fmt.Errorf("creating dir inode: %w", err)
	}

	entry := &DirEntry{Name: name, Ino: ino, Type: TypeDir}
	if err := fs.meta.CreateDirEntry(ctx, parentIno, entry); err != nil {
		return nil, fmt.Errorf("creating dir entry: %w", err)
	}

	return meta, nil
}

// Create creates a new regular file within the specified parent.
func (fs *FileSystem) Create(ctx context.Context, parentIno uint64, name string, mode uint32) (*InodeMeta, error) {
	ino := fs.allocIno()
	now := time.Now().UnixNano()

	meta := &InodeMeta{
		Ino:       ino,
		Type:      TypeFile,
		Mode:      mode,
		LinkCount: 1,
		ATime:     now,
		MTime:     now,
		CTime:     now,
	}
	if err := fs.meta.CreateInode(ctx, meta); err != nil {
		return nil, fmt.Errorf("creating file inode: %w", err)
	}

	entry := &DirEntry{Name: name, Ino: ino, Type: TypeFile}
	if err := fs.meta.CreateDirEntry(ctx, parentIno, entry); err != nil {
		return nil, fmt.Errorf("creating dir entry: %w", err)
	}

	return meta, nil
}

// Unlink removes a directory entry and decrements the target inode's link count.
// If the link count reaches zero, the inode is deleted.
func (fs *FileSystem) Unlink(ctx context.Context, parentIno uint64, name string) error {
	entry, err := fs.meta.LookupDirEntry(ctx, parentIno, name)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", name, err)
	}

	if err := fs.meta.DeleteDirEntry(ctx, parentIno, name); err != nil {
		return fmt.Errorf("deleting dir entry: %w", err)
	}

	inode, err := fs.meta.GetInode(ctx, entry.Ino)
	if err != nil {
		return fmt.Errorf("getting inode %d: %w", entry.Ino, err)
	}

	if inode.LinkCount <= 1 {
		return fs.meta.DeleteInode(ctx, entry.Ino)
	}

	inode.LinkCount--
	inode.CTime = time.Now().UnixNano()
	return fs.meta.UpdateInode(ctx, inode)
}

// Rmdir removes an empty directory. It returns an error if the directory is not empty.
func (fs *FileSystem) Rmdir(ctx context.Context, parentIno uint64, name string) error {
	entry, err := fs.meta.LookupDirEntry(ctx, parentIno, name)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", name, err)
	}

	if entry.Type != TypeDir {
		return fmt.Errorf("%q is not a directory", name)
	}

	children, err := fs.meta.ListDirectory(ctx, entry.Ino)
	if err != nil {
		return fmt.Errorf("listing directory %d: %w", entry.Ino, err)
	}
	if len(children) > 0 {
		return fmt.Errorf("directory %q is not empty", name)
	}

	if err := fs.meta.DeleteDirEntry(ctx, parentIno, name); err != nil {
		return fmt.Errorf("deleting dir entry: %w", err)
	}

	return fs.meta.DeleteInode(ctx, entry.Ino)
}

// ReadDir returns all directory entries within the given directory inode.
func (fs *FileSystem) ReadDir(ctx context.Context, ino uint64) ([]*DirEntry, error) {
	return fs.meta.ListDirectory(ctx, ino)
}

// Read reads data from a file at the given offset and length.
func (fs *FileSystem) Read(ctx context.Context, ino uint64, offset, length int64) ([]byte, error) {
	inode, err := fs.meta.GetInode(ctx, ino)
	if err != nil {
		return nil, fmt.Errorf("getting inode %d: %w", ino, err)
	}
	if inode.Type != TypeFile {
		return nil, fmt.Errorf("inode %d is not a file", ino)
	}
	if len(inode.ChunkIDs) == 0 {
		return nil, nil
	}
	return fs.chunks.ReadChunks(ctx, inode.ChunkIDs, offset, length)
}

// Write writes data to a file at the given offset. It reads the existing content,
// splices in the new data, writes new chunks, and updates the inode metadata.
func (fs *FileSystem) Write(ctx context.Context, ino uint64, offset int64, data []byte) (int, error) {
	inode, err := fs.meta.GetInode(ctx, ino)
	if err != nil {
		return 0, fmt.Errorf("getting inode %d: %w", ino, err)
	}
	if inode.Type != TypeFile {
		return 0, fmt.Errorf("inode %d is not a file", ino)
	}

	// Read existing data if any.
	var existing []byte
	if len(inode.ChunkIDs) > 0 {
		existing, err = fs.chunks.ReadChunks(ctx, inode.ChunkIDs, 0, inode.Size)
		if err != nil {
			return 0, fmt.Errorf("reading existing chunks: %w", err)
		}
	}

	// Calculate the total size after write.
	endOffset := offset + int64(len(data))
	newSize := int64(len(existing))
	if endOffset > newSize {
		newSize = endOffset
	}

	// Build the new content buffer.
	buf := make([]byte, newSize)
	copy(buf, existing)
	copy(buf[offset:], data)

	// Write new chunks.
	chunkIDs, err := fs.chunks.WriteChunks(ctx, buf)
	if err != nil {
		return 0, fmt.Errorf("writing chunks: %w", err)
	}

	// Update inode.
	now := time.Now().UnixNano()
	inode.ChunkIDs = chunkIDs
	inode.Size = int64(len(buf))
	inode.MTime = now
	inode.CTime = now
	if err := fs.meta.UpdateInode(ctx, inode); err != nil {
		return 0, fmt.Errorf("updating inode: %w", err)
	}

	return len(data), nil
}

// Rename moves a directory entry from one parent to another (or renames within the same parent).
func (fs *FileSystem) Rename(ctx context.Context, oldParent uint64, oldName string, newParent uint64, newName string) error {
	entry, err := fs.meta.LookupDirEntry(ctx, oldParent, oldName)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", oldName, err)
	}

	// Create entry in new location.
	newEntry := &DirEntry{Name: newName, Ino: entry.Ino, Type: entry.Type}
	if err := fs.meta.CreateDirEntry(ctx, newParent, newEntry); err != nil {
		return fmt.Errorf("creating new dir entry: %w", err)
	}

	// Remove old entry.
	if err := fs.meta.DeleteDirEntry(ctx, oldParent, oldName); err != nil {
		return fmt.Errorf("deleting old dir entry: %w", err)
	}

	return nil
}

// Symlink creates a symbolic link within the specified parent directory.
func (fs *FileSystem) Symlink(ctx context.Context, parentIno uint64, name, target string) (*InodeMeta, error) {
	ino := fs.allocIno()
	now := time.Now().UnixNano()

	meta := &InodeMeta{
		Ino:       ino,
		Type:      TypeSymlink,
		Mode:      0777,
		LinkCount: 1,
		Target:    target,
		ATime:     now,
		MTime:     now,
		CTime:     now,
	}
	if err := fs.meta.CreateInode(ctx, meta); err != nil {
		return nil, fmt.Errorf("creating symlink inode: %w", err)
	}

	entry := &DirEntry{Name: name, Ino: ino, Type: TypeSymlink}
	if err := fs.meta.CreateDirEntry(ctx, parentIno, entry); err != nil {
		return nil, fmt.Errorf("creating dir entry: %w", err)
	}

	return meta, nil
}

// Readlink returns the target path of a symbolic link.
func (fs *FileSystem) Readlink(ctx context.Context, ino uint64) (string, error) {
	inode, err := fs.meta.GetInode(ctx, ino)
	if err != nil {
		return "", fmt.Errorf("getting inode %d: %w", ino, err)
	}
	if inode.Type != TypeSymlink {
		return "", fmt.Errorf("inode %d is not a symlink", ino)
	}
	return inode.Target, nil
}
