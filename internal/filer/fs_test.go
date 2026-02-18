package filer

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// memMeta is an in-memory implementation of MetadataClient for testing.
type memMeta struct {
	mu      sync.RWMutex
	inodes  map[uint64]*InodeMeta
	dirents map[string]*DirEntry // key: "parentIno/name"
}

func newMemMeta() *memMeta {
	return &memMeta{
		inodes:  make(map[uint64]*InodeMeta),
		dirents: make(map[string]*DirEntry),
	}
}

func direntKey(parentIno uint64, name string) string {
	return fmt.Sprintf("%d/%s", parentIno, name)
}

func direntPrefix(parentIno uint64) string {
	return fmt.Sprintf("%d/", parentIno)
}

func (m *memMeta) CreateInode(_ context.Context, meta *InodeMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *meta
	if meta.ChunkIDs != nil {
		cp.ChunkIDs = make([]string, len(meta.ChunkIDs))
		copy(cp.ChunkIDs, meta.ChunkIDs)
	}
	if meta.Xattrs != nil {
		cp.Xattrs = make(map[string]string, len(meta.Xattrs))
		for k, v := range meta.Xattrs {
			cp.Xattrs[k] = v
		}
	}
	m.inodes[meta.Ino] = &cp
	return nil
}

func (m *memMeta) GetInode(_ context.Context, ino uint64) (*InodeMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.inodes[ino]
	if !ok {
		return nil, fmt.Errorf("inode %d not found", ino)
	}
	cp := *meta
	if meta.ChunkIDs != nil {
		cp.ChunkIDs = make([]string, len(meta.ChunkIDs))
		copy(cp.ChunkIDs, meta.ChunkIDs)
	}
	return &cp, nil
}

func (m *memMeta) UpdateInode(_ context.Context, meta *InodeMeta) error {
	return m.CreateInode(context.Background(), meta)
}

func (m *memMeta) DeleteInode(_ context.Context, ino uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inodes, ino)
	return nil
}

func (m *memMeta) CreateDirEntry(_ context.Context, parentIno uint64, entry *DirEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *entry
	m.dirents[direntKey(parentIno, entry.Name)] = &cp
	return nil
}

func (m *memMeta) DeleteDirEntry(_ context.Context, parentIno uint64, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.dirents, direntKey(parentIno, name))
	return nil
}

func (m *memMeta) LookupDirEntry(_ context.Context, parentIno uint64, name string) (*DirEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.dirents[direntKey(parentIno, name)]
	if !ok {
		return nil, fmt.Errorf("entry %q not found in inode %d", name, parentIno)
	}
	cp := *entry
	return &cp, nil
}

func (m *memMeta) ListDirectory(_ context.Context, parentIno uint64) ([]*DirEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	prefix := direntPrefix(parentIno)
	var result []*DirEntry
	for k, v := range m.dirents {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			cp := *v
			result = append(result, &cp)
		}
	}
	return result, nil
}

// memChunks is an in-memory implementation of ChunkClient for testing.
type memChunks struct {
	mu     sync.Mutex
	nextID int
	store  map[string][]byte
}

func newMemChunks() *memChunks {
	return &memChunks{
		store: make(map[string][]byte),
	}
}

func (c *memChunks) ReadChunks(_ context.Context, chunkIDs []string, offset, length int64) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Concatenate all chunks in order.
	var all []byte
	for _, id := range chunkIDs {
		data, ok := c.store[id]
		if !ok {
			return nil, fmt.Errorf("chunk %s not found", id)
		}
		all = append(all, data...)
	}

	// Apply offset and length.
	if offset >= int64(len(all)) {
		return nil, nil
	}
	end := offset + length
	if end > int64(len(all)) {
		end = int64(len(all))
	}
	result := make([]byte, end-offset)
	copy(result, all[offset:end])
	return result, nil
}

func (c *memChunks) WriteChunks(_ context.Context, data []byte) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Store all data as a single chunk for simplicity.
	id := fmt.Sprintf("chunk-%d", c.nextID)
	c.nextID++
	stored := make([]byte, len(data))
	copy(stored, data)
	c.store[id] = stored
	return []string{id}, nil
}

func setupTestFS() (*FileSystem, *memMeta, *memChunks) {
	meta := newMemMeta()
	chunks := newMemChunks()
	fs := NewFileSystem(meta, chunks)
	return fs, meta, chunks
}

func TestMkdir(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	dir, err := fs.Mkdir(ctx, RootIno, "mydir", 0755)
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if dir.Type != TypeDir {
		t.Errorf("expected dir type, got %s", dir.Type)
	}
	if dir.Mode != 0755 {
		t.Errorf("expected mode 0755, got %o", dir.Mode)
	}
	if dir.LinkCount != 2 {
		t.Errorf("expected linkCount 2, got %d", dir.LinkCount)
	}

	// Verify lookup.
	got, err := fs.Lookup(ctx, RootIno, "mydir")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.Ino != dir.Ino {
		t.Errorf("lookup inode mismatch: got %d, want %d", got.Ino, dir.Ino)
	}
}

func TestCreateFile(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	file, err := fs.Create(ctx, RootIno, "hello.txt", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if file.Type != TypeFile {
		t.Errorf("expected file type, got %s", file.Type)
	}
	if file.Mode != 0644 {
		t.Errorf("expected mode 0644, got %o", file.Mode)
	}
	if file.LinkCount != 1 {
		t.Errorf("expected linkCount 1, got %d", file.LinkCount)
	}
}

func TestLookup(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	_, err := fs.Create(ctx, RootIno, "test.txt", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := fs.Lookup(ctx, RootIno, "test.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.Type != TypeFile {
		t.Errorf("expected file type, got %s", got.Type)
	}

	// Lookup non-existent.
	_, err = fs.Lookup(ctx, RootIno, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent entry")
	}
}

func TestReadDir(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	_, _ = fs.Create(ctx, RootIno, "a.txt", 0644)
	_, _ = fs.Create(ctx, RootIno, "b.txt", 0644)
	_, _ = fs.Mkdir(ctx, RootIno, "subdir", 0755)

	entries, err := fs.ReadDir(ctx, RootIno)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	names := make(map[string]InodeType)
	for _, e := range entries {
		names[e.Name] = e.Type
	}
	if names["a.txt"] != TypeFile || names["b.txt"] != TypeFile || names["subdir"] != TypeDir {
		t.Errorf("unexpected entries: %v", names)
	}
}

func TestUnlink(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	_, err := fs.Create(ctx, RootIno, "gone.txt", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := fs.Unlink(ctx, RootIno, "gone.txt"); err != nil {
		t.Fatalf("Unlink: %v", err)
	}

	_, err = fs.Lookup(ctx, RootIno, "gone.txt")
	if err == nil {
		t.Error("expected error after unlink")
	}
}

func TestRmdir_Empty(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	_, err := fs.Mkdir(ctx, RootIno, "emptydir", 0755)
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	if err := fs.Rmdir(ctx, RootIno, "emptydir"); err != nil {
		t.Fatalf("Rmdir: %v", err)
	}

	_, err = fs.Lookup(ctx, RootIno, "emptydir")
	if err == nil {
		t.Error("expected error after rmdir")
	}
}

func TestRmdir_NotEmpty(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	dir, err := fs.Mkdir(ctx, RootIno, "fulldir", 0755)
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	_, err = fs.Create(ctx, dir.Ino, "child.txt", 0644)
	if err != nil {
		t.Fatalf("Create child: %v", err)
	}

	err = fs.Rmdir(ctx, RootIno, "fulldir")
	if err == nil {
		t.Error("expected error when removing non-empty directory")
	}
}

func TestWriteRead(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	file, err := fs.Create(ctx, RootIno, "data.bin", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	payload := []byte("Hello, NovaStor!")
	n, err := fs.Write(ctx, file.Ino, 0, payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(payload) {
		t.Errorf("expected write of %d bytes, got %d", len(payload), n)
	}

	// Read back full content.
	data, err := fs.Read(ctx, file.Ino, 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "Hello, NovaStor!" {
		t.Errorf("read mismatch: got %q", data)
	}

	// Read a substring.
	partial, err := fs.Read(ctx, file.Ino, 7, 9)
	if err != nil {
		t.Fatalf("Read partial: %v", err)
	}
	if string(partial) != "NovaStor!" {
		t.Errorf("partial read mismatch: got %q", partial)
	}

	// Overwrite at offset.
	n, err = fs.Write(ctx, file.Ino, 7, []byte("World!!!!!!"))
	if err != nil {
		t.Fatalf("Write at offset: %v", err)
	}
	if n != 11 {
		t.Errorf("expected write of 11 bytes, got %d", n)
	}
	full, err := fs.Read(ctx, file.Ino, 0, 100)
	if err != nil {
		t.Fatalf("Read after overwrite: %v", err)
	}
	expected := "Hello, World!!!!!!"
	if string(full) != expected {
		t.Errorf("after overwrite: got %q, want %q", full, expected)
	}
}

func TestRename(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	file, err := fs.Create(ctx, RootIno, "old.txt", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := fs.Rename(ctx, RootIno, "old.txt", RootIno, "new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Old name should be gone.
	_, err = fs.Lookup(ctx, RootIno, "old.txt")
	if err == nil {
		t.Error("expected error for old name after rename")
	}

	// New name should resolve to the same inode.
	got, err := fs.Lookup(ctx, RootIno, "new.txt")
	if err != nil {
		t.Fatalf("Lookup new name: %v", err)
	}
	if got.Ino != file.Ino {
		t.Errorf("inode mismatch after rename: got %d, want %d", got.Ino, file.Ino)
	}

	// Rename across directories.
	dir, err := fs.Mkdir(ctx, RootIno, "dest", 0755)
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := fs.Rename(ctx, RootIno, "new.txt", dir.Ino, "moved.txt"); err != nil {
		t.Fatalf("Rename across dirs: %v", err)
	}
	moved, err := fs.Lookup(ctx, dir.Ino, "moved.txt")
	if err != nil {
		t.Fatalf("Lookup moved: %v", err)
	}
	if moved.Ino != file.Ino {
		t.Errorf("inode mismatch after cross-dir rename: got %d, want %d", moved.Ino, file.Ino)
	}
}

func TestSymlink(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	_, err := fs.Create(ctx, RootIno, "target.txt", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	link, err := fs.Symlink(ctx, RootIno, "link.txt", "target.txt")
	if err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	if link.Type != TypeSymlink {
		t.Errorf("expected symlink type, got %s", link.Type)
	}
	if link.Target != "target.txt" {
		t.Errorf("expected target 'target.txt', got %q", link.Target)
	}

	// Readlink.
	target, err := fs.Readlink(ctx, link.Ino)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "target.txt" {
		t.Errorf("readlink mismatch: got %q", target)
	}

	// Lookup the symlink entry.
	looked, err := fs.Lookup(ctx, RootIno, "link.txt")
	if err != nil {
		t.Fatalf("Lookup symlink: %v", err)
	}
	if looked.Type != TypeSymlink {
		t.Errorf("lookup type mismatch: got %s", looked.Type)
	}
}

func TestTruncate(t *testing.T) {
	fs, _, _ := setupTestFS()
	ctx := context.Background()

	// Create a file with some data.
	file, err := fs.Create(ctx, RootIno, "data.bin", 0644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	payload := []byte("Hello, NovaStor! This is some test data.")
	_, err = fs.Write(ctx, file.Ino, 0, payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify initial size.
	meta, err := fs.Stat(ctx, file.Ino)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	initialSize := meta.Size
	if initialSize != int64(len(payload)) {
		t.Fatalf("initial size mismatch: got %d, want %d", initialSize, len(payload))
	}

	// Truncate to smaller size.
	newSize := int64(10)
	if err := fs.Truncate(ctx, file.Ino, newSize); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Verify truncated size.
	meta, err = fs.Stat(ctx, file.Ino)
	if err != nil {
		t.Fatalf("Stat after truncate: %v", err)
	}
	if meta.Size != newSize {
		t.Errorf("size after truncate: got %d, want %d", meta.Size, newSize)
	}

	// Verify data was truncated.
	data, err := fs.Read(ctx, file.Ino, 0, 100)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	expectedData := payload[:newSize]
	if string(data) != string(expectedData) {
		t.Errorf("data after truncate: got %q, want %q", data, expectedData)
	}

	// Truncate to zero.
	if err := fs.Truncate(ctx, file.Ino, 0); err != nil {
		t.Fatalf("Truncate to zero: %v", err)
	}

	meta, err = fs.Stat(ctx, file.Ino)
	if err != nil {
		t.Fatalf("Stat after truncate to zero: %v", err)
	}
	if meta.Size != 0 {
		t.Errorf("size after truncate to zero: got %d, want 0", meta.Size)
	}

	data, err = fs.Read(ctx, file.Ino, 0, 100)
	if err != nil {
		t.Fatalf("Read after truncate to zero: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("data after truncate to zero: got %d bytes, want 0", len(data))
	}

	// Truncate to larger size (extend with zeros).
	extendedSize := int64(20)
	if err := fs.Truncate(ctx, file.Ino, extendedSize); err != nil {
		t.Fatalf("Truncate extend: %v", err)
	}

	meta, err = fs.Stat(ctx, file.Ino)
	if err != nil {
		t.Fatalf("Stat after extend: %v", err)
	}
	if meta.Size != extendedSize {
		t.Errorf("size after extend: got %d, want %d", meta.Size, extendedSize)
	}

	data, err = fs.Read(ctx, file.Ino, 0, 100)
	if err != nil {
		t.Fatalf("Read after extend: %v", err)
	}
	if int64(len(data)) != extendedSize {
		t.Errorf("data length after extend: got %d, want %d", len(data), extendedSize)
	}
	// All bytes should be zero since we truncated to 0 first.
	for i, b := range data {
		if b != 0 {
			t.Errorf("data[%d] after extend: got %d, want 0", i, b)
		}
	}
}
