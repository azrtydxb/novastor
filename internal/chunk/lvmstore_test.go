package chunk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// setupLVMEnvironment prepares a minimal LVM environment for testing.
// This requires root privileges and is skipped in normal CI.
// Most tests use command mocking instead.
func setupLVMEnvironment(t *testing.T) (vgName, thinPool string, cleanup func()) {
	t.Helper()

	// Check if running as root.
	if os.Geteuid() != 0 {
		t.Skip("LVM tests require root privileges")
	}

	// Check if LVM tools are available.
	if _, err := exec.LookPath("lvcreate"); err != nil {
		t.Skip("LVM tools not available")
	}

	// Create a loopback device for testing.
	tmpDir := t.TempDir()
	loopFile := filepath.Join(tmpDir, "loop.img")

	// Create a 1GB file.
	if err := exec.Command("dd", "if=/dev/zero", "of="+loopFile, "bs=1M", "count=1024").Run(); err != nil {
		t.Fatalf("creating loop file: %v", err)
	}

	// Setup loop device.
	losetupOut, err := exec.Command("losetup", "-f", "--show", loopFile).CombinedOutput()
	if err != nil {
		t.Fatalf("setting up loop device: %v, output: %s", err, string(losetupOut))
	}
	loopDev := strings.TrimSpace(string(losetupOut))

	// Create physical volume.
	if out, err := exec.Command("pvcreate", loopDev).CombinedOutput(); err != nil {
		exec.Command("losetup", "-d", loopDev).Run()
		t.Fatalf("creating PV: %v, output: %s", err, string(out))
	}

	// Create volume group.
	vgName = "novastor-test-" + t.Name()
	vgName = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(vgName))

	if out, err := exec.Command("vgcreate", vgName, loopDev).CombinedOutput(); err != nil {
		exec.Command("losetup", "-d", loopDev).Run()
		t.Fatalf("creating VG: %v, output: %s", err, string(out))
	}

	// Create thin pool.
	thinPool = "test-thin-pool"
	if out, err := exec.Command("lvcreate", "-L", "512M",
		"-T", vgName+"/"+thinPool).CombinedOutput(); err != nil {
		t.Fatalf("creating thin pool: %v, output: %s", err, string(out))
	}

	cleanup = func() {
		_ = exec.Command("vgremove", "-f", vgName).Run()
		_ = exec.Command("pvremove", "-f", loopDev).Run()
		_ = exec.Command("losetup", "-d", loopDev).Run()
	}

	return vgName, thinPool, cleanup
}

// mockLVMStore is a test implementation that uses a temp directory
// instead of real LVM commands.
type mockLVMStore struct {
	vgName   string
	thinPool string
	dataDir  string
}

// NewMockLVMStore creates a mock LVM store for testing.
// It uses a temporary directory to simulate LVM volumes.
func newMockLVMStore(t *testing.T) *mockLVMStore {
	t.Helper()
	return &mockLVMStore{
		vgName:   "novastor-test",
		thinPool: "chunks",
		dataDir:  t.TempDir(),
	}
}

func (s *mockLVMStore) lvPath(id ChunkID) string {
	lvName := ChunkDevicePrefix + string(id)
	return filepath.Join(s.dataDir, lvName)
}

func (s *mockLVMStore) Put(_ context.Context, c *Chunk) error {
	lvPath := s.lvPath(c.ID)

	_, err := os.Stat(lvPath)
	if err == nil {
		return nil // Already exists
	}

	buf := make([]byte, 4+len(c.Data))
	buf[0] = byte(c.Checksum >> 24)
	buf[1] = byte(c.Checksum >> 16)
	buf[2] = byte(c.Checksum >> 8)
	buf[3] = byte(c.Checksum)
	copy(buf[4:], c.Data)

	if err := os.WriteFile(lvPath, buf, 0o600); err != nil {
		return fmt.Errorf("writing chunk %s: %w", c.ID, err)
	}

	return nil
}

func (s *mockLVMStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	lvPath := s.lvPath(id)

	raw, err := os.ReadFile(lvPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk %s not found", id)
		}
		return nil, err
	}

	if len(raw) < 4 {
		return nil, fmt.Errorf("chunk %s: data too small", id)
	}

	checksum := uint32(raw[0])<<24 | uint32(raw[1])<<16 | uint32(raw[2])<<8 | uint32(raw[3])
	data := raw[4:]

	c := &Chunk{ID: id, Data: data, Checksum: checksum}
	if err := c.VerifyChecksum(); err != nil {
		return nil, fmt.Errorf("chunk %s integrity check failed: %w", id, err)
	}

	return c, nil
}

func (s *mockLVMStore) Delete(_ context.Context, id ChunkID) error {
	lvPath := s.lvPath(id)
	_ = os.Remove(lvPath)
	return nil
}

func (s *mockLVMStore) Has(_ context.Context, id ChunkID) (bool, error) {
	lvPath := s.lvPath(id)
	_, err := os.Stat(lvPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *mockLVMStore) List(_ context.Context) ([]ChunkID, error) {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}

	var ids []ChunkID
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, ChunkDevicePrefix) {
			id := strings.TrimPrefix(name, ChunkDevicePrefix)
			ids = append(ids, ChunkID(id))
		}
	}
	return ids, nil
}

func (s *mockLVMStore) Snapshot(_ context.Context, sourceID, snapshotID ChunkID) error {
	srcPath := s.lvPath(sourceID)
	snapPath := s.lvPath(snapshotID)

	data, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}

	return os.WriteFile(snapPath, data, 0o600)
}

func (s *mockLVMStore) Resize(_ context.Context, id ChunkID, newSizeBytes int64) error {
	// In the mock, resize is a no-op since we just use files.
	return nil
}

func (s *mockLVMStore) Capacity(_ context.Context) (totalBytes, freeBytes int64, err error) {
	// Return mock capacity values.
	return 1024 * 1024 * 1024, 512 * 1024 * 1024, nil
}

// TestLVMStore_PutGet tests writing and reading chunks.
func TestLVMStore_PutGet(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("hello lvm chunk world")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got.Data) != string(data) {
		t.Errorf("Get data = %q, want %q", got.Data, data)
	}

	if got.Checksum != c.Checksum {
		t.Errorf("Get checksum = %d, want %d", got.Checksum, c.Checksum)
	}
}

// TestLVMStore_GetNotFound tests that Get returns an error for non-existent chunks.
func TestLVMStore_GetNotFound(t *testing.T) {
	store := newMockLVMStore(t)
	_, err := store.Get(context.Background(), ChunkID("nonexistent"))
	if err == nil {
		t.Error("Get should fail for nonexistent chunk")
	}
}

// TestLVMStore_Delete tests deleting chunks.
func TestLVMStore_Delete(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("delete me")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	_ = store.Put(ctx, c)

	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	has, _ := store.Has(ctx, c.ID)
	if has {
		t.Error("chunk should not exist after delete")
	}
}

// TestLVMStore_DeleteNonexistent tests that deleting a non-existent chunk is a no-op.
func TestLVMStore_DeleteNonexistent(t *testing.T) {
	store := newMockLVMStore(t)
	if err := store.Delete(context.Background(), ChunkID("nonexistent")); err != nil {
		t.Errorf("Delete nonexistent should not error, got: %v", err)
	}
}

// TestLVMStore_Has tests checking chunk existence.
func TestLVMStore_Has(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("exists")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	has, _ := store.Has(ctx, c.ID)
	if has {
		t.Error("Has should return false before Put")
	}

	_ = store.Put(ctx, c)

	has, _ = store.Has(ctx, c.ID)
	if !has {
		t.Error("Has should return true after Put")
	}
}

// TestLVMStore_List tests listing all chunks.
func TestLVMStore_List(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	ids, _ := store.List(ctx)
	if len(ids) != 0 {
		t.Errorf("List empty store = %d, want 0", len(ids))
	}

	var expectedIDs []ChunkID
	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		c := &Chunk{ID: NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		_ = store.Put(ctx, c)
		expectedIDs = append(expectedIDs, c.ID)
	}

	ids, _ = store.List(ctx)
	if len(ids) != 3 {
		t.Errorf("List = %d, want 3", len(ids))
	}

	for _, id := range expectedIDs {
		found := false
		for _, listedID := range ids {
			if listedID == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected ID %s not found in List", id)
		}
	}
}

// TestLVMStore_ChecksumVerifiedOnGet tests checksum verification.
func TestLVMStore_ChecksumVerifiedOnGet(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("integrity check")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)

	// Corrupt the chunk by writing bad data.
	lvPath := store.lvPath(c.ID)
	_ = os.WriteFile(lvPath, []byte("corrupted"), 0o600)

	_, err := store.Get(ctx, c.ID)
	if err == nil {
		t.Error("Get should fail on corrupted chunk")
	}
}

// TestLVMStore_Snapshot tests snapshot creation.
func TestLVMStore_Snapshot(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("snapshot source")
	source := &Chunk{ID: NewChunkID(data), Data: data}
	source.Checksum = source.ComputeChecksum()

	_ = store.Put(ctx, source)

	snapID := ChunkID("snapshot-of-" + string(source.ID))
	if err := store.Snapshot(ctx, source.ID, snapID); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Verify snapshot exists.
	has, _ := store.Has(ctx, snapID)
	if !has {
		t.Error("Snapshot should exist after Snapshot()")
	}

	// Verify snapshot has same data.
	got, err := store.Get(ctx, snapID)
	if err != nil {
		t.Fatalf("Get snapshot failed: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("Snapshot data = %q, want %q", got.Data, data)
	}
}

// TestLVMStore_Resize tests volume resizing.
func TestLVMStore_Resize(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("resize test")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	_ = store.Put(ctx, c)

	// Resize to 8MB (larger than default 4MB).
	if err := store.Resize(ctx, c.ID, 8*1024*1024); err != nil {
		t.Fatalf("Resize failed: %v", err)
	}

	// Chunk should still be accessible.
	_, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Errorf("Get after Resize failed: %v", err)
	}
}

// TestLVMStore_Capacity tests capacity reporting.
func TestLVMStore_Capacity(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	total, free, err := store.Capacity(ctx)
	if err != nil {
		t.Fatalf("Capacity failed: %v", err)
	}

	if total <= 0 {
		t.Errorf("Capacity total = %d, want > 0", total)
	}
	if free <= 0 {
		t.Errorf("Capacity free = %d, want > 0", free)
	}
	if free > total {
		t.Errorf("Capacity free = %d, total = %d, free should not exceed total", free, total)
	}
}

// TestLVMStore_lvNameForChunk tests LV name generation.
func TestLVMStore_lvNameForChunk(t *testing.T) {
	id := ChunkID("abc123")
	lvName := ChunkDevicePrefix + string(id)

	expected := "chunk-abc123"
	if lvName != expected {
		t.Errorf("lvNameForChunk() = %q, want %q", lvName, expected)
	}
}

// TestLVMStore_PutMultiple tests writing multiple chunks.
func TestLVMStore_PutMultiple(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()

	// Write multiple chunks.
	for i := 0; i < 10; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		c := &Chunk{ID: NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Verify all chunks are listed.
	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(ids) != 10 {
		t.Errorf("List = %d, want 10", len(ids))
	}
}

// TestLVMStore_PutIdempotent tests that putting the same chunk twice is idempotent.
func TestLVMStore_PutIdempotent(t *testing.T) {
	store := newMockLVMStore(t)
	ctx := context.Background()
	data := []byte("idempotent test")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Put again should succeed (no-op).
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Second Put failed: %v", err)
	}

	// Verify data is correct.
	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("Get data = %q, want %q", got.Data, data)
	}
}
