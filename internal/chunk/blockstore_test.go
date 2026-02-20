package chunk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestBitmapBasic tests basic bitmap operations.
func TestBitmapBasic(t *testing.T) {
	const totalBlocks = 10000
	bm := NewBitmap(totalBlocks)

	if bm.TotalBlocks() != totalBlocks {
		t.Errorf("expected TotalBlocks=%d, got %d", totalBlocks, bm.TotalBlocks())
	}

	if bm.UsedBlocks() != 0 {
		t.Errorf("expected UsedBlocks=0, got %d", bm.UsedBlocks())
	}

	if bm.FreeBlocks() != totalBlocks {
		t.Errorf("expected FreeBlocks=%d, got %d", totalBlocks, bm.FreeBlocks())
	}
}

// TestBitmapSetClear tests setting and clearing blocks.
func TestBitmapSetClear(t *testing.T) {
	const totalBlocks = 10000
	bm := NewBitmap(totalBlocks)

	// Set a block.
	if err := bm.Set(100); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if !bm.IsSet(100) {
		t.Error("expected block 100 to be set")
	}

	if bm.UsedBlocks() != 1 {
		t.Errorf("expected UsedBlocks=1, got %d", bm.UsedBlocks())
	}

	// Clear the block.
	if err := bm.Clear(100); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	if bm.IsSet(100) {
		t.Error("expected block 100 to be clear")
	}

	if bm.UsedBlocks() != 0 {
		t.Errorf("expected UsedBlocks=0, got %d", bm.UsedBlocks())
	}
}

// TestBitmapAlloc tests block allocation.
func TestBitmapAlloc(t *testing.T) {
	const totalBlocks = 10000
	bm := NewBitmap(totalBlocks)

	// Allocate 100 blocks.
	start, err := bm.Alloc(100)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	if start != 0 {
		t.Errorf("expected start=0, got %d", start)
	}

	if bm.UsedBlocks() != 100 {
		t.Errorf("expected UsedBlocks=100, got %d", bm.UsedBlocks())
	}

	// Allocate another 100 blocks.
	start, err = bm.Alloc(100)
	if err != nil {
		t.Fatalf("second Alloc failed: %v", err)
	}

	if start != 100 {
		t.Errorf("expected start=100, got %d", start)
	}

	// Allocate the rest.
	remaining := uint64(totalBlocks - 200)
	start, err = bm.Alloc(remaining)
	if err != nil {
		t.Fatalf("Alloc remaining failed: %v", err)
	}

	if start != 200 {
		t.Errorf("expected start=200, got %d", start)
	}

	// Should be out of space now.
	_, err = bm.Alloc(1)
	if err != ErrOutOfSpace {
		t.Errorf("expected ErrOutOfSpace, got %v", err)
	}
}

// TestBitmapFree tests freeing blocks.
func TestBitmapFree(t *testing.T) {
	const totalBlocks = 10000
	bm := NewBitmap(totalBlocks)

	// Allocate some blocks.
	start, err := bm.Alloc(500)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Free them.
	if err := bm.Free(start, 500); err != nil {
		t.Fatalf("Free failed: %v", err)
	}

	if bm.UsedBlocks() != 0 {
		t.Errorf("expected UsedBlocks=0, got %d", bm.UsedBlocks())
	}

	if bm.FreeBlocks() != totalBlocks {
		t.Errorf("expected FreeBlocks=%d, got %d", totalBlocks, bm.FreeBlocks())
	}
}

// TestBitmapFragmentation tests fragmentation calculation.
func TestBitmapFragmentation(t *testing.T) {
	const totalBlocks = 10000
	bm := NewBitmap(totalBlocks)

	// Empty bitmap should have 0 fragmentation.
	if frag := bm.Fragmentation(); frag != 0 {
		t.Errorf("expected fragmentation=0, got %f", frag)
	}

	// Fully used bitmap should have 0 fragmentation.
	for i := uint64(0); i < totalBlocks; i++ {
		_ = bm.Set(i)
	}
	if frag := bm.Fragmentation(); frag != 0 {
		t.Errorf("expected fragmentation=0 for full bitmap, got %f", frag)
	}

	// Clear every other block to create fragmentation.
	for i := uint64(0); i < totalBlocks; i += 2 {
		_ = bm.Clear(i)
	}

	frag := bm.Fragmentation()
	if frag <= 0 || frag > 1 {
		t.Errorf("expected fragmentation between 0 and 1, got %f", frag)
	}

	// With this pattern, fragmentation should be high.
	if frag < 0.5 {
		t.Errorf("expected high fragmentation for checkerboard pattern, got %f", frag)
	}
}

// TestBitmapPersistence tests saving and loading bitmaps.
func TestBitmapPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	bitmapPath := filepath.Join(tmpDir, "test_bitmap.bin")

	const totalBlocks = 10000

	// Create and modify a bitmap.
	bm1 := NewBitmap(totalBlocks)
	bm1.SetPath(bitmapPath)

	_, err := bm1.Alloc(100)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	if err := bm1.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Load the bitmap.
	bm2, err := OpenBitmap(bitmapPath, totalBlocks)
	if err != nil {
		t.Fatalf("OpenBitmap failed: %v", err)
	}

	if bm2.UsedBlocks() != 100 {
		t.Errorf("expected UsedBlocks=100, got %d", bm2.UsedBlocks())
	}

	// Verify the blocks are set.
	for i := uint64(0); i < 100; i++ {
		if !bm2.IsSet(i) {
			t.Errorf("expected block %d to be set", i)
		}
	}

	if bm2.IsSet(100) {
		t.Error("expected block 100 to be clear")
	}
}

// TestBitmapConcurrent tests concurrent bitmap operations.
func TestBitmapConcurrent(t *testing.T) {
	const totalBlocks = 10000
	bm := NewBitmap(totalBlocks)
	bm.SetAutoSync(false)

	done := make(chan bool, 10)

	// Start multiple goroutines setting and clearing blocks.
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				block := uint64(id*100 + j)
				_ = bm.Set(block)
				_ = bm.Clear(block)
			}
			done <- true
		}(i)
	}

	// Start goroutines allocating.
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				_, _ = bm.Alloc(10)
			}
			done <- true
		}()
	}

	// Wait for completion.
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify consistency.
	if bm.UsedBlocks() > bm.TotalBlocks() {
		t.Errorf("UsedBlocks %d exceeds TotalBlocks %d", bm.UsedBlocks(), bm.TotalBlocks())
	}
}

// TestBlockStoreConformance tests the block store against the conformance suite.
// This uses a temporary file instead of a real block device.
func TestBlockStoreConformance(t *testing.T) {
	// Conformance test runs many sub-tests on the same store, requiring more space.
	// Skip in short mode as it would need a much larger device.
	if testing.Short() {
		t.Skip("skipping conformance test in short mode (requires larger device)")
	}

	// Create a temporary file to simulate a block device.
	tmpDir := t.TempDir()
	devicePath := filepath.Join(tmpDir, "device.img")

	// Create a device file.
	const deviceSize = 100 * 1024 * 1024
	f, err := os.Create(devicePath)
	if err != nil {
		t.Fatalf("creating device file: %v", err)
	}
	if err := f.Truncate(int64(deviceSize)); err != nil {
		f.Close()
		t.Fatalf("truncating device file: %v", err)
	}
	f.Close()

	// Create block store.
	config := BlockStoreConfig{
		Devices:     []string{devicePath},
		MetadataDir: tmpDir,
		AutoSync:    true,
		Logger:      nil,
	}

	store, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore failed: %v", err)
	}
	defer store.Close()

	// Run conformance tests.
	StoreConformanceTest(store, t)
}

// TestBlockStoreBasic tests basic block store operations.
func TestBlockStoreBasic(t *testing.T) {
	// Adaptive sizing: use smaller device in short mode
	deviceSize := 20 * 1024 * 1024 // 20MB for short mode
	if !testing.Short() {
		deviceSize = 100 * 1024 * 1024 // 100MB for full mode
	}

	tmpDir := t.TempDir()
	devicePath := filepath.Join(tmpDir, "device.img")
	f, err := os.Create(devicePath)
	if err != nil {
		t.Fatalf("creating device file: %v", err)
	}
	if err := f.Truncate(int64(deviceSize)); err != nil {
		f.Close()
		t.Fatalf("truncating device file: %v", err)
	}
	f.Close()

	config := BlockStoreConfig{
		Devices:     []string{devicePath},
		MetadataDir: tmpDir,
		AutoSync:    true,
	}

	store, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create a chunk.
	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]

	// Put the chunk.
	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the chunk.
	retrieved, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(retrieved.Data) != len(data) {
		t.Errorf("data size mismatch: got %d, want %d", len(retrieved.Data), len(data))
	}

	// Check the chunk exists.
	has, err := store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if !has {
		t.Error("expected Has to return true")
	}

	// List chunks.
	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(ids))
	}

	// Delete the chunk.
	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone.
	has, err = store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has failed after Delete: %v", err)
	}
	if has {
		t.Error("expected Has to return false after Delete")
	}
}

// TestBlockStoreMultiDevice tests using multiple devices.
func TestBlockStoreMultiDevice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-device test in short mode")
	}

	tmpDir := t.TempDir()

	// Create three device files.
	const deviceSize = 50 * 1024 * 1024
	var devices []string

	for i := 0; i < 3; i++ {
		devicePath := filepath.Join(tmpDir, fmt.Sprintf("device%d.img", i))
		f, err := os.Create(devicePath)
		if err != nil {
			t.Fatalf("creating device file %d: %v", i, err)
		}
		if err := f.Truncate(int64(deviceSize)); err != nil {
			f.Close()
			t.Fatalf("truncating device file %d: %v", i, err)
		}
		f.Close()
		devices = append(devices, devicePath)
	}

	config := BlockStoreConfig{
		Devices:     devices,
		MetadataDir: tmpDir,
		AutoSync:    true,
	}

	store, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create multiple chunks.
	const numChunks = 10
	var ids []ChunkID

	for i := 0; i < numChunks; i++ {
		data := make([]byte, ChunkSize)
		for j := range data {
			data[j] = byte(i)
		}
		chunks := SplitData(data)
		c := chunks[0]

		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put failed for chunk %d: %v", i, err)
		}
		ids = append(ids, c.ID)
	}

	// Verify all chunks can be retrieved.
	for _, id := range ids {
		_, err := store.Get(ctx, id)
		if err != nil {
			t.Errorf("Get failed for %s: %v", id, err)
		}
	}

	// Check stats.
	stats := store.Stats()
	if deviceCount, ok := stats["devices"].(int); !ok || deviceCount != 3 {
		t.Errorf("expected 3 devices, got %v", stats["devices"])
	}

	if chunkCount, ok := stats["chunks"].(int); !ok || chunkCount != numChunks {
		t.Errorf("expected %d chunks, got %v", numChunks, stats["chunks"])
	}
}

// TestBlockStoreRecovery tests metadata recovery across restarts.
func TestBlockStoreRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping recovery test in short mode")
	}

	tmpDir := t.TempDir()
	devicePath := filepath.Join(tmpDir, "device.img")

	const deviceSize = 100 * 1024 * 1024
	f, err := os.Create(devicePath)
	if err != nil {
		t.Fatalf("creating device file: %v", err)
	}
	if err := f.Truncate(int64(deviceSize)); err != nil {
		f.Close()
		t.Fatalf("truncating device file: %v", err)
	}
	f.Close()

	config := BlockStoreConfig{
		Devices:     []string{devicePath},
		MetadataDir: tmpDir,
		AutoSync:    true,
	}

	// Create first store and write chunks.
	store1, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore failed: %v", err)
	}

	ctx := context.Background()

	const numChunks = 5
	var ids []ChunkID

	for i := 0; i < numChunks; i++ {
		data := make([]byte, ChunkSize)
		for j := range data {
			data[j] = byte(i)
		}
		chunks := SplitData(data)
		c := chunks[0]

		if err := store1.Put(ctx, c); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		ids = append(ids, c.ID)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Create second store (should recover metadata).
	store2, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore (recovery) failed: %v", err)
	}
	defer store2.Close()

	// Verify all chunks can be retrieved.
	for _, id := range ids {
		retrieved, err := store2.Get(ctx, id)
		if err != nil {
			t.Errorf("Get failed for %s after recovery: %v", id, err)
		}
		if len(retrieved.Data) != ChunkSize {
			t.Errorf("chunk size mismatch after recovery")
		}
	}

	// Check that stats are correct.
	stats := store2.Stats()
	if chunkCount, ok := stats["chunks"].(int); !ok || chunkCount != numChunks {
		t.Errorf("expected %d chunks after recovery, got %v", numChunks, stats["chunks"])
	}
}

// TestBlockStoreOutOfSpace tests handling of out-of-space conditions.
func TestBlockStoreOutOfSpace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping out-of-space test in short mode")
	}

	tmpDir := t.TempDir()
	devicePath := filepath.Join(tmpDir, "device.img")

	// Create a device just large enough for 2 chunks (including per-chunk headers).
	const deviceSize = dataOffset + 2*(ChunkSize+BlockSize) + BlockSize
	f, err := os.Create(devicePath)
	if err != nil {
		t.Fatalf("creating device file: %v", err)
	}
	if err := f.Truncate(int64(deviceSize)); err != nil {
		f.Close()
		t.Fatalf("truncating device file: %v", err)
	}
	f.Close()

	config := BlockStoreConfig{
		Devices:     []string{devicePath},
		MetadataDir: tmpDir,
		AutoSync:    true,
	}

	store, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Fill the store with chunks.
	for i := 0; i < 2; i++ {
		data := make([]byte, ChunkSize)
		for j := range data {
			data[j] = byte(i + 1) // Use non-zero values
		}
		chunks := SplitData(data)
		if err := store.Put(ctx, chunks[0]); err != nil {
			t.Fatalf("Put failed for chunk %d: %v", i, err)
		}
	}

	// Try to add one more - should fail.
	data := make([]byte, ChunkSize)
	for j := range data {
		data[j] = 0xFF // Unique pattern different from previous chunks
	}
	chunks := SplitData(data)
	err = store.Put(ctx, chunks[0])
	if err != ErrOutOfSpace {
		t.Errorf("expected ErrOutOfSpace, got %v", err)
	}
}

// TestCreateBlockStoreBackend tests creating a block store via the factory.
func TestCreateBlockStoreBackend(t *testing.T) {
	// Adaptive sizing: use smaller device in short mode
	deviceSize := 20 * 1024 * 1024 // 20MB for short mode
	if !testing.Short() {
		deviceSize = 100 * 1024 * 1024 // 100MB for full mode
	}

	tmpDir := t.TempDir()
	devicePath := filepath.Join(tmpDir, "device.img")
	f, err := os.Create(devicePath)
	if err != nil {
		t.Fatalf("creating device file: %v", err)
	}
	if err := f.Truncate(int64(deviceSize)); err != nil {
		f.Close()
		t.Fatalf("truncating device file: %v", err)
	}
	f.Close()

	// Create via factory.
	config := map[string]string{
		"devices":      devicePath,
		"metadata_dir": tmpDir,
		"auto_sync":    "true",
	}

	store, err := CreateBackend("block", config)
	if err != nil {
		t.Fatalf("CreateBackend failed: %v", err)
	}
	// Close the store if it implements io.Closer.
	if bs, ok := store.(*BlockStore); ok {
		defer bs.Close()
	}

	ctx := context.Background()

	// Test basic operation.
	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	chunks := SplitData(data)
	c := chunks[0]

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(retrieved.Data) != len(data) {
		t.Errorf("data size mismatch")
	}
}

// TestBlockStoreDeduplication tests automatic deduplication.
func TestBlockStoreDeduplication(t *testing.T) {
	// Adaptive sizing: use smaller device in short mode
	deviceSize := 20 * 1024 * 1024 // 20MB for short mode
	if !testing.Short() {
		deviceSize = 100 * 1024 * 1024 // 100MB for full mode
	}

	tmpDir := t.TempDir()
	devicePath := filepath.Join(tmpDir, "device.img")
	f, err := os.Create(devicePath)
	if err != nil {
		t.Fatalf("creating device file: %v", err)
	}
	if err := f.Truncate(int64(deviceSize)); err != nil {
		f.Close()
		t.Fatalf("truncating device file: %v", err)
	}
	f.Close()

	config := BlockStoreConfig{
		Devices:     []string{devicePath},
		MetadataDir: tmpDir,
		AutoSync:    true,
	}

	store, err := NewBlockStore(config)
	if err != nil {
		t.Fatalf("NewBlockStore failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create two chunks with identical data.
	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = 0x42
	}

	c1 := &Chunk{ID: NewChunkID(data), Data: data}
	c1.Checksum = c1.ComputeChecksum()

	c2 := &Chunk{ID: NewChunkID(data), Data: data} // Same data, same ID
	c2.Checksum = c2.ComputeChecksum()

	// Put both - second should be deduplicated.
	if err := store.Put(ctx, c1); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	if err := store.Put(ctx, c2); err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	// List should only show one chunk.
	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 chunk (deduplication), got %d", len(ids))
	}

	// Stats should show only one chunk stored.
	stats := store.Stats()
	if chunkCount, ok := stats["chunks"].(int); !ok || chunkCount != 1 {
		t.Errorf("expected 1 chunk in stats, got %v", stats["chunks"])
	}
}
