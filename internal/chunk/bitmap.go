package chunk

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// BlockSize is the allocation block size (4KB).
	// This is the standard block size for most storage systems.
	BlockSize = 4 * 1024

	// BlocksPerChunk is the number of 4KB blocks in a 4MB chunk.
	BlocksPerChunk = ChunkSize / BlockSize // 1024 blocks

	// bitmapMagic is the magic number for bitmap metadata.
	bitmapMagic = 0x42554954 // "BLUE" in little-endian

	// bitmapVersion is the current metadata format version.
	bitmapVersion = 1
)

var (
	// ErrOutOfSpace is returned when no free blocks are available.
	ErrOutOfSpace = fmt.Errorf("no free space available")
)

// Bitmap manages free space using a bitmap allocator.
// Each bit represents one 4KB block. A set bit means the block is in use.
type Bitmap struct {
	mu          sync.RWMutex
	totalBlocks uint64
	usedBlocks  uint64
	data        []byte
	dirty       bool
	filePath    string
	autoSync    bool
}

// BitmapHeader represents the on-disk metadata header.
type BitmapHeader struct {
	Magic     uint32
	Version   uint32
	TotalBlocks uint64
	UsedBlocks  uint64
}

// NewBitmap creates a new bitmap allocator with the specified number of blocks.
func NewBitmap(totalBlocks uint64) *Bitmap {
	bitmapBytes := (totalBlocks + 7) / 8
	return &Bitmap{
		totalBlocks: totalBlocks,
		usedBlocks:  0,
		data:        make([]byte, bitmapBytes),
		dirty:       true,
		autoSync:    false,
	}
}

// OpenBitmap loads an existing bitmap from disk.
func OpenBitmap(path string, totalBlocks uint64) (*Bitmap, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Create a new bitmap if it doesn't exist.
			bm := NewBitmap(totalBlocks)
			bm.filePath = path
			if err := bm.Sync(); err != nil {
				return nil, fmt.Errorf("creating new bitmap: %w", err)
			}
			return bm, nil
		}
		return nil, fmt.Errorf("opening bitmap: %w", err)
	}
	defer f.Close()

	// Read header.
	var header BitmapHeader
	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("reading bitmap header: %w", err)
	}

	if header.Magic != bitmapMagic {
		return nil, fmt.Errorf("invalid bitmap magic: 0x%x", header.Magic)
	}
	if header.Version != bitmapVersion {
		return nil, fmt.Errorf("unsupported bitmap version: %d", header.Version)
	}

	// Validate total blocks matches.
	if header.TotalBlocks != totalBlocks {
		return nil, fmt.Errorf("block count mismatch: expected %d, got %d", totalBlocks, header.TotalBlocks)
	}

	bitmapBytes := (totalBlocks + 7) / 8
	data := make([]byte, bitmapBytes)
	if _, err := f.Read(data); err != nil {
		return nil, fmt.Errorf("reading bitmap data: %w", err)
	}

	return &Bitmap{
		totalBlocks: totalBlocks,
		usedBlocks:  header.UsedBlocks,
		data:        data,
		filePath:    path,
		dirty:       false,
		autoSync:    false,
	}, nil
}

// SetAutoSync enables automatic sync after each modification.
func (b *Bitmap) SetAutoSync(enabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.autoSync = enabled
}

// Sync writes the bitmap to disk if dirty.
func (b *Bitmap) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.dirty && b.filePath != "" {
		// Check if file exists to determine if we need to write.
		if _, err := os.Stat(b.filePath); err == nil {
			return nil
		}
	}

	if b.filePath == "" {
		return fmt.Errorf("no file path set for bitmap")
	}

	// Ensure directory exists.
	if err := os.MkdirAll(filepath.Dir(b.filePath), 0o755); err != nil {
		return fmt.Errorf("creating bitmap directory: %w", err)
	}

	f, err := os.Create(b.filePath)
	if err != nil {
		return fmt.Errorf("creating bitmap file: %w", err)
	}
	defer f.Close()

	header := BitmapHeader{
		Magic:      bitmapMagic,
		Version:    bitmapVersion,
		TotalBlocks: b.totalBlocks,
		UsedBlocks:  b.usedBlocks,
	}

	if err := binary.Write(f, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("writing bitmap header: %w", err)
	}

	if _, err := f.Write(b.data); err != nil {
		return fmt.Errorf("writing bitmap data: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing bitmap file: %w", err)
	}

	b.dirty = false
	return nil
}

// SetPath sets the file path for persistence.
func (b *Bitmap) SetPath(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.filePath = path
	b.dirty = true
}

// TotalBlocks returns the total number of blocks.
func (b *Bitmap) TotalBlocks() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.totalBlocks
}

// UsedBlocks returns the number of blocks in use.
func (b *Bitmap) UsedBlocks() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.usedBlocks
}

// FreeBlocks returns the number of free blocks.
func (b *Bitmap) FreeBlocks() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.totalBlocks - b.usedBlocks
}

// IsSet returns true if the block at the given index is marked as used.
func (b *Bitmap) IsSet(blockIndex uint64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if blockIndex >= b.totalBlocks {
		return false
	}

	byteIndex := blockIndex / 8
	bitIndex := blockIndex % 8
	return (b.data[byteIndex] & (1 << bitIndex)) != 0
}

// Set marks a block as used.
func (b *Bitmap) Set(blockIndex uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blockIndex >= b.totalBlocks {
		return fmt.Errorf("block index %d out of range (total: %d)", blockIndex, b.totalBlocks)
	}

	byteIndex := blockIndex / 8
	bitIndex := blockIndex % 8
	mask := byte(1 << bitIndex)

	if b.data[byteIndex]&mask == 0 {
		b.data[byteIndex] |= mask
		b.usedBlocks++
		b.dirty = true
		if b.autoSync {
			_ = b.syncUnsafe()
		}
	}

	return nil
}

// Clear marks a block as free.
func (b *Bitmap) Clear(blockIndex uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blockIndex >= b.totalBlocks {
		return fmt.Errorf("block index %d out of range (total: %d)", blockIndex, b.totalBlocks)
	}

	byteIndex := blockIndex / 8
	bitIndex := blockIndex % 8
	mask := byte(1 << bitIndex)

	if b.data[byteIndex]&mask != 0 {
		b.data[byteIndex] &^= mask
		b.usedBlocks--
		b.dirty = true
		if b.autoSync {
			_ = b.syncUnsafe()
		}
	}

	return nil
}

// sync syncs the bitmap without holding the lock (internal use).
func (b *Bitmap) syncUnsafe() error {
	if b.filePath == "" {
		return nil
	}

	// Store the dirty flag before releasing lock.
	dirty := b.dirty

	// Temporarily release the lock for I/O.
	b.mu.Unlock()
	defer b.mu.Lock()

	if !dirty {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(b.filePath), 0o755); err != nil {
		return err
	}

	f, err := os.Create(b.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	header := BitmapHeader{
		Magic:      bitmapMagic,
		Version:    bitmapVersion,
		TotalBlocks: b.totalBlocks,
		UsedBlocks:  b.usedBlocks,
	}

	if err := binary.Write(f, binary.LittleEndian, header); err != nil {
		return err
	}

	if _, err := f.Write(b.data); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	b.dirty = false
	return nil
}

// Alloc allocates the specified number of contiguous blocks.
// Returns the starting block index, or an error if not enough contiguous space is available.
func (b *Bitmap) Alloc(blocks uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blocks == 0 {
		return 0, fmt.Errorf("cannot allocate zero blocks")
	}

	if blocks > b.totalBlocks-b.usedBlocks {
		return 0, ErrOutOfSpace
	}

	// Find a contiguous run of free blocks.
	start, found := b.findContiguousLocked(blocks)
	if !found {
		return 0, ErrOutOfSpace
	}

	// Mark blocks as used.
	for i := uint64(0); i < blocks; i++ {
		byteIndex := (start + i) / 8
		bitIndex := (start + i) % 8
		b.data[byteIndex] |= 1 << bitIndex
	}

	b.usedBlocks += blocks
	b.dirty = true
	if b.autoSync {
		_ = b.syncUnsafe()
	}

	return start, nil
}

// findContiguousLocked finds a contiguous run of free blocks.
// Must be called with the lock held.
func (b *Bitmap) findContiguousLocked(blocks uint64) (uint64, bool) {
	if blocks == 0 {
		return 0, false
	}

	var start uint64
	var count uint64

	for i := uint64(0); i < b.totalBlocks; i++ {
		byteIndex := i / 8
		bitIndex := i % 8

		if b.data[byteIndex]&(1<<bitIndex) == 0 {
			// Block is free.
			if count == 0 {
				start = i
			}
			count++
			if count == blocks {
				return start, true
			}
		} else {
			// Block is in use, reset count.
			count = 0
		}
	}

	return 0, false
}

// Free releases the specified blocks starting at the given offset.
func (b *Bitmap) Free(offset uint64, blocks uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if offset+blocks > b.totalBlocks {
		return fmt.Errorf("free range out of bounds: offset=%d, blocks=%d, total=%d",
			offset, blocks, b.totalBlocks)
	}

	for i := uint64(0); i < blocks; i++ {
		byteIndex := (offset + i) / 8
		bitIndex := (offset + i) % 8
		mask := byte(1 << bitIndex)

		if b.data[byteIndex]&mask != 0 {
			b.data[byteIndex] &^= mask
			b.usedBlocks--
		}
	}

	b.dirty = true
	if b.autoSync {
		_ = b.syncUnsafe()
	}

	return nil
}

// FindFree finds a single free block and returns its index.
// Returns ErrOutOfSpace if no free blocks are available.
func (b *Bitmap) FindFree() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.usedBlocks >= b.totalBlocks {
		return 0, ErrOutOfSpace
	}

	for i := uint64(0); i < b.totalBlocks; i++ {
		byteIndex := i / 8
		bitIndex := i % 8

		if b.data[byteIndex]&(1<<bitIndex) == 0 {
			// Found a free block.
			b.data[byteIndex] |= 1 << bitIndex
			b.usedBlocks++
			b.dirty = true
			if b.autoSync {
				_ = b.syncUnsafe()
			}
			return i, nil
		}
	}

	return 0, ErrOutOfSpace
}

// FindContiguous searches for contiguous free blocks starting from the given offset.
// If wrap is true, the search wraps around to the beginning if not found.
// Returns the starting offset and whether it was found.
func (b *Bitmap) FindContiguous(blocks uint64, startOffset uint64, wrap bool) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blocks == 0 || blocks > b.totalBlocks {
		return 0, false
	}

	// Search from startOffset.
	for i := startOffset; i+blocks <= b.totalBlocks; i++ {
		if b.isRangeFreeLocked(i, blocks) {
			return i, true
		}
	}

	if wrap {
		// Search from beginning.
		for i := uint64(0); i+blocks <= startOffset; i++ {
			if b.isRangeFreeLocked(i, blocks) {
				return i, true
			}
		}
	}

	return 0, false
}

// isRangeFreeLocked checks if a range of blocks is free.
// Must be called with the lock held.
func (b *Bitmap) isRangeFreeLocked(offset uint64, blocks uint64) bool {
	if offset+blocks > b.totalBlocks {
		return false
	}

	for i := uint64(0); i < blocks; i++ {
		byteIndex := (offset + i) / 8
		bitIndex := (offset + i) % 8

		if b.data[byteIndex]&(1<<bitIndex) != 0 {
			return false
		}
	}

	return true
}

// IsRangeFree checks if the specified range of blocks is free.
func (b *Bitmap) IsRangeFree(offset uint64, blocks uint64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isRangeFreeLocked(offset, blocks)
}

// Usage returns the usage ratio (0.0 to 1.0).
func (b *Bitmap) Usage() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.totalBlocks == 0 {
		return 0
	}
	return float64(b.usedBlocks) / float64(b.totalBlocks)
}

// Fragmentation returns an estimate of fragmentation (0.0 to 1.0).
// Higher values indicate more fragmentation.
func (b *Bitmap) Fragmentation() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.usedBlocks == 0 || b.usedBlocks == b.totalBlocks {
		return 0
	}

	// Count free runs.
	freeRuns := 0
	inFreeRun := false
	maxFreeRun := uint64(0)
	currentFreeRun := uint64(0)

	for i := uint64(0); i < b.totalBlocks; i++ {
		byteIndex := i / 8
		bitIndex := i % 8

		if b.data[byteIndex]&(1<<bitIndex) == 0 {
			if !inFreeRun {
				freeRuns++
				inFreeRun = true
			}
			currentFreeRun++
		} else {
			if inFreeRun {
				if currentFreeRun > maxFreeRun {
					maxFreeRun = currentFreeRun
				}
				currentFreeRun = 0
			}
			inFreeRun = false
		}
	}

	if freeRuns == 0 {
		return 0
	}

	// Fragmentation is inversely related to the size of the largest free run
	// relative to total free space.
	totalFree := b.totalBlocks - b.usedBlocks
	if totalFree == 0 {
		return 0
	}

	// If the largest free run contains all free space, fragmentation is 0.
	// If it contains very little, fragmentation is high.
	return 1.0 - (float64(maxFreeRun) / float64(totalFree))
}
