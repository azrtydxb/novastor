package chunk

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/piwi3910/novastor/internal/metrics"
	"go.uber.org/zap"
)

const (
	// BlockBackendMagic is the magic number for block backend metadata.
	BlockBackendMagic = 0x42535452 // "BSTR" in little-endian

	// BlockBackendVersion is the current metadata format version.
	BlockBackendVersion = 1

	// metadataOffset is where the metadata starts on the device (first 4KB).
	metadataOffset = 0

	// dataOffset is where chunk data starts (second 4KB, after metadata).
	dataOffset = BlockSize
)

// BlockStoreConfig holds configuration for the block store backend.
type BlockStoreConfig struct {
	// Devices is a list of block device paths to pool together.
	Devices []string

	// MetadataDir is the directory for storing metadata files.
	MetadataDir string

	// AutoSync enables automatic metadata sync after each operation.
	AutoSync bool

	// Logger is the logger to use.
	Logger *zap.Logger
}

// BlockStoreDevice represents a single block device in the pool.
type BlockStoreDevice struct {
	path   string
	file   *os.File
	bitmap *Bitmap
	size   int64
	mu     sync.Mutex
}

// BlockStoreMetadata is persisted on disk for recovery.
type BlockStoreMetadata struct {
	Magic       uint32
	Version     uint32
	DeviceCount uint32
	Reserved    [12]byte
}

// ChunkIndexEntry maps a chunk ID to its location.
type ChunkIndexEntry struct {
	DeviceIndex uint16
	BlockOffset uint64
	BlockCount  uint16 // Always BlocksPerChunk for now
	DataLen     uint32 // Actual data length (for variable-sized chunks)
	Checksum    uint32
	Reserved    uint32 // For future use
}

// BlockStore implements the Store interface using raw block devices.
// This is inspired by BlueStore's design, using a bitmap allocator
// for free space management.
type BlockStore struct {
	mu       sync.RWMutex
	config   BlockStoreConfig
	devices  []*BlockStoreDevice
	index    map[ChunkID]ChunkIndexEntry
	indexMu  sync.RWMutex
	metadata string
	closed   bool
}

// NewBlockStore creates a new block store backend.
func NewBlockStore(config BlockStoreConfig) (*BlockStore, error) {
	if len(config.Devices) == 0 {
		return nil, fmt.Errorf("block store requires at least one device")
	}

	if config.MetadataDir == "" {
		config.MetadataDir = "/var/lib/novastor/blockstore"
	}

	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}

	// Ensure metadata directory exists.
	if err := os.MkdirAll(config.MetadataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating metadata directory: %w", err)
	}

	bs := &BlockStore{
		config:   config,
		devices:  make([]*BlockStoreDevice, 0, len(config.Devices)),
		index:    make(map[ChunkID]ChunkIndexEntry),
		metadata: filepath.Join(config.MetadataDir, "chunk_index.bin"),
	}

	// Initialize devices.
	for i, devicePath := range config.Devices {
		if err := bs.initDevice(i, devicePath); err != nil {
			// Close any opened devices on failure.
			bs.Close()
			return nil, fmt.Errorf("initializing device %s: %w", devicePath, err)
		}
	}

	// Load or create index.
	if err := bs.loadIndex(); err != nil {
		config.Logger.Warn("failed to load chunk index, starting fresh",
			zap.Error(err))
		// Recover by scanning devices.
		if err := bs.recoverIndex(); err != nil {
			config.Logger.Warn("failed to recover index", zap.Error(err))
		}
	}

	config.Logger.Info("block store initialized",
		zap.Int("devices", len(bs.devices)),
		zap.String("metadataDir", config.MetadataDir),
	)

	return bs, nil
}

// initDevice initializes a single block device.
func (bs *BlockStore) initDevice(index int, path string) error {
	// Open device for read/write.
	// Use O_SYNC for writes to ensure data reaches disk.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		return fmt.Errorf("opening device: %w", err)
	}

	// Get device size.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("getting device info: %w", err)
	}

	size := info.Size()
	if size == 0 {
		f.Close()
		return fmt.Errorf("device has zero size")
	}

	// Calculate usable space (subtract metadata area).
	usableSize := size - dataOffset
	totalBlocks := uint64(usableSize) / BlockSize

	// Load or create bitmap.
	bitmapPath := filepath.Join(bs.config.MetadataDir, fmt.Sprintf("bitmap_%d.bin", index))
	bitmap, err := OpenBitmap(bitmapPath, totalBlocks)
	if err != nil {
		bs.config.Logger.Warn("creating new bitmap",
			zap.Int("device", index),
			zap.String("path", path),
			zap.Uint64("totalBlocks", totalBlocks),
		)
		bitmap = NewBitmap(totalBlocks)
		bitmap.SetPath(bitmapPath)
	}

	dev := &BlockStoreDevice{
		path:   path,
		file:   f,
		bitmap: bitmap,
		size:   size,
	}

	bs.devices = append(bs.devices, dev)

	bs.config.Logger.Info("device initialized",
		zap.Int("index", index),
		zap.String("path", path),
		zap.Int64("sizeBytes", size),
		zap.Uint64("totalBlocks", totalBlocks),
		zap.Uint64("freeBlocks", bitmap.FreeBlocks()),
	)

	return nil
}

// loadIndex loads the chunk index from disk.
func (bs *BlockStore) loadIndex() error {
	bs.indexMu.Lock()
	defer bs.indexMu.Unlock()

	f, err := os.Open(bs.metadata)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Not an error, first start
		}
		return err
	}
	defer f.Close()

	var header struct {
		Magic    uint32
		Version  uint32
		Count    uint32
		Reserved [20]byte
	}

	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		return err
	}

	if header.Magic != BlockBackendMagic {
		return fmt.Errorf("invalid index magic: 0x%x", header.Magic)
	}
	if header.Version != BlockBackendVersion {
		return fmt.Errorf("unsupported index version: %d", header.Version)
	}

	bs.index = make(map[ChunkID]ChunkIndexEntry)

	// Read chunk ID strings and entries.
	for i := uint32(0); i < header.Count; i++ {
		var idLen uint16
		if err := binary.Read(f, binary.LittleEndian, &idLen); err != nil {
			return err
		}

		idBytes := make([]byte, idLen)
		if _, err := f.Read(idBytes); err != nil {
			return err
		}

		var entry ChunkIndexEntry
		if err := binary.Read(f, binary.LittleEndian, &entry); err != nil {
			return err
		}

		bs.index[ChunkID(idBytes)] = entry
	}

	bs.config.Logger.Info("loaded chunk index", zap.Int("entries", len(bs.index)))
	return nil
}

// saveIndex saves the chunk index to disk.
func (bs *BlockStore) saveIndex() error {
	bs.indexMu.Lock()
	defer bs.indexMu.Unlock()

	if err := os.MkdirAll(filepath.Dir(bs.metadata), 0o755); err != nil {
		return err
	}

	tmpPath := bs.metadata + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	header := struct {
		Magic    uint32
		Version  uint32
		Count    uint32
		Reserved [20]byte
	}{
		Magic:   BlockBackendMagic,
		Version: BlockBackendVersion,
		Count:   uint32(len(bs.index)),
	}

	if err := binary.Write(f, binary.LittleEndian, header); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	for id, entry := range bs.index {
		idBytes := []byte(id)
		idLen := uint16(len(idBytes))

		if err := binary.Write(f, binary.LittleEndian, idLen); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		if _, err := f.Write(idBytes); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, bs.metadata); err != nil {
		return err
	}

	return nil
}

// recoverIndex scans devices to rebuild the index.
// This is used when the index file is missing or corrupted.
func (bs *BlockStore) recoverIndex() error {
	bs.indexMu.Lock()
	defer bs.indexMu.Unlock()

	bs.config.Logger.Info("recovering chunk index from devices")

	// For each device, scan the bitmap and try to read chunk headers.
	for devIdx, dev := range bs.devices {
		dev.mu.Lock()
		bitmap := dev.bitmap
		dev.mu.Unlock()

		// Scan for allocated chunks.
		for blockOffset := uint64(0); blockOffset < bitmap.TotalBlocks(); blockOffset += BlocksPerChunk {
			if bitmap.IsSet(blockOffset) {
				// Try to read the chunk to verify and get its ID.
				if err := bs.readChunkHeader(devIdx, blockOffset); err != nil {
					bs.config.Logger.Warn("failed to read chunk during recovery",
						zap.Int("device", devIdx),
						zap.Uint64("offset", blockOffset),
						zap.Error(err),
					)
					// Clear the bitmap entry if the chunk is invalid.
					_ = bitmap.Free(blockOffset, BlocksPerChunk)
				}
			}
		}

		// Sync the recovered bitmap.
		_ = bitmap.Sync()
	}

	bs.config.Logger.Info("index recovery complete", zap.Int("entries", len(bs.index)))
	return nil
}

// readChunkHeader reads and verifies a chunk's header, updating the index.
func (bs *BlockStore) readChunkHeader(devIdx int, blockOffset uint64) error {
	if devIdx >= len(bs.devices) {
		return fmt.Errorf("invalid device index: %d", devIdx)
	}

	dev := bs.devices[devIdx]
	dev.mu.Lock()
	defer dev.mu.Unlock()

	// Calculate file offset.
	offset := dataOffset + int64(blockOffset*BlockSize)

	// Read chunk header: 4 bytes data length + 4 bytes checksum.
	headerBuf := make([]byte, 8)
	n, err := dev.file.ReadAt(headerBuf, offset)
	if err != nil {
		return err
	}
	if n != 8 {
		return fmt.Errorf("short header read: %d bytes", n)
	}

	dataLen := binary.BigEndian.Uint32(headerBuf[:4])
	checksum := binary.BigEndian.Uint32(headerBuf[4:8])

	// Sanity check data length.
	if dataLen > ChunkSize {
		return fmt.Errorf("invalid data length: %d", dataLen)
	}

	// Read the chunk data to compute ID.
	dataBuf := make([]byte, dataLen)
	n, err = dev.file.ReadAt(dataBuf, offset+8)
	if err != nil {
		return err
	}
	if uint32(n) != dataLen {
		return fmt.Errorf("short data read: %d < %d", n, dataLen)
	}

	// Compute chunk ID.
	chunkID := NewChunkID(dataBuf)

	// Verify checksum.
	table := crc32.MakeTable(crc32.Castagnoli)
	computedChecksum := crc32.Checksum(dataBuf, table)
	if computedChecksum != checksum {
		return fmt.Errorf("checksum mismatch")
	}

	// Update index.
	entry := ChunkIndexEntry{
		DeviceIndex: uint16(devIdx),
		BlockOffset: blockOffset,
		BlockCount:  BlocksPerChunk,
		DataLen:     dataLen,
		Checksum:    checksum,
	}
	bs.index[chunkID] = entry

	return nil
}

// Put stores a chunk on the block device.
func (bs *BlockStore) Put(_ context.Context, c *Chunk) error {
	if len(c.Data) > ChunkSize {
		return fmt.Errorf("chunk data exceeds ChunkSize: %d > %d", len(c.Data), ChunkSize)
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("block store is closed")
	}

	// Check if chunk already exists.
	bs.indexMu.RLock()
	_, exists := bs.index[c.ID]
	bs.indexMu.RUnlock()

	if exists {
		// Chunk already stored, deduplication is automatic.
		return nil
	}

	// Find a device with enough free space.
	var targetDev *BlockStoreDevice
	var targetDevIdx int

	for i, dev := range bs.devices {
		dev.mu.Lock()
		freeBlocks := dev.bitmap.FreeBlocks()
		dev.mu.Unlock()

		if freeBlocks >= BlocksPerChunk {
			targetDev = dev
			targetDevIdx = i
			break
		}
	}

	if targetDev == nil {
		return ErrOutOfSpace
	}

	// Allocate blocks.
	targetDev.mu.Lock()
	blockOffset, err := targetDev.bitmap.Alloc(BlocksPerChunk)
	targetDev.mu.Unlock()

	if err != nil {
		return fmt.Errorf("allocating blocks: %w", err)
	}

	// Prepare chunk data with length and checksum header.
	// Format: [4 bytes data length][4 bytes checksum][N bytes data]
	buf := make([]byte, 8+len(c.Data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(c.Data)))
	binary.BigEndian.PutUint32(buf[4:8], c.Checksum)
	copy(buf[8:], c.Data)

	// Write to device.
	offset := dataOffset + int64(blockOffset*BlockSize)
	targetDev.mu.Lock()
	n, err := targetDev.file.WriteAt(buf, offset)
	if err == nil && bs.config.AutoSync {
		_ = targetDev.file.Sync()
	}
	targetDev.mu.Unlock()

	if err != nil {
		// Free the allocated blocks on error.
		targetDev.mu.Lock()
		_ = targetDev.bitmap.Free(blockOffset, BlocksPerChunk)
		targetDev.mu.Unlock()
		return fmt.Errorf("writing chunk: %w", err)
	}

	if n != len(buf) {
		targetDev.mu.Lock()
		_ = targetDev.bitmap.Free(blockOffset, BlocksPerChunk)
		targetDev.mu.Unlock()
		return fmt.Errorf("short write: %d < %d", n, len(buf))
	}

	// Update index.
	entry := ChunkIndexEntry{
		DeviceIndex: uint16(targetDevIdx),
		BlockOffset: blockOffset,
		BlockCount:  BlocksPerChunk,
		DataLen:     uint32(len(c.Data)),
		Checksum:    c.Checksum,
	}

	bs.indexMu.Lock()
	bs.index[c.ID] = entry
	bs.indexMu.Unlock()

	// Persist metadata if auto-sync is enabled.
	if bs.config.AutoSync {
		_ = bs.saveIndex()
		targetDev.mu.Lock()
		_ = targetDev.bitmap.Sync()
		targetDev.mu.Unlock()
	}

	metrics.ChunkOpsTotal.WithLabelValues("write").Inc()
	metrics.ChunkBytesTotal.WithLabelValues("write").Add(float64(len(c.Data)))

	return nil
}

// Get retrieves a chunk from the block device.
func (bs *BlockStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return nil, fmt.Errorf("block store is closed")
	}

	// Find chunk in index.
	bs.indexMu.RLock()
	entry, exists := bs.index[id]
	bs.indexMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("chunk %s not found", id)
	}

	if int(entry.DeviceIndex) >= len(bs.devices) {
		return nil, fmt.Errorf("invalid device index: %d", entry.DeviceIndex)
	}

	dev := bs.devices[entry.DeviceIndex]

	// Read chunk header (8 bytes: 4 data length + 4 checksum).
	headerBuf := make([]byte, 8)
	offset := dataOffset + int64(entry.BlockOffset*BlockSize)

	dev.mu.Lock()
	_, err := dev.file.ReadAt(headerBuf, offset)
	dev.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("reading chunk header: %w", err)
	}

	storedDataLen := binary.BigEndian.Uint32(headerBuf[:4])
	storedChecksum := binary.BigEndian.Uint32(headerBuf[4:8])

	// Sanity check: data length should match index.
	if storedDataLen != entry.DataLen {
		return nil, fmt.Errorf("data length mismatch: index=%d stored=%d", entry.DataLen, storedDataLen)
	}

	// Read the data.
	dataBuf := make([]byte, storedDataLen)
	dev.mu.Lock()
	_, err = dev.file.ReadAt(dataBuf, offset+8)
	dev.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("reading chunk data: %w", err)
	}

	// Verify checksum against the stored checksum.
	table := crc32.MakeTable(crc32.Castagnoli)
	computedChecksum := crc32.Checksum(dataBuf, table)
	if computedChecksum != storedChecksum {
		return nil, fmt.Errorf("checksum mismatch for chunk %s: stored=%d computed=%d", id, storedChecksum, computedChecksum)
	}

	chunk := &Chunk{
		ID:       id,
		Data:     dataBuf,
		Checksum: storedChecksum,
	}

	metrics.ChunkOpsTotal.WithLabelValues("read").Inc()
	metrics.ChunkBytesTotal.WithLabelValues("read").Add(float64(len(dataBuf)))

	return chunk, nil
}

// Delete removes a chunk from the block device.
func (bs *BlockStore) Delete(_ context.Context, id ChunkID) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return fmt.Errorf("block store is closed")
	}

	// Find chunk in index.
	bs.indexMu.Lock()
	entry, exists := bs.index[id]
	if exists {
		delete(bs.index, id)
	}
	bs.indexMu.Unlock()

	if !exists {
		// Already deleted, that's fine.
		return nil
	}

	if int(entry.DeviceIndex) >= len(bs.devices) {
		return fmt.Errorf("invalid device index: %d", entry.DeviceIndex)
	}

	dev := bs.devices[entry.DeviceIndex]

	// Free the blocks.
	dev.mu.Lock()
	err := dev.bitmap.Free(entry.BlockOffset, uint64(entry.BlockCount))
	syncErr := dev.bitmap.Sync()
	dev.mu.Unlock()

	if err != nil {
		return fmt.Errorf("freeing blocks: %w", err)
	}
	if syncErr != nil {
		bs.config.Logger.Warn("failed to sync bitmap", zap.Error(syncErr))
	}

	// Persist metadata if auto-sync is enabled.
	if bs.config.AutoSync {
		_ = bs.saveIndex()
	}

	metrics.ChunkOpsTotal.WithLabelValues("delete").Inc()

	return nil
}

// Has checks if a chunk exists.
func (bs *BlockStore) Has(_ context.Context, id ChunkID) (bool, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return false, fmt.Errorf("block store is closed")
	}

	bs.indexMu.RLock()
	_, exists := bs.index[id]
	bs.indexMu.RUnlock()

	return exists, nil
}

// List returns all chunk IDs.
func (bs *BlockStore) List(_ context.Context) ([]ChunkID, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return nil, fmt.Errorf("block store is closed")
	}

	bs.indexMu.RLock()
	defer bs.indexMu.RUnlock()

	ids := make([]ChunkID, 0, len(bs.index))
	for id := range bs.index {
		ids = append(ids, id)
	}

	return ids, nil
}

// Close closes the block store and releases resources.
func (bs *BlockStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed {
		return nil
	}

	bs.closed = true

	// Save index before closing.
	if err := bs.saveIndex(); err != nil {
		bs.config.Logger.Error("failed to save chunk index", zap.Error(err))
	}

	// Sync and close all devices.
	for _, dev := range bs.devices {
		dev.mu.Lock()
		_ = dev.bitmap.Sync()
		if dev.file != nil {
			_ = dev.file.Close()
		}
		dev.mu.Unlock()
	}

	return nil
}

// Stats returns statistics about the block store.
func (bs *BlockStore) Stats() map[string]interface{} {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	stats := make(map[string]interface{})

	var totalBlocks, usedBlocks, freeBlocks uint64
	var totalBytes, usedBytes, freeBytes uint64

	for i, dev := range bs.devices {
		dev.mu.Lock()
		total := dev.bitmap.TotalBlocks()
		used := dev.bitmap.UsedBlocks()
		free := dev.bitmap.FreeBlocks()
		dev.mu.Unlock()

		totalBlocks += total
		usedBlocks += used
		freeBlocks += free

		totalBytes += total * BlockSize
		usedBytes += used * BlockSize
		freeBytes += free * BlockSize

		stats[fmt.Sprintf("device_%d_path", i)] = dev.path
		stats[fmt.Sprintf("device_%d_total_blocks", i)] = total
		stats[fmt.Sprintf("device_%d_used_blocks", i)] = used
		stats[fmt.Sprintf("device_%d_free_blocks", i)] = free
	}

	bs.indexMu.RLock()
	chunkCount := len(bs.index)
	bs.indexMu.RUnlock()

	stats["devices"] = len(bs.devices)
	stats["chunks"] = chunkCount
	stats["total_blocks"] = totalBlocks
	stats["used_blocks"] = usedBlocks
	stats["free_blocks"] = freeBlocks
	stats["total_bytes"] = totalBytes
	stats["used_bytes"] = usedBytes
	stats["free_bytes"] = freeBytes

	if totalBlocks > 0 {
		stats["usage_ratio"] = float64(usedBlocks) / float64(totalBlocks)
	}

	return stats
}

// Sync flushes all pending metadata to disk.
func (bs *BlockStore) Sync() error {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed {
		return fmt.Errorf("block store is closed")
	}

	if err := bs.saveIndex(); err != nil {
		return fmt.Errorf("saving index: %w", err)
	}

	for _, dev := range bs.devices {
		dev.mu.Lock()
		err := dev.bitmap.Sync()
		dev.mu.Unlock()

		if err != nil {
			return fmt.Errorf("syncing bitmap: %w", err)
		}
	}

	return nil
}
