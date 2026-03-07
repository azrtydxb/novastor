// Package agent provides storage node agent functionality for NovaStor.
// This package includes the chunk block device management that assembles
// block devices from distributed chunks for NVMe-oF exports.
package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/azrtydxb/novastor/internal/chunk"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
)

const (
	// chunkBlockSize is the size of each chunk (4MB).
	chunkBlockSize = 4 * 1024 * 1024

	// blockDeviceDir is where block device files assembled from chunks are stored.
	blockDeviceDir = "/var/lib/novastor/blockdevices"
)

// ChunkBlockDevice manages a block device assembled from chunks.
type ChunkBlockDevice struct {
	volumeID   string
	chunkStore chunk.Store
	metaClient *metadata.GRPCClient
	devicePath string
	loopDevice string
	mu         sync.Mutex
}

// NewChunkBlockDevice creates a new ChunkBlockDevice for the given volume.
func NewChunkBlockDevice(volumeID string, chunkStore chunk.Store, metaClient *metadata.GRPCClient) *ChunkBlockDevice {
	return &ChunkBlockDevice{
		volumeID:   volumeID,
		chunkStore: chunkStore,
		metaClient: metaClient,
	}
}

// Assemble assembles chunks into a block device file and attaches it as a loop device.
func (cbd *ChunkBlockDevice) Assemble(ctx context.Context) (string, error) {
	cbd.mu.Lock()
	defer cbd.mu.Unlock()

	// If already assembled, return existing loop device.
	if cbd.loopDevice != "" {
		return cbd.loopDevice, nil
	}

	// Ensure block device directory exists.
	if err := os.MkdirAll(blockDeviceDir, 0o750); err != nil {
		return "", fmt.Errorf("creating block device directory: %w", err)
	}

	devicePath := filepath.Join(blockDeviceDir, cbd.volumeID)

	// Get volume metadata to find chunk list.
	volMeta, err := cbd.metaClient.GetVolumeMeta(ctx, cbd.volumeID)
	if err != nil {
		return "", fmt.Errorf("getting volume metadata: %w", err)
	}

	if len(volMeta.ChunkIDs) == 0 {
		return "", fmt.Errorf("volume %s has no chunks", cbd.volumeID)
	}

	logging.L.Info("chunk block device: assembling volume",
		zap.String("volumeID", cbd.volumeID),
		zap.Int("numChunks", len(volMeta.ChunkIDs)),
	)

	// Create or open the block device file.
	devFile, err := os.OpenFile(devicePath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return "", fmt.Errorf("opening device file: %w", err)
	}

	// Calculate expected size and pre-allocate.
	expectedSize := int64(len(volMeta.ChunkIDs)) * chunkBlockSize
	if err := devFile.Truncate(expectedSize); err != nil {
		_ = devFile.Close()
		return "", fmt.Errorf("pre-allocating device file: %w", err)
	}

	// Assemble chunks in parallel.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(8) // Limit parallel chunk fetches

	for i, chunkID := range volMeta.ChunkIDs {
		chunkIndex, cid := i, chunkID // Capture loop variables
		offset := int64(chunkIndex) * chunkBlockSize

		g.Go(func() error {
			return cbd.writeChunk(gCtx, cid, devFile, offset)
		})
	}

	if err := g.Wait(); err != nil {
		_ = devFile.Close()
		return "", fmt.Errorf("assembling chunks: %w", err)
	}

	if err := devFile.Sync(); err != nil {
		_ = devFile.Close()
		return "", fmt.Errorf("syncing device file: %w", err)
	}

	if err := devFile.Close(); err != nil {
		return "", fmt.Errorf("closing device file: %w", err)
	}

	cbd.devicePath = devicePath

	// Attach loop device.
	loopDev, err := attachLoopDevice(ctx, devicePath)
	if err != nil {
		return "", fmt.Errorf("attaching loop device: %w", err)
	}

	cbd.loopDevice = loopDev

	logging.L.Info("chunk block device: assembled",
		zap.String("volumeID", cbd.volumeID),
		zap.String("devicePath", devicePath),
		zap.String("loopDevice", loopDev),
	)

	return loopDev, nil
}

// writeChunk writes a single chunk to the device file at the given offset.
func (cbd *ChunkBlockDevice) writeChunk(ctx context.Context, chunkID string, devFile *os.File, offset int64) error {
	// Fetch chunk from local store.
	c, err := cbd.chunkStore.Get(ctx, chunk.ChunkID(chunkID))
	if err != nil {
		return fmt.Errorf("getting chunk %s: %w", chunkID, err)
	}

	// Write chunk at the correct offset.
	if _, err := devFile.WriteAt(c.Data, offset); err != nil {
		return fmt.Errorf("writing chunk %s at offset %d: %w", chunkID, offset, err)
	}

	return nil
}

// Disassemble detaches the loop device and removes the block device file.
func (cbd *ChunkBlockDevice) Disassemble(ctx context.Context) error {
	cbd.mu.Lock()
	defer cbd.mu.Unlock()

	var errs []error

	// Detach loop device if attached.
	if cbd.loopDevice != "" {
		if err := detachLoopDevice(ctx, cbd.loopDevice); err != nil {
			errs = append(errs, fmt.Errorf("detaching loop device: %w", err))
		}
		cbd.loopDevice = ""
	}

	// Remove device file.
	if cbd.devicePath != "" {
		if err := os.Remove(cbd.devicePath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("removing device file: %w", err))
		}
		cbd.devicePath = ""
	}

	if len(errs) > 0 {
		return errs[0]
	}

	logging.L.Info("chunk block device: disassembled", zap.String("volumeID", cbd.volumeID))
	return nil
}

// GetLoopDevice returns the loop device path (empty if not assembled).
func (cbd *ChunkBlockDevice) GetLoopDevice() string {
	cbd.mu.Lock()
	defer cbd.mu.Unlock()
	return cbd.loopDevice
}

// Verify checks that all chunks for the volume exist in the local store.
func (cbd *ChunkBlockDevice) Verify(ctx context.Context) error {
	volMeta, err := cbd.metaClient.GetVolumeMeta(ctx, cbd.volumeID)
	if err != nil {
		return fmt.Errorf("getting volume metadata: %w", err)
	}

	for _, chunkID := range volMeta.ChunkIDs {
		exists, err := cbd.chunkStore.Has(ctx, chunk.ChunkID(chunkID))
		if err != nil {
			return fmt.Errorf("checking chunk %s: %w", chunkID, err)
		}
		if !exists {
			return fmt.Errorf("chunk %s not found in local store", chunkID)
		}
	}

	return nil
}

// EnsureChunks creates any missing chunks as empty (zeroed) blocks.
// This is used during volume provisioning when the volume has just been
// created and chunks don't exist yet.
func (cbd *ChunkBlockDevice) EnsureChunks(ctx context.Context) error {
	// Retry GetVolumeMeta with back-off because the volume metadata may
	// have just been written to the Raft leader and followers haven't
	// applied the log entry to their FSM yet. With round-robin load
	// balancing a read can land on a stale follower, returning NotFound.
	var volMeta *metadata.VolumeMeta
	for attempt := 0; attempt < 10; attempt++ {
		var err error
		volMeta, err = cbd.metaClient.GetVolumeMeta(ctx, cbd.volumeID)
		if err == nil {
			break
		}
		if status.Code(err) != codes.NotFound || attempt == 9 {
			return fmt.Errorf("getting volume metadata: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	emptyBlock := make([]byte, chunkBlockSize)
	created := 0

	for _, chunkID := range volMeta.ChunkIDs {
		exists, err := cbd.chunkStore.Has(ctx, chunk.ChunkID(chunkID))
		if err != nil {
			return fmt.Errorf("checking chunk %s: %w", chunkID, err)
		}
		if !exists {
			c := &chunk.Chunk{
				ID:   chunk.ChunkID(chunkID),
				Data: emptyBlock,
			}
			c.Checksum = c.ComputeChecksum()
			if err := cbd.chunkStore.Put(ctx, c); err != nil {
				return fmt.Errorf("creating empty chunk %s: %w", chunkID, err)
			}
			created++
		}
	}

	if created > 0 {
		logging.L.Info("chunk block device: created empty chunks for new volume",
			zap.String("volumeID", cbd.volumeID),
			zap.Int("created", created),
			zap.Int("total", len(volMeta.ChunkIDs)),
		)
	}

	return nil
}
