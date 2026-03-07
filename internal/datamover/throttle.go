package datamover

import (
	"context"
	"sync"

	"github.com/azrtydxb/novastor/internal/chunk"
	"github.com/azrtydxb/novastor/internal/metadata"

	"golang.org/x/time/rate"
)

// ThrottledReplicator wraps a ShardReplicator with rate limiting.
// It enforces a bandwidth limit across all concurrent operations.
type ThrottledReplicator struct {
	inner          ShardReplicator
	limiter        *rate.Limiter
	mu             sync.Mutex
	bytesPerSecond int64
}

// NewThrottledReplicator creates a new throttled replicator.
func NewThrottledReplicator(inner ShardReplicator, bytesPerSecond int64) *ThrottledReplicator {
	limiter := rate.NewLimiter(rate.Limit(bytesPerSecond), int(bytesPerSecond))
	return &ThrottledReplicator{
		inner:          inner,
		limiter:        limiter,
		bytesPerSecond: bytesPerSecond,
	}
}

// SetBandwidth updates the bandwidth limit at runtime.
func (t *ThrottledReplicator) SetBandwidth(bytesPerSecond int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bytesPerSecond = bytesPerSecond
	t.limiter.SetLimit(rate.Limit(bytesPerSecond))
	t.limiter.SetBurst(int(bytesPerSecond))
}

// GetBandwidth returns the current bandwidth limit.
func (t *ThrottledReplicator) GetBandwidth() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bytesPerSecond
}

// waitForBytes waits until the rate limiter allows transferring the given number of bytes.
func (t *ThrottledReplicator) waitForBytes(ctx context.Context, bytes int64) error {
	// Wait until we can transfer the requested bytes
	err := t.limiter.WaitN(ctx, int(bytes))
	return err
}

// ReplicateChunk copies a whole chunk from source to destination with throttling.
func (t *ThrottledReplicator) ReplicateChunk(ctx context.Context, chunkID string, sourceNode, destNode string) error {
	// Estimate chunk size for throttling (default 4MB)
	// In production, this should be queried from metadata
	const estimatedChunkSize = 4 * 1024 * 1024
	if err := t.waitForBytes(ctx, estimatedChunkSize); err != nil {
		return err
	}
	return t.inner.ReplicateChunk(ctx, chunkID, sourceNode, destNode)
}

// ReplicateShard copies a specific EC shard from source to destination with throttling.
func (t *ThrottledReplicator) ReplicateShard(ctx context.Context, chunkID string, shardIndex int, sourceNode, destNode string) error {
	// Estimate shard size (4MB / total shards, defaulting to ~1MB)
	const estimatedShardSize = 1024 * 1024
	if err := t.waitForBytes(ctx, estimatedShardSize); err != nil {
		return err
	}
	return t.inner.ReplicateShard(ctx, chunkID, shardIndex, sourceNode, destNode)
}

// ReconstructShard reconstructs a missing EC shard with throttling on I/O.
func (t *ThrottledReplicator) ReconstructShard(ctx context.Context, chunkID string, shardIndex int, sourcePlacements []*metadata.ShardPlacement, destNode string, ec *chunk.ErasureCoder) error {
	// Throttle for reading all source shards and writing the reconstructed one
	const estimatedShardSize = 1024 * 1024
	// We need to read dataShards worth of data and write 1 shard
	dataShards := ec.DataShards()
	totalBytes := int64(dataShards+1) * estimatedShardSize
	if err := t.waitForBytes(ctx, totalBytes); err != nil {
		return err
	}
	return t.inner.ReconstructShard(ctx, chunkID, shardIndex, sourcePlacements, destNode, ec)
}

// DeleteChunk removes a chunk from a node (no throttling needed for deletes).
func (t *ThrottledReplicator) DeleteChunk(ctx context.Context, chunkID string, node string) error {
	return t.inner.DeleteChunk(ctx, chunkID, node)
}
