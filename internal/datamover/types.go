package datamover

import (
	"context"
	"time"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/metadata"
)

// DataMover coordinates chunk healing operations across the cluster.
// It manages a persistent queue of healing tasks and a pool of workers
// that execute them with throttling and deduplication.
type DataMover interface {
	// Submit adds a task to the execution queue (idempotent: same taskID → no-op)
	Submit(ctx context.Context, task *metadata.HealTask) error

	// Status returns current progress for a task
	Status(ctx context.Context, taskID string) (*metadata.HealTask, error)

	// Cancel attempts to stop a task (best-effort)
	Cancel(ctx context.Context, taskID string) error

	// Start begins the worker pool; blocks until ctx is cancelled
	Start(ctx context.Context) error
}

// Config holds configuration for the DataMover.
type Config struct {
	// WorkerCount is the number of concurrent healing workers
	WorkerCount int

	// BandwidthBytesPerSecond is the total I/O budget across all workers
	BandwidthBytesPerSecond int64

	// RetryMaxAttempts is the maximum number of retry attempts for failed tasks
	RetryMaxAttempts int

	// RetryBackoffBase is the base duration for exponential backoff
	RetryBackoffBase time.Duration

	// HeartbeatInterval is how often workers refresh their locks
	HeartbeatInterval time.Duration

	// LockTTL is the time-to-live for chunk heal locks
	LockTTL time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		WorkerCount:             4,
		BandwidthBytesPerSecond: 100 * 1024 * 1024, // 100 MB/s
		RetryMaxAttempts:        5,
		RetryBackoffBase:        10 * time.Second,
		HeartbeatInterval:       30 * time.Second,
		LockTTL:                 5 * time.Minute,
	}
}

// ShardReplicator handles copying chunks or shards between nodes.
type ShardReplicator interface {
	// ReplicateChunk copies a whole chunk from source to destination
	ReplicateChunk(ctx context.Context, chunkID string, sourceNode, destNode string) error

	// ReplicateShard copies a specific EC shard from source to destination
	ReplicateShard(ctx context.Context, chunkID string, shardIndex int, sourceNode, destNode string) error

	// ReconstructShard reads available EC shards, reconstructs the missing shard,
	// and writes it to the destination
	ReconstructShard(ctx context.Context, chunkID string, shardIndex int, sourcePlacements []*metadata.ShardPlacement, destNode string, ec *chunk.ErasureCoder) error

	// DeleteChunk removes a chunk from a node
	DeleteChunk(ctx context.Context, chunkID string, node string) error
}

// ChunkLocker provides distributed locking for chunk healing operations.
type ChunkLocker interface {
	// Acquire atomically writes a lock entry; returns error if held by a live owner
	Acquire(ctx context.Context, lockID, ownerTaskID string) error

	// Heartbeat extends the lock TTL
	Heartbeat(ctx context.Context, lockID, ownerTaskID string) error

	// Release removes the lock entry
	Release(ctx context.Context, lockID, ownerTaskID string) error
}

// MetadataStore defines the metadata operations needed by the DataMover.
type MetadataStore interface {
	// Heal task operations
	PutHealTask(ctx context.Context, task *metadata.HealTask) error
	GetHealTask(ctx context.Context, taskID string) (*metadata.HealTask, error)
	ListPendingHealTasks(ctx context.Context) ([]*metadata.HealTask, error)
	ListHealTasksByVolume(ctx context.Context, volumeID string) ([]*metadata.HealTask, error)
	DeleteHealTask(ctx context.Context, taskID string) error

	// Shard placement operations
	PutShardPlacement(ctx context.Context, sp *metadata.ShardPlacement) error
	GetShardPlacements(ctx context.Context, chunkID string) ([]*metadata.ShardPlacement, error)
	DeleteShardPlacement(ctx context.Context, chunkID string, shardIndex int) error

	// Lock operations
	AcquireChunkLock(ctx context.Context, lockID, ownerTaskID string) error
	HeartbeatChunkLock(ctx context.Context, lockID, ownerTaskID string) error
	ReleaseChunkLock(ctx context.Context, lockID, ownerTaskID string) error
}

// ErasureCoderFactory creates erasure coders for given parameters.
type ErasureCoderFactory interface {
	GetErasureCoder(dataShards, parityShards int) (*chunk.ErasureCoder, error)
}
