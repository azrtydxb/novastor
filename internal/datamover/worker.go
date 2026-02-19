package datamover

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/metadata"
)

// worker processes healing tasks from the queue.
type worker struct {
	id              int
	replicator      ShardReplicator
	locker          ChunkLocker
	metaStore       MetadataStore
	ecFactory       ErasureCoderFactory
	heartbeatCancel context.CancelFunc
}

// newWorker creates a new worker with the given dependencies.
func newWorker(id int, replicator ShardReplicator, locker ChunkLocker, metaStore MetadataStore, ecFactory ErasureCoderFactory) *worker {
	return &worker{
		id:         id,
		replicator: replicator,
		locker:     locker,
		metaStore:  metaStore,
		ecFactory:  ecFactory,
	}
}

// processTask executes a single healing task.
func (w *worker) processTask(ctx context.Context, task *metadata.HealTask, cfg *Config) error {
	// Generate lock ID for this (chunkID, destNode) pair
	lockID := LockID(task.ChunkID, task.DestNode)

	// Calculate retry delay based on retry count
	retryDelay := time.Duration(1<<uint(task.RetryCount)) * cfg.RetryBackoffBase
	if task.RetryCount == 0 {
		retryDelay = 0
	}

	// Wait for backoff if this is a retry
	if retryDelay > 0 {
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Update task status to in-progress
	task.Status = "in-progress"
	task.UpdatedAt = time.Now().Unix()
	if err := w.metaStore.PutHealTask(ctx, task); err != nil {
		return fmt.Errorf("updating task status: %w", err)
	}

	// Acquire distributed lock
	if err := w.locker.Acquire(ctx, lockID, task.ID); err != nil {
		// Lock held by another task - requeue with delay
		task.Status = "pending"
		task.UpdatedAt = time.Now().Unix()
		_ = w.metaStore.PutHealTask(ctx, task)
		return fmt.Errorf("lock acquisition failed: %w", err)
	}

	// Start heartbeat goroutine
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	heartbeatDone := make(chan error, 1)
	w.heartbeatCancel = heartbeatCancel

	go func() {
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				heartbeatDone <- nil
				return
			case <-ticker.C:
				if err := w.locker.Heartbeat(ctx, lockID, task.ID); err != nil {
					heartbeatDone <- err
					return
				}
			}
		}
	}()

	// Ensure heartbeat is stopped and lock is released
	defer func() {
		heartbeatCancel()
		_ = <-heartbeatDone
		_ = w.locker.Release(context.Background(), lockID, task.ID)
	}()

	// Execute the task based on type
	var execErr error
	switch task.Type {
	case "replicate":
		execErr = w.executeReplicate(ctx, task)
	case "reconstruct":
		execErr = w.executeReconstruct(ctx, task)
	case "delete-excess":
		execErr = w.executeDeleteExcess(ctx, task)
	default:
		execErr = fmt.Errorf("unknown task type: %s", task.Type)
	}

	// Update task based on execution result
	task.UpdatedAt = time.Now().Unix()
	if execErr != nil {
		task.RetryCount++
		task.LastError = execErr.Error()
		if task.RetryCount >= cfg.RetryMaxAttempts {
			task.Status = "failed"
		} else {
			task.Status = "pending"
		}
	} else {
		task.Status = "completed"
		task.BytesDone = task.SizeBytes
	}

	if err := w.metaStore.PutHealTask(ctx, task); err != nil {
		log.Printf("Worker %d: failed to update task %s: %v", w.id, task.ID, err)
	}

	return execErr
}

// executeReplicate copies a chunk from source to destination node.
func (w *worker) executeReplicate(ctx context.Context, task *metadata.HealTask) error {
	if len(task.SourceNodes) == 0 {
		return fmt.Errorf("no source nodes specified")
	}

	// Try each source node until one succeeds
	var lastErr error
	for _, source := range task.SourceNodes {
		if source == task.DestNode {
			continue
		}
		err := w.replicator.ReplicateChunk(ctx, task.ChunkID, source, task.DestNode)
		if err == nil {
			// Success - update placement map
			if task.ShardIndex == -1 {
				// Replicated chunk - add ShardPlacement
				sp := &metadata.ShardPlacement{
					ChunkID:      task.ChunkID,
					VolumeID:     task.VolumeID,
					ShardIndex:   -1,
					NodeID:       task.DestNode,
					LastVerified: time.Now().Unix(),
				}
				if err := w.metaStore.PutShardPlacement(ctx, sp); err != nil {
					return fmt.Errorf("updating shard placement: %w", err)
				}
			}
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("all source nodes failed: %w", lastErr)
}

// executeReconstruct reads available EC shards, reconstructs the missing one, and writes it.
func (w *worker) executeReconstruct(ctx context.Context, task *metadata.HealTask) error {
	// Get current shard placements for this chunk
	placements, err := w.metaStore.GetShardPlacements(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("getting shard placements: %w", err)
	}

	// Determine EC parameters from placements
	// Find total shard count by looking at the highest shard index
	dataShards := 4
	parityShards := 2
	for _, sp := range placements {
		if sp.ShardIndex >= dataShards+parityShards {
			// This indicates more shards than default - need to derive from volume metadata
			// For now, use the shard index to estimate
			if sp.ShardIndex >= 10 { // Heuristic for larger EC configurations
				dataShards = 8
				parityShards = 4
			}
		}
	}

	ec, err := w.ecFactory.GetErasureCoder(dataShards, parityShards)
	if err != nil {
		return fmt.Errorf("getting erasure coder: %w", err)
	}

	// Reconstruct the shard
	err = w.replicator.ReconstructShard(ctx, task.ChunkID, task.ShardIndex, placements, task.DestNode, ec)
	if err != nil {
		return fmt.Errorf("reconstructing shard: %w", err)
	}

	// Add new ShardPlacement
	sp := &metadata.ShardPlacement{
		ChunkID:      task.ChunkID,
		VolumeID:     task.VolumeID,
		ShardIndex:   task.ShardIndex,
		NodeID:       task.DestNode,
		LastVerified: time.Now().Unix(),
	}
	if err := w.metaStore.PutShardPlacement(ctx, sp); err != nil {
		return fmt.Errorf("updating shard placement: %w", err)
	}

	return nil
}

// executeDeleteExcess removes an excess replica from a node.
func (w *worker) executeDeleteExcess(ctx context.Context, task *metadata.HealTask) error {
	// First verify we still have enough replicas/ shards
	placements, err := w.metaStore.GetShardPlacements(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("getting shard placements: %w", err)
	}

	// Count replicas for this shard index
	count := 0
	for _, sp := range placements {
		if sp.ShardIndex == task.ShardIndex {
			count++
		}
	}

	// For replicated chunks, ensure we keep at least 2 replicas
	// For EC, ensure we keep at least dataShards
	minRequired := 2
	if task.ShardIndex >= 0 {
		minRequired = 1 // EC: at least 1 copy of this shard
	}

	if count <= minRequired {
		return fmt.Errorf("cannot delete: only %d replicas remaining (minimum %d)", count, minRequired)
	}

	// Delete the chunk/shard from the destination node
	if task.ShardIndex == -1 {
		err = w.replicator.DeleteChunk(ctx, task.ChunkID, task.DestNode)
	} else {
		// For EC shards, we need a shard-level delete
		err = w.replicator.DeleteChunk(ctx, task.ChunkID, task.DestNode)
	}
	if err != nil {
		return fmt.Errorf("deleting from %s: %w", task.DestNode, err)
	}

	// Remove the ShardPlacement entry
	if err := w.metaStore.DeleteShardPlacement(ctx, task.ChunkID, task.ShardIndex); err != nil {
		return fmt.Errorf("removing shard placement: %w", err)
	}

	return nil
}

// stop cancels any ongoing heartbeat for this worker.
func (w *worker) stop() {
	if w.heartbeatCancel != nil {
		w.heartbeatCancel()
	}
}

// defaultErasureCoderFactory implements ErasureCoderFactory.
type defaultErasureCoderFactory struct {
	coders map[string]*chunk.ErasureCoder
	mu     sync.RWMutex
}

// NewDefaultErasureCoderFactory creates a new factory that caches erasure coders.
func NewDefaultErasureCoderFactory() ErasureCoderFactory {
	return &defaultErasureCoderFactory{
		coders: make(map[string]*chunk.ErasureCoder),
	}
}

// GetErasureCoder returns an erasure coder for the given parameters, caching the result.
func (f *defaultErasureCoderFactory) GetErasureCoder(dataShards, parityShards int) (*chunk.ErasureCoder, error) {
	key := fmt.Sprintf("%d:%d", dataShards, parityShards)

	f.mu.RLock()
	ec, ok := f.coders[key]
	f.mu.RUnlock()

	if ok {
		return ec, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after acquiring write lock
	ec, ok = f.coders[key]
	if ok {
		return ec, nil
	}

	newEC, err := chunk.NewErasureCoder(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	f.coders[key] = newEC
	return newEC, nil
}
