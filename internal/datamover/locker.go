package datamover

import (
	"context"
	"fmt"
	"time"
)

// metadataChunkLocker implements ChunkLocker using the metadata store.
type metadataChunkLocker struct {
	metaStore MetadataStore
}

// NewMetadataChunkLocker creates a new ChunkLocker backed by the metadata store.
func NewMetadataChunkLocker(metaStore MetadataStore) ChunkLocker {
	return &metadataChunkLocker{metaStore: metaStore}
}

// Acquire atomically writes a lock entry; returns error if held by a live owner.
func (l *metadataChunkLocker) Acquire(ctx context.Context, lockID, ownerTaskID string) error {
	return l.metaStore.AcquireChunkLock(ctx, lockID, ownerTaskID)
}

// Heartbeat extends the lock TTL.
func (l *metadataChunkLocker) Heartbeat(ctx context.Context, lockID, ownerTaskID string) error {
	return l.metaStore.HeartbeatChunkLock(ctx, lockID, ownerTaskID)
}

// Release removes the lock entry.
func (l *metadataChunkLocker) Release(ctx context.Context, lockID, ownerTaskID string) error {
	return l.metaStore.ReleaseChunkLock(ctx, lockID, ownerTaskID)
}

// LockID generates a unique lock identifier for a (chunkID, destNode) pair.
func LockID(chunkID, destNode string) string {
	return fmt.Sprintf("%s:%s", chunkID, destNode)
}

// LockRunner manages a lock with automatic heartbeat until the context is canceled.
type LockRunner struct {
	locker    ChunkLocker
	lockID    string
	ownerTask string
	interval  time.Duration
}

// NewLockRunner creates a new LockRunner that will heartbeat at the given interval.
func NewLockRunner(locker ChunkLocker, lockID, ownerTask string, interval time.Duration) *LockRunner {
	return &LockRunner{
		locker:    locker,
		lockID:    lockID,
		ownerTask: ownerTask,
		interval:  interval,
	}
}

// Run starts the heartbeat loop. It will heartbeat until the context is canceled
// or a heartbeat fails. Returns any error from the initial lock acquisition or heartbeat.
func (lr *LockRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(lr.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context canceled, attempt to release the lock
			_ = lr.locker.Release(context.Background(), lr.lockID, lr.ownerTask)
			return ctx.Err()
		case <-ticker.C:
			if err := lr.locker.Heartbeat(ctx, lr.lockID, lr.ownerTask); err != nil {
				// Heartbeat failed, try to release the lock
				_ = lr.locker.Release(context.Background(), lr.lockID, lr.ownerTask)
				return fmt.Errorf("heartbeat failed: %w", err)
			}
		}
	}
}

// WithLock executes a function while holding a lock with automatic heartbeat.
// The lock is automatically released when the function returns or the context is canceled.
func WithLock(ctx context.Context, locker ChunkLocker, lockID, ownerTask string, interval time.Duration, fn func() error) error {
	// First, acquire the lock
	if err := locker.Acquire(ctx, lockID, ownerTask); err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}

	// Start heartbeat in a goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan error, 1)

	go func() {
		runner := NewLockRunner(locker, lockID, ownerTask, interval)
		heartbeatDone <- runner.Run(heartbeatCtx)
	}()

	// Execute the function
	var err error
	func() {
		defer cancelHeartbeat()
		err = fn()
	}()

	// Wait for heartbeat to finish (it will release the lock)
	heartbeatErr := <-heartbeatDone

	// Prefer function error over heartbeat error
	if err != nil {
		return err
	}
	return heartbeatErr
}
