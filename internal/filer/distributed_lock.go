package filer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
)

// DefaultLeaseTTL is the default TTL for lock leases.
// Clients should renew before this expires.
const DefaultLeaseTTL = 30 * time.Second

// RenewalInterval is how often we attempt to renew locks we hold.
const RenewalInterval = 10 * time.Second

// RenewalTimeout is how long we wait for a renewal to succeed.
const RenewalTimeout = 5 * time.Second

// DistributedLockManager manages distributed file locks using the metadata service.
// It implements lease-based locking with automatic renewal and expiration handling.
type DistributedLockManager struct {
	// client is the metadata service client for distributed lock operations.
	client metadata.LockClient
	// filerID uniquely identifies this filer instance.
	filerID string
	// localLeases tracks locks held by this filer instance for renewal.
	localLeases map[string]*localLease
	// mu protects localLeases.
	mu sync.RWMutex
	// done is closed to stop the renewal goroutine.
	done chan struct{}
	// wg waits for the renewal goroutine to exit.
	wg sync.WaitGroup
}

// localLease represents a lease held by this filer instance.
type localLease struct {
	lease   *metadata.LockLease
	renewAt time.Time
}

// NewDistributedLockManager creates a new distributed lock manager.
func NewDistributedLockManager(client metadata.LockClient, filerID string) *DistributedLockManager {
	lm := &DistributedLockManager{
		client:      client,
		filerID:     filerID,
		localLeases: make(map[string]*localLease),
		done:        make(chan struct{}),
	}
	lm.startRenewalGoroutine()
	return lm
}

// startRenewalGoroutine starts a background goroutine that renews leases.
func (lm *DistributedLockManager) startRenewalGoroutine() {
	lm.wg.Add(1)
	go func() {
		defer lm.wg.Done()
		ticker := time.NewTicker(RenewalInterval)
		defer ticker.Stop()

		for {
			select {
			case <-lm.done:
				return
			case <-ticker.C:
				lm.renewLeases()
			}
		}
	}()
}

// renewLeases renews all local leases that are due for renewal.
func (lm *DistributedLockManager) renewLeases() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()

	for leaseID, local := range lm.localLeases {
		if now.Before(local.renewAt) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), RenewalTimeout)
		args := &metadata.RenewLockArgs{
			LeaseID: leaseID,
			TTL:     DefaultLeaseTTL,
		}
		lease, err := lm.client.RenewLock(ctx, args)
		cancel()

		if err != nil {
			// Renewal failed - remove from local tracking.
			delete(lm.localLeases, leaseID)
			metrics.LockRenewalFailures.Inc()
			metrics.LockOperationsTotal.WithLabelValues("renew", "failure").Inc()
			// The lease will eventually expire in the metadata service.
			continue
		}

		// Update local lease info.
		local.lease = lease
		local.renewAt = time.Now().Add(RenewalInterval)
		metrics.LockOperationsTotal.WithLabelValues("renew", "success").Inc()
	}

	metrics.DistributedLockLeases.Set(float64(len(lm.localLeases)))
}

// Lock attempts to acquire a distributed file lock.
// The lock will be automatically renewed until explicitly released.
func (lm *DistributedLockManager) Lock(owner string, ino uint64, start, end int64, lockType LockType) error {
	return lm.LockWithVolume(owner, "", ino, start, end, lockType)
}

// LockWithVolume attempts to acquire a distributed file lock with a volume ID.
func (lm *DistributedLockManager) LockWithVolume(owner, volumeID string, ino uint64, start, end int64, lockType LockType) error {
	// Convert lock type.
	var metaType metadata.LockType
	switch lockType {
	case LockRead:
		metaType = metadata.LockRead
	case LockWrite:
		metaType = metadata.LockWrite
	default:
		return fmt.Errorf("invalid lock type: %d", lockType)
	}

	ctx := context.Background()
	args := &metadata.AcquireLockArgs{
		Owner:    owner,
		VolumeID: volumeID,
		Ino:      ino,
		Start:    start,
		End:      end,
		Type:     metaType,
		TTL:      DefaultLeaseTTL,
		FilerID:  lm.filerID,
	}

	result, err := lm.client.AcquireLock(ctx, args)
	if err != nil {
		metrics.LockOperationsTotal.WithLabelValues("acquire", "failure").Inc()
		return fmt.Errorf("acquiring lock: %w", err)
	}

	metrics.LockOperationsTotal.WithLabelValues("acquire", "success").Inc()

	// Track locally for renewal.
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.localLeases[result.LeaseID] = &localLease{
		lease: &metadata.LockLease{
			LeaseID:   result.LeaseID,
			Owner:     owner,
			VolumeID:  volumeID,
			Ino:       ino,
			Start:     start,
			End:       end,
			Type:      metaType,
			ExpiresAt: result.ExpiresAt,
			FilerID:   lm.filerID,
		},
		renewAt: time.Now().Add(RenewalInterval),
	}
	metrics.DistributedLockLeases.Set(float64(len(lm.localLeases)))

	return nil
}

// Unlock releases a distributed file lock.
// The lease ID identifies which lock to release.
func (lm *DistributedLockManager) Unlock(leaseID string) error {
	return lm.UnlockWithOwner(leaseID, "")
}

// UnlockWithOwner releases a lock, verifying the owner if provided.
func (lm *DistributedLockManager) UnlockWithOwner(leaseID, owner string) error {
	ctx := context.Background()
	args := &metadata.ReleaseLockArgs{
		LeaseID: leaseID,
		Owner:   owner,
	}

	if err := lm.client.ReleaseLock(ctx, args); err != nil {
		metrics.LockOperationsTotal.WithLabelValues("release", "failure").Inc()
		return fmt.Errorf("releasing lock: %w", err)
	}

	// Remove from local tracking.
	lm.mu.Lock()
	delete(lm.localLeases, leaseID)
	metrics.DistributedLockLeases.Set(float64(len(lm.localLeases)))
	lm.mu.Unlock()

	metrics.LockOperationsTotal.WithLabelValues("release", "success").Inc()
	return nil
}

// TestLock checks if a lock could be acquired without actually acquiring it.
// Returns nil if no conflict exists, or the conflicting lock info.
func (lm *DistributedLockManager) TestLock(volumeID string, ino uint64, start, end int64, lockType LockType) (*metadata.LockLease, error) {
	var metaType metadata.LockType
	switch lockType {
	case LockRead:
		metaType = metadata.LockRead
	case LockWrite:
		metaType = metadata.LockWrite
	default:
		return nil, fmt.Errorf("invalid lock type: %d", lockType)
	}

	ctx := context.Background()
	args := &metadata.TestLockArgs{
		VolumeID: volumeID,
		Ino:      ino,
		Start:    start,
		End:      end,
		Type:     metaType,
	}

	return lm.client.TestLock(ctx, args)
}

// ReleaseAllForOwner releases all locks held by a specific owner.
// This is typically called when an NFS client disconnects.
func (lm *DistributedLockManager) ReleaseAllForOwner(owner string) error {
	ctx := context.Background()
	volumeID := "" // Release across all volumes.

	// List all locks to find ones owned by this client.
	locks, err := lm.client.ListLocks(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("listing locks: %w", err)
	}

	var releaseErrors []error
	for _, lock := range locks {
		if lock.Owner == owner {
			args := &metadata.ReleaseLockArgs{
				LeaseID: lock.LeaseID,
				Owner:   owner,
			}
			if err := lm.client.ReleaseLock(ctx, args); err != nil {
				releaseErrors = append(releaseErrors, err)
			}

			// Remove from local tracking if we held it.
			lm.mu.Lock()
			delete(lm.localLeases, lock.LeaseID)
			lm.mu.Unlock()
		}
	}

	metrics.DistributedLockLeases.Set(float64(len(lm.localLeases)))

	if len(releaseErrors) > 0 {
		return fmt.Errorf("errors releasing %d locks: %v", len(releaseErrors), releaseErrors)
	}
	return nil
}

// Close stops the lock manager and releases all locally held locks.
func (lm *DistributedLockManager) Close() error {
	close(lm.done)
	lm.wg.Wait()

	// Release all local locks.
	lm.mu.Lock()
	defer lm.mu.Unlock()

	ctx := context.Background()
	var releaseErrors []error

	for leaseID, local := range lm.localLeases {
		args := &metadata.ReleaseLockArgs{
			LeaseID: leaseID,
			Owner:   local.lease.Owner,
		}
		if err := lm.client.ReleaseLock(ctx, args); err != nil {
			releaseErrors = append(releaseErrors, err)
		}
	}

	lm.localLeases = make(map[string]*localLease)
	metrics.DistributedLockLeases.Set(0)

	if len(releaseErrors) > 0 {
		return fmt.Errorf("errors releasing locks during close: %v", releaseErrors)
	}
	return nil
}

// GetLocalLeaseCount returns the number of leases currently held by this filer.
func (lm *DistributedLockManager) GetLocalLeaseCount() int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return len(lm.localLeases)
}

// RunCleanup runs a one-time cleanup of expired locks.
// This should be called periodically by one filer instance (e.g., the leader).
func (lm *DistributedLockManager) RunCleanup(ctx context.Context) (int, error) {
	count, err := lm.client.CleanupExpiredLocks(ctx)
	if err != nil {
		return 0, fmt.Errorf("cleaning expired locks: %w", err)
	}
	if count > 0 {
		metrics.LockLeaseExpirations.Add(float64(count))
	}
	return count, nil
}

// StartCleanupPeriodic starts a background goroutine that periodically cleans up expired locks.
// Only one filer instance should run this (e.g., the leader).
func (lm *DistributedLockManager) StartCleanupPeriodic(ctx context.Context, interval time.Duration) {
	lm.wg.Add(1)
	go func() {
		defer lm.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-lm.done:
				return
			case <-ticker.C:
				_, _ = lm.RunCleanup(ctx)
			}
		}
	}()
}
