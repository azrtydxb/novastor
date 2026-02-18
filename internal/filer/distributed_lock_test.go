package filer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
)

// Helper to call metadata.conflicts with proper types.
func checkConflicts(a, b *metadata.LockLease) bool {
	// This is a local implementation that mirrors metadata.conflicts
	// for use in the mock client.
	if a.Owner == b.Owner {
		return false
	}
	if a.IsExpired() || b.IsExpired() {
		return false
	}
	if !overlaps(a.Start, a.End, b.Start, b.End) {
		return false
	}
	if a.Type == metadata.LockRead && b.Type == metadata.LockRead {
		return false
	}
	return true
}

// mockLockClient is a mock implementation of metadata.LockClient for testing.
type mockLockClient struct {
	mu            sync.Mutex
	leases        map[string]*metadata.LockLease
	leaseCounter  int
	failRenew     bool
	failAcquire   bool
	cleanupCalled bool
}

func newMockLockClient() *mockLockClient {
	return &mockLockClient{
		leases: make(map[string]*metadata.LockLease),
	}
}

func (m *mockLockClient) AcquireLock(_ context.Context, args *metadata.AcquireLockArgs) (*metadata.AcquireLockResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failAcquire {
		return nil, fmt.Errorf("acquire failed")
	}

	// Check for conflicts with existing locks.
	for _, lease := range m.leases {
		if lease.IsExpired() {
			continue
		}
		candidate := &metadata.LockLease{
			Owner:     args.Owner,
			Ino:       args.Ino,
			Start:     args.Start,
			End:       args.End,
			Type:      args.Type,
			ExpiresAt: time.Now().Add(args.TTL).UnixNano(),
		}
		if lease.Ino == args.Ino && checkConflicts(candidate, lease) {
			return &metadata.AcquireLockResult{
				ConflictingOwner: lease.Owner,
			}, fmt.Errorf("lock conflict")
		}
	}

	// Create new lease.
	leaseID := fmt.Sprintf("lease-%d", m.leaseCounter)
	m.leaseCounter++

	lease := &metadata.LockLease{
		LeaseID:   leaseID,
		Owner:     args.Owner,
		VolumeID:  args.VolumeID,
		Ino:       args.Ino,
		Start:     args.Start,
		End:       args.End,
		Type:      args.Type,
		ExpiresAt: time.Now().Add(args.TTL).UnixNano(),
		FilerID:   args.FilerID,
	}
	m.leases[leaseID] = lease

	return &metadata.AcquireLockResult{
		LeaseID:   leaseID,
		ExpiresAt: lease.ExpiresAt,
	}, nil
}

func (m *mockLockClient) RenewLock(_ context.Context, args *metadata.RenewLockArgs) (*metadata.LockLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failRenew {
		return nil, fmt.Errorf("renew failed")
	}

	lease, ok := m.leases[args.LeaseID]
	if !ok {
		return nil, fmt.Errorf("lease not found")
	}

	lease.ExpiresAt = time.Now().Add(args.TTL).UnixNano()
	return lease, nil
}

func (m *mockLockClient) ReleaseLock(_ context.Context, args *metadata.ReleaseLockArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, ok := m.leases[args.LeaseID]
	if !ok {
		return fmt.Errorf("lease not found")
	}

	if args.Owner != "" && lease.Owner != args.Owner {
		return fmt.Errorf("owner mismatch")
	}

	delete(m.leases, args.LeaseID)
	return nil
}

func (m *mockLockClient) TestLock(_ context.Context, args *metadata.TestLockArgs) (*metadata.LockLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, lease := range m.leases {
		if lease.IsExpired() {
			continue
		}
		if lease.Ino != args.Ino {
			continue
		}
		candidate := &metadata.LockLease{
			Ino:       args.Ino,
			Start:     args.Start,
			End:       args.End,
			Type:      args.Type,
			ExpiresAt: time.Now().Add(time.Minute).UnixNano(), // Set far future so it's not expired
		}
		if checkConflicts(candidate, lease) {
			return lease, nil
		}
	}
	return nil, nil
}

func (m *mockLockClient) GetLock(_ context.Context, leaseID string) (*metadata.LockLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, ok := m.leases[leaseID]
	if !ok {
		return nil, fmt.Errorf("lease not found")
	}
	return lease, nil
}

func (m *mockLockClient) ListLocks(_ context.Context, volumeID string) ([]*metadata.LockLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*metadata.LockLease
	for _, lease := range m.leases {
		if lease.IsExpired() {
			continue
		}
		if volumeID != "" && lease.VolumeID != volumeID {
			continue
		}
		result = append(result, lease)
	}
	return result, nil
}

func (m *mockLockClient) CleanupExpiredLocks(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cleanupCalled = true
	cleaned := 0
	now := time.Now().UnixNano()

	for leaseID, lease := range m.leases {
		if lease.ExpiresAt < now {
			delete(m.leases, leaseID)
			cleaned++
		}
	}
	return cleaned, nil
}

// TestDistributedLockManager_Acquire tests acquiring locks.
func TestDistributedLockManager_Acquire(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	err := lm.Lock("client1", 1, 0, 100, LockWrite)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if lm.GetLocalLeaseCount() != 1 {
		t.Errorf("expected 1 local lease, got %d", lm.GetLocalLeaseCount())
	}

	// Verify the lease exists in the mock.
	leases, _ := client.ListLocks(context.Background(), "")
	if len(leases) != 1 {
		t.Errorf("expected 1 lease in client, got %d", len(leases))
	}
}

// TestDistributedLockManager_Conflict tests that conflicting locks are rejected.
func TestDistributedLockManager_Conflict(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	// Acquire first lock.
	if err := lm.Lock("client1", 1, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Try to acquire conflicting lock.
	err := lm.Lock("client2", 1, 50, 150, LockWrite)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}

// TestDistributedLockManager_ReadWriteShared tests that read locks are shared.
func TestDistributedLockManager_ReadWriteShared(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	// Create two lock managers for different "filers".
	lm2 := NewDistributedLockManager(client, "filer2")
	defer mustClose(lm2)

	// Acquire read lock from first manager.
	if err := lm.Lock("client1", 1, 0, 100, LockRead); err != nil {
		t.Fatalf("failed to acquire first read lock: %v", err)
	}

	// Acquire read lock from second manager (simulating different filer).
	if err := lm2.Lock("client2", 1, 0, 100, LockRead); err != nil {
		t.Fatalf("failed to acquire second read lock: %v", err)
	}

	leases, _ := client.ListLocks(context.Background(), "")
	if len(leases) != 2 {
		t.Errorf("expected 2 leases, got %d", len(leases))
	}
}

// TestDistributedLockManager_Unlock tests releasing locks.
func TestDistributedLockManager_Unlock(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	// Need to get the lease ID somehow. For this test, we'll
	// use the mock directly to find it.
	_, _ = client.AcquireLock(context.Background(), &metadata.AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    metadata.LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	})

	leases, _ := client.ListLocks(context.Background(), "")
	if len(leases) != 1 {
		t.Fatalf("expected 1 lease, got %d", len(leases))
	}
	leaseID := leases[0].LeaseID

	// Unlock using the lease ID.
	if err := lm.Unlock(leaseID); err != nil {
		t.Fatalf("failed to unlock: %v", err)
	}

	leases, _ = client.ListLocks(context.Background(), "")
	if len(leases) != 0 {
		t.Errorf("expected 0 leases after unlock, got %d", len(leases))
	}
}

// TestDistributedLockManager_TestLock tests the TestLock method.
func TestDistributedLockManager_TestLock(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	// Acquire a lock.
	if err := lm.Lock("client1", 1, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Test for conflict.
	conflict, err := lm.TestLock("", 1, 50, 150, LockWrite)
	if err != nil {
		t.Fatalf("TestLock failed: %v", err)
	}
	if conflict == nil {
		t.Fatal("expected conflict, got nil")
	}

	// Test no conflict.
	conflict, err = lm.TestLock("", 1, 200, 300, LockWrite)
	if err != nil {
		t.Fatalf("TestLock failed: %v", err)
	}
	if conflict != nil {
		t.Errorf("expected no conflict, got %v", conflict)
	}
}

// TestDistributedLockManager_ReleaseAllForOwner tests releasing all locks for an owner.
func TestDistributedLockManager_ReleaseAllForOwner(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	// Acquire multiple locks for the same owner.
	if err := lm.Lock("client1", 1, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}
	if err := lm.Lock("client1", 2, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire second lock: %v", err)
	}

	// Release all for owner.
	if err := lm.ReleaseAllForOwner("client1"); err != nil {
		t.Fatalf("failed to release all: %v", err)
	}

	leases, _ := client.ListLocks(context.Background(), "")
	if len(leases) != 0 {
		t.Errorf("expected 0 leases after release all, got %d", len(leases))
	}
}

// TestDistributedLockManager_Cleanup tests the cleanup functionality.
func TestDistributedLockManager_Cleanup(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	ctx := context.Background()

	// Add an expired lease directly to the mock.
	expiredLease := &metadata.LockLease{
		LeaseID:   "expired-lease",
		Owner:     "client1",
		Ino:       1,
		Start:     0,
		End:       100,
		Type:      metadata.LockWrite,
		ExpiresAt: time.Now().Add(-time.Hour).UnixNano(),
	}
	client.mu.Lock()
	client.leases["expired-lease"] = expiredLease
	client.mu.Unlock()

	// Run cleanup.
	count, err := lm.RunCleanup(ctx)
	if err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected to clean 1 lock, got %d", count)
	}
}

// TestDistributedLockManager_ConcurrentAcquire tests concurrent lock acquisition.
func TestDistributedLockManager_ConcurrentAcquire(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	var wg sync.WaitGroup
	errors := make(chan error, 10)
	successes := 0

	// Try to acquire the same lock from multiple "clients".
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			owner := fmt.Sprintf("client%d", clientID)
			err := lm.Lock(owner, 1, 0, 100, LockWrite)
			if err == nil {
				successes++
			} else {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Only one should succeed.
	if successes != 1 {
		t.Errorf("expected 1 successful lock, got %d", successes)
	}

	// Collect conflicts.
	conflictCount := 0
	for err := range errors {
		if err != nil {
			conflictCount++
		}
	}
	if conflictCount != 9 {
		t.Errorf("expected 9 conflicts, got %d", conflictCount)
	}
}

// TestDistributedLockManager_Close tests that Close releases all locks.
func TestDistributedLockManager_Close(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")

	// Acquire some locks.
	if err := lm.Lock("client1", 1, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}
	if err := lm.Lock("client1", 2, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire second lock: %v", err)
	}

	// Close the manager.
	if err := lm.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Verify all locks were released.
	leases, _ := client.ListLocks(context.Background(), "")
	if len(leases) != 0 {
		t.Errorf("expected 0 leases after close, got %d", len(leases))
	}
}

// TestDistributedLockManager_SameOwnerNoConflict tests that same owner doesn't conflict.
func TestDistributedLockManager_SameOwnerNoConflict(t *testing.T) {
	client := newMockLockClient()
	lm := NewDistributedLockManager(client, "filer1")
	defer mustClose(lm)

	// Acquire first lock.
	if err := lm.Lock("client1", 1, 0, 100, LockWrite); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Acquire overlapping lock from same owner.
	if err := lm.Lock("client1", 1, 50, 150, LockWrite); err != nil {
		t.Errorf("same owner should not conflict: %v", err)
	}

	if lm.GetLocalLeaseCount() != 2 {
		t.Errorf("expected 2 local leases, got %d", lm.GetLocalLeaseCount())
	}
}

// mustClose is a helper that closes a resource and ignores any error.
// Used in tests to avoid unchecked error violations from defer.
func mustClose(c interface{ Close() error }) {
	_ = c.Close()
}
