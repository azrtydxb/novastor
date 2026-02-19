package metadata

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// mockLockStore is a minimal lock store implementation for testing.
// It directly manipulates the FSM without going through Raft.
type mockLockStore struct {
	fsm *FSM
}

func newMockLockStore() *mockLockStore {
	return &mockLockStore{fsm: NewFSM()}
}

func (m *mockLockStore) applyOp(op *fsmOp) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	// Directly apply to FSM using the internal applyLog representation.
	log := raft.Log{
		Data: data,
	}
	result := m.fsm.Apply(&log)
	if result != nil {
		if err, ok := result.(error); ok {
			return err
		}
	}
	return nil
}

func (m *mockLockStore) acquireLockDirect(args *AcquireLockArgs) (*AcquireLockResult, error) {
	leaseID := GenerateLeaseID()
	lease := &LockLease{
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

	// Check for conflicts.
	locks, _ := m.getLocksForInode(args.Ino)
	for _, existing := range locks {
		if existing.IsExpired() {
			continue
		}
		if conflicts(lease, existing) {
			return &AcquireLockResult{ConflictingOwner: existing.Owner},
				fmt.Errorf("lock conflict: owner %q holds conflicting lock", existing.Owner)
		}
	}

	// Store lease.
	leaseData, _ := json.Marshal(lease)
	if err := m.applyOp(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockKey(leaseID), Value: leaseData}); err != nil {
		return nil, err
	}

	// Update index.
	if err := m.addLockToIndex(args.Ino, leaseID); err != nil {
		m.applyOp(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: lockKey(leaseID)})
		return nil, err
	}

	return &AcquireLockResult{LeaseID: leaseID, ExpiresAt: lease.ExpiresAt}, nil
}

func (m *mockLockStore) getLock(leaseID string) (*LockLease, error) {
	data, err := m.fsm.Get(bucketLocks, lockKey(leaseID))
	if err != nil {
		return nil, err
	}
	var lease LockLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

func (m *mockLockStore) renewLock(leaseID string, ttl time.Duration) (*LockLease, error) {
	lease, err := m.getLock(leaseID)
	if err != nil {
		return nil, err
	}
	if lease.IsExpired() {
		return nil, fmt.Errorf("lease expired")
	}
	lease.ExpiresAt = time.Now().Add(ttl).UnixNano()
	leaseData, _ := json.Marshal(lease)
	if err := m.applyOp(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockKey(leaseID), Value: leaseData}); err != nil {
		return nil, err
	}
	return lease, nil
}

func (m *mockLockStore) releaseLock(leaseID string) error {
	lease, err := m.getLock(leaseID)
	if err != nil {
		return err
	}
	_ = m.applyOp(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: lockKey(leaseID)})
	_ = m.removeLockFromIndex(lease.Ino, leaseID)
	return nil
}

func (m *mockLockStore) testLock(args *TestLockArgs) (*LockLease, error) {
	locks, _ := m.getLocksForInode(args.Ino)
	candidate := &LockLease{
		VolumeID:  args.VolumeID,
		Ino:       args.Ino,
		Start:     args.Start,
		End:       args.End,
		Type:      args.Type,
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(), // Set far future so it's not expired
	}
	for _, existing := range locks {
		if existing.IsExpired() {
			continue
		}
		if conflicts(candidate, existing) {
			return existing, nil
		}
	}
	return nil, nil
}

func (m *mockLockStore) cleanupExpiredLocks() (int, error) {
	all, _ := m.fsm.GetAll(bucketLocks)
	now := time.Now().UnixNano()
	cleaned := 0
	for key, data := range all {
		// Skip index entries (they start with "index:")
		if len(key) >= 6 && key[:6] == "index:" {
			continue
		}
		var lease LockLease
		if err := json.Unmarshal(data, &lease); err != nil {
			continue
		}
		if lease.ExpiresAt < now {
			_ = m.applyOp(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: key})
			_ = m.removeLockFromIndex(lease.Ino, lease.LeaseID)
			cleaned++
		}
	}
	return cleaned, nil
}

func (m *mockLockStore) getLocksForInode(ino uint64) ([]*LockLease, error) {
	indexData, err := m.fsm.Get(bucketLocks, lockIndexKey(ino))
	if err != nil {
		return nil, nil
	}
	var index lockIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		return nil, err
	}
	var locks []*LockLease
	for _, leaseID := range index.LeaseIDs {
		leaseData, err := m.fsm.Get(bucketLocks, lockKey(leaseID))
		if err != nil {
			continue
		}
		var lease LockLease
		if err := json.Unmarshal(leaseData, &lease); err != nil {
			continue
		}
		locks = append(locks, &lease)
	}
	return locks, nil
}

func (m *mockLockStore) addLockToIndex(ino uint64, leaseID string) error {
	indexData, err := m.fsm.Get(bucketLocks, lockIndexKey(ino))
	if err != nil {
		index := lockIndex{Ino: ino, LeaseIDs: []string{leaseID}}
		data, _ := json.Marshal(index)
		return m.applyOp(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockIndexKey(ino), Value: data})
	}
	var index lockIndex
	_ = json.Unmarshal(indexData, &index)
	index.LeaseIDs = append(index.LeaseIDs, leaseID)
	data, _ := json.Marshal(index)
	return m.applyOp(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockIndexKey(ino), Value: data})
}

func (m *mockLockStore) removeLockFromIndex(ino uint64, leaseID string) error {
	indexData, err := m.fsm.Get(bucketLocks, lockIndexKey(ino))
	if err != nil {
		return nil
	}
	var index lockIndex
	_ = json.Unmarshal(indexData, &index)
	newLeaseIDs := make([]string, 0, len(index.LeaseIDs))
	for _, id := range index.LeaseIDs {
		if id != leaseID {
			newLeaseIDs = append(newLeaseIDs, id)
		}
	}
	if len(newLeaseIDs) == 0 {
		return m.applyOp(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: lockIndexKey(ino)})
	}
	index.LeaseIDs = newLeaseIDs
	data, _ := json.Marshal(index)
	return m.applyOp(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockIndexKey(ino), Value: data})
}

// TestAcquireLock tests acquiring a lock.
func TestAcquireLock(t *testing.T) {
	store := newMockLockStore()

	args := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if result.LeaseID == "" {
		t.Fatal("expected non-empty lease ID")
	}

	// Verify the lock exists.
	lease, err := store.getLock(result.LeaseID)
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}

	if lease.Owner != "client1" {
		t.Errorf("expected owner client1, got %s", lease.Owner)
	}
	if lease.Ino != 1 {
		t.Errorf("expected ino 1, got %d", lease.Ino)
	}
}

// TestAcquireLockConflict tests that conflicting locks are rejected.
func TestAcquireLockConflict(t *testing.T) {
	store := newMockLockStore()

	// Acquire first lock.
	args1 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	_, err := store.acquireLockDirect(args1)
	if err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Try to acquire conflicting lock from different client.
	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     1,
		Start:   50,
		End:     150,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args2)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if result.ConflictingOwner != "client1" {
		t.Errorf("expected conflicting owner client1, got %s", result.ConflictingOwner)
	}
}

// TestReadLockShared tests that multiple read locks can coexist.
func TestReadLockShared(t *testing.T) {
	store := newMockLockStore()

	// Acquire first read lock.
	args1 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockRead,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result1, err := store.acquireLockDirect(args1)
	if err != nil {
		t.Fatalf("failed to acquire first read lock: %v", err)
	}

	// Acquire second read lock from different client.
	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockRead,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result2, err := store.acquireLockDirect(args2)
	if err != nil {
		t.Fatalf("failed to acquire second read lock: %v", err)
	}

	if result1.LeaseID == result2.LeaseID {
		t.Fatal("expected different lease IDs")
	}
}

// TestReadWriteConflict tests that read and write locks conflict.
func TestReadWriteConflict(t *testing.T) {
	store := newMockLockStore()

	// Acquire read lock.
	args1 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockRead,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args1); err != nil {
		t.Fatalf("failed to acquire read lock: %v", err)
	}

	// Try to acquire overlapping write lock.
	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     1,
		Start:   50,
		End:     150,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args2)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if result.ConflictingOwner != "client1" {
		t.Errorf("expected conflicting owner client1, got %s", result.ConflictingOwner)
	}
}

// TestRenewLock tests renewing a lock lease.
func TestRenewLock(t *testing.T) {
	store := newMockLockStore()

	// Acquire lock.
	args := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Second,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Get original expiration.
	lease, err := store.getLock(result.LeaseID)
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	originalExpiry := lease.ExpiresAt

	// Wait a bit and renew.
	time.Sleep(100 * time.Millisecond)

	renewed, err := store.renewLock(result.LeaseID, time.Minute)
	if err != nil {
		t.Fatalf("failed to renew lock: %v", err)
	}

	if renewed.ExpiresAt <= originalExpiry {
		t.Errorf("expected renewed expiry %d > original %d", renewed.ExpiresAt, originalExpiry)
	}
}

// TestReleaseLock tests releasing a lock.
func TestReleaseLock(t *testing.T) {
	store := newMockLockStore()

	// Acquire lock.
	args := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Release lock.
	if err := store.releaseLock(result.LeaseID); err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	// Verify lock is gone.
	if _, err := store.getLock(result.LeaseID); err == nil {
		t.Fatal("expected error getting released lock")
	}

	// Another client should now be able to acquire the lock.
	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args2); err != nil {
		t.Errorf("expected to acquire lock after release: %v", err)
	}
}

// TestLockExpiry tests that expired locks don't conflict.
func TestLockExpiry(t *testing.T) {
	store := newMockLockStore()

	// Acquire lock with short TTL.
	args := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     100 * time.Millisecond,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Wait for expiry.
	time.Sleep(200 * time.Millisecond)

	// Verify lock is expired.
	lease, err := store.getLock(result.LeaseID)
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if !lease.IsExpired() {
		t.Error("expected lease to be expired")
	}

	// Another client should be able to acquire the lock.
	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args2); err != nil {
		t.Errorf("expected to acquire lock after expiry: %v", err)
	}
}

// TestNonOverlappingLocks tests that non-overlapping locks don't conflict.
func TestNonOverlappingLocks(t *testing.T) {
	store := newMockLockStore()

	// Acquire first lock.
	args1 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     50,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args1); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Acquire non-overlapping lock from different client.
	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     1,
		Start:   50,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args2); err != nil {
		t.Errorf("non-overlapping locks should not conflict: %v", err)
	}
}

// TestCleanupExpiredLocks tests the cleanup function.
func TestCleanupExpiredLocks(t *testing.T) {
	store := newMockLockStore()

	// Acquire locks with different TTLs.
	args1 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     100 * time.Millisecond,
		FilerID: "filer1",
	}

	args2 := &AcquireLockArgs{
		Owner:   "client2",
		Ino:     2,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args1); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}
	if _, err := store.acquireLockDirect(args2); err != nil {
		t.Fatalf("failed to acquire second lock: %v", err)
	}

	// Wait for first lock to expire.
	time.Sleep(200 * time.Millisecond)

	// Run cleanup.
	count, err := store.cleanupExpiredLocks()
	if err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected to clean 1 lock, got %d", count)
	}

	// Verify second lock still exists by trying to acquire a conflicting lock.
	args3 := &AcquireLockArgs{
		Owner:   "client3",
		Ino:     2,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	result, err := store.acquireLockDirect(args3)
	if err == nil {
		t.Fatal("expected conflict for second lock")
	}
	if result.ConflictingOwner != "client2" {
		t.Errorf("expected conflicting owner client2, got %s", result.ConflictingOwner)
	}
}

// TestSameOwnerNoConflict tests that the same owner doesn't conflict with itself.
func TestSameOwnerNoConflict(t *testing.T) {
	store := newMockLockStore()

	// Acquire first lock.
	args1 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args1); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}

	// Acquire overlapping lock from same owner.
	args2 := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   50,
		End:     150,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args2); err != nil {
		t.Errorf("same owner should not conflict with itself: %v", err)
	}
}

// TestTestLock tests the TestLock operation.
func TestTestLock(t *testing.T) {
	store := newMockLockStore()

	// Acquire a lock.
	args := &AcquireLockArgs{
		Owner:   "client1",
		Ino:     1,
		Start:   0,
		End:     100,
		Type:    LockWrite,
		TTL:     time.Minute,
		FilerID: "filer1",
	}

	if _, err := store.acquireLockDirect(args); err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Test conflicting lock.
	testArgs := &TestLockArgs{
		Ino:   1,
		Start: 50,
		End:   150,
		Type:  LockWrite,
	}

	conflict, err := store.testLock(testArgs)
	if err != nil {
		t.Fatalf("TestLock failed: %v", err)
	}
	if conflict == nil {
		t.Fatal("expected conflict, got nil")
	}
	if conflict.Owner != "client1" {
		t.Errorf("expected conflicting owner client1, got %s", conflict.Owner)
	}

	// Test non-conflicting lock.
	testArgs2 := &TestLockArgs{
		Ino:   1,
		Start: 200,
		End:   300,
		Type:  LockWrite,
	}

	conflict, err = store.testLock(testArgs2)
	if err != nil {
		t.Fatalf("TestLock failed: %v", err)
	}
	if conflict != nil {
		t.Errorf("expected no conflict, got %v", conflict)
	}
}
