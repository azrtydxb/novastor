// Package metadata provides the metadata service for NovaStor.
// This file implements distributed locking primitives for coordinating
// access to shared filesystem resources.
package metadata

import (
	"context"
	"fmt"
	"time"
)

// LockType represents the type of a file lock.
type LockType int

const (
	// LockRead is a shared read lock.
	LockRead LockType = iota
	// LockWrite is an exclusive write lock.
	LockWrite
)

func (lt LockType) String() string {
	switch lt {
	case LockRead:
		return "read"
	case LockWrite:
		return "write"
	default:
		return "unknown"
	}
}

// LockLease represents a distributed file lock lease.
type LockLease struct {
	// LeaseID is the unique identifier for this lease.
	LeaseID string `json:"leaseID"`
	// Owner identifies the client holding the lock (typically client:hostname:pid).
	Owner string `json:"owner"`
	// VolumeID is the volume this lock applies to (optional, for namespace isolation).
	VolumeID string `json:"volumeID,omitempty"`
	// Ino is the inode number being locked.
	Ino uint64 `json:"ino"`
	// Start is the byte offset where the lock begins.
	Start int64 `json:"start"`
	// End is the byte offset where the lock ends (-1 means EOF).
	End int64 `json:"end"`
	// Type is the lock type (read or write).
	Type LockType `json:"type"`
	// ExpiresAt is the Unix nanosecond timestamp when this lease expires.
	ExpiresAt int64 `json:"expiresAt"`
	// FilerID identifies which filer instance granted this lock.
	FilerID string `json:"filerID,omitempty"`
}

// IsExpired returns true if the lease has expired.
func (l *LockLease) IsExpired() bool {
	return time.Now().UnixNano() > l.ExpiresAt
}

// RemainingDuration returns the remaining duration until expiration.
func (l *LockLease) RemainingDuration() time.Duration {
	expiration := time.Unix(0, l.ExpiresAt)
	remaining := time.Until(expiration)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// lockKey generates the storage key for a lock lease.
func lockKey(leaseID string) string {
	return leaseID
}

// lockIndexKey is the key for the lock index (maps inode to list of lease IDs).
func lockIndexKey(ino uint64) string {
	return fmt.Sprintf("index:%d", ino)
}

// lockIndex stores the mapping from inode to lease IDs.
type lockIndex struct {
	Ino      uint64   `json:"ino"`
	LeaseIDs []string `json:"leaseIDs"`
}

// AcquireLockArgs contains the arguments for acquiring a lock.
type AcquireLockArgs struct {
	Owner    string        `json:"owner"`
	VolumeID string        `json:"volumeID,omitempty"`
	Ino      uint64        `json:"ino"`
	Start    int64         `json:"start"`
	End      int64         `json:"end"`
	Type     LockType      `json:"type"`
	TTL      time.Duration `json:"ttl"` // Lease TTL
	FilerID  string        `json:"filerID,omitempty"`
}

// RenewLockArgs contains the arguments for renewing a lock.
type RenewLockArgs struct {
	LeaseID string        `json:"leaseID"`
	TTL     time.Duration `json:"ttl"`
}

// ReleaseLockArgs contains the arguments for releasing a lock.
type ReleaseLockArgs struct {
	LeaseID string `json:"leaseID"`
	Owner   string `json:"owner"`
}

// TestLockArgs contains the arguments for testing a lock.
type TestLockArgs struct {
	VolumeID string   `json:"volumeID,omitempty"`
	Ino      uint64   `json:"ino"`
	Start    int64    `json:"start"`
	End      int64    `json:"end"`
	Type     LockType `json:"type"`
}

// AcquireLockResult contains the result of acquiring a lock.
type AcquireLockResult struct {
	LeaseID          string `json:"leaseID"`
	ExpiresAt        int64  `json:"expiresAt"`
	ConflictingOwner string `json:"conflictingOwner,omitempty"`
}

// overlaps returns true if two byte ranges overlap.
// End == -1 means the range extends to EOF.
func overlaps(s1, e1, s2, e2 int64) bool {
	// Normalize: treat -1 (EOF) as max int64 for comparison.
	if e1 == -1 {
		e1 = int64(^uint64(0) >> 1) // math.MaxInt64
	}
	if e2 == -1 {
		e2 = int64(^uint64(0) >> 1)
	}
	return s1 < e2 && s2 < e1
}

// conflicts returns true if two locks conflict.
// Write locks conflict with any overlapping lock from a different owner.
// Read locks conflict only with overlapping write locks from a different owner.
func conflicts(a, b *LockLease) bool {
	// Skip expired locks
	if a.IsExpired() || b.IsExpired() {
		return false
	}
	if a.Owner == b.Owner {
		return false
	}
	if !overlaps(a.Start, a.End, b.Start, b.End) {
		return false
	}
	// Two read locks never conflict.
	if a.Type == LockRead && b.Type == LockRead {
		return false
	}
	return true
}

// FSM operations for locks are applied through the standard apply mechanism
// using the bucketLocks bucket. The lock manager handles coordination.

// LockClient defines the interface for distributed lock operations.
// Both RaftStore (direct) and GRPCClient (remote) implement this interface.
type LockClient interface {
	AcquireLock(ctx context.Context, args *AcquireLockArgs) (*AcquireLockResult, error)
	RenewLock(ctx context.Context, args *RenewLockArgs) (*LockLease, error)
	ReleaseLock(ctx context.Context, args *ReleaseLockArgs) error
	TestLock(ctx context.Context, args *TestLockArgs) (*LockLease, error)
	GetLock(ctx context.Context, leaseID string) (*LockLease, error)
	ListLocks(ctx context.Context, volumeID string) ([]*LockLease, error)
	CleanupExpiredLocks(ctx context.Context) (int, error)
}

// GenerateLeaseID creates a unique lease ID.
func GenerateLeaseID() string {
	// Use nanosecond timestamp + random component for uniqueness.
	// In production with Raft, we could use a monotonic counter.
	return fmt.Sprintf("lease-%d-%d", time.Now().UnixNano(), time.Now().Nanosecond()%10000)
}
