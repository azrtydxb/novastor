package filer

import (
	"fmt"
	"sync"

	"github.com/azrtydxb/novastor/internal/metrics"
)

// LockType represents the type of a file lock.
type LockType int

const (
	// LockRead is a shared read lock.
	LockRead LockType = iota
	// LockWrite is an exclusive write lock.
	LockWrite
)

// FileLock represents an advisory byte-range lock on a file.
type FileLock struct {
	Owner string
	Ino   uint64
	Start int64
	End   int64 // -1 means EOF
	Type  LockType
}

// LockManager manages advisory file locks.
type LockManager struct {
	mu    sync.Mutex
	locks map[uint64][]*FileLock // ino -> locks
}

// NewLockManager creates a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{locks: make(map[uint64][]*FileLock)}
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
func conflicts(a, b *FileLock) bool {
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

// Lock acquires a lock. Returns an error if a conflicting lock exists.
func (lm *LockManager) Lock(lock *FileLock) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, existing := range lm.locks[lock.Ino] {
		if conflicts(lock, existing) {
			return fmt.Errorf("lock conflict: owner %q holds conflicting lock on inode %d [%d,%d)",
				existing.Owner, existing.Ino, existing.Start, existing.End)
		}
	}

	lm.locks[lock.Ino] = append(lm.locks[lock.Ino], lock)
	lm.updateActiveLocks()
	return nil
}

// Unlock releases a lock matching the given owner, inode, and byte range.
func (lm *LockManager) Unlock(owner string, ino uint64, start, end int64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	locks := lm.locks[ino]
	for i, l := range locks {
		if l.Owner == owner && l.Start == start && l.End == end {
			lm.locks[ino] = append(locks[:i], locks[i+1:]...)
			if len(lm.locks[ino]) == 0 {
				delete(lm.locks, ino)
			}
			lm.updateActiveLocks()
			return nil
		}
	}
	return fmt.Errorf("no matching lock found for owner %q on inode %d [%d,%d)", owner, ino, start, end)
}

// TestLock checks if a lock could be acquired without actually acquiring it.
// Returns nil if no conflict exists, or the conflicting lock if one does.
func (lm *LockManager) TestLock(lock *FileLock) (*FileLock, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, existing := range lm.locks[lock.Ino] {
		if conflicts(lock, existing) {
			return existing, nil
		}
	}
	return nil, nil
}

// ReleaseAll releases all locks held by a given owner (for client disconnect cleanup).
func (lm *LockManager) ReleaseAll(owner string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for ino, locks := range lm.locks {
		filtered := locks[:0]
		for _, l := range locks {
			if l.Owner != owner {
				filtered = append(filtered, l)
			}
		}
		if len(filtered) == 0 {
			delete(lm.locks, ino)
		} else {
			lm.locks[ino] = filtered
		}
	}
	lm.updateActiveLocks()
}

// updateActiveLocks updates the active locks metric. Must be called with lm.mu held.
func (lm *LockManager) updateActiveLocks() {
	total := 0
	for _, locks := range lm.locks {
		total += len(locks)
	}
	metrics.ActiveLocks.Set(float64(total))
}
