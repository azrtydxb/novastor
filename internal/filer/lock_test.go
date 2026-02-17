package filer

import (
	"testing"
)

func TestLock_ReadLock(t *testing.T) {
	lm := NewLockManager()
	lock := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockRead}
	if err := lm.Lock(lock); err != nil {
		t.Fatalf("failed to acquire read lock: %v", err)
	}
}

func TestLock_WriteLock(t *testing.T) {
	lm := NewLockManager()
	lock := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	if err := lm.Lock(lock); err != nil {
		t.Fatalf("failed to acquire write lock: %v", err)
	}
}

func TestLock_ReadReadNoConflict(t *testing.T) {
	lm := NewLockManager()
	lock1 := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockRead}
	lock2 := &FileLock{Owner: "client2", Ino: 1, Start: 0, End: 100, Type: LockRead}

	if err := lm.Lock(lock1); err != nil {
		t.Fatalf("failed to acquire first read lock: %v", err)
	}
	if err := lm.Lock(lock2); err != nil {
		t.Fatalf("two read locks should not conflict: %v", err)
	}
}

func TestLock_ReadWriteConflict(t *testing.T) {
	lm := NewLockManager()
	readLock := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockRead}
	writeLock := &FileLock{Owner: "client2", Ino: 1, Start: 50, End: 150, Type: LockWrite}

	if err := lm.Lock(readLock); err != nil {
		t.Fatalf("failed to acquire read lock: %v", err)
	}
	if err := lm.Lock(writeLock); err == nil {
		t.Fatal("expected conflict between read and write lock from different owners")
	}
}

func TestLock_WriteWriteConflict(t *testing.T) {
	lm := NewLockManager()
	lock1 := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	lock2 := &FileLock{Owner: "client2", Ino: 1, Start: 50, End: 150, Type: LockWrite}

	if err := lm.Lock(lock1); err != nil {
		t.Fatalf("failed to acquire first write lock: %v", err)
	}
	if err := lm.Lock(lock2); err == nil {
		t.Fatal("expected conflict between two write locks from different owners")
	}
}

func TestLock_NonOverlapping(t *testing.T) {
	lm := NewLockManager()
	lock1 := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 50, Type: LockWrite}
	lock2 := &FileLock{Owner: "client2", Ino: 1, Start: 50, End: 100, Type: LockWrite}

	if err := lm.Lock(lock1); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}
	if err := lm.Lock(lock2); err != nil {
		t.Fatalf("non-overlapping locks should not conflict: %v", err)
	}
}

func TestLock_SameOwnerNoConflict(t *testing.T) {
	lm := NewLockManager()
	lock1 := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	lock2 := &FileLock{Owner: "client1", Ino: 1, Start: 50, End: 150, Type: LockWrite}

	if err := lm.Lock(lock1); err != nil {
		t.Fatalf("failed to acquire first lock: %v", err)
	}
	if err := lm.Lock(lock2); err != nil {
		t.Fatalf("same owner should not conflict with itself: %v", err)
	}
}

func TestUnlock(t *testing.T) {
	lm := NewLockManager()
	lock := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockWrite}

	if err := lm.Lock(lock); err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if err := lm.Unlock("client1", 1, 0, 100); err != nil {
		t.Fatalf("failed to unlock: %v", err)
	}

	// After unlocking, another client should be able to acquire a write lock on the same range.
	lock2 := &FileLock{Owner: "client2", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	if err := lm.Lock(lock2); err != nil {
		t.Fatalf("expected lock to succeed after unlock: %v", err)
	}
}

func TestTestLock(t *testing.T) {
	lm := NewLockManager()
	existing := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	if err := lm.Lock(existing); err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Test a conflicting lock.
	probe := &FileLock{Owner: "client2", Ino: 1, Start: 50, End: 150, Type: LockWrite}
	conflicting, err := lm.TestLock(probe)
	if err != nil {
		t.Fatalf("unexpected error from TestLock: %v", err)
	}
	if conflicting == nil {
		t.Fatal("expected TestLock to return the conflicting lock")
	}
	if conflicting.Owner != "client1" {
		t.Fatalf("expected conflicting owner to be client1, got %s", conflicting.Owner)
	}

	// Test a non-conflicting lock.
	noConflict := &FileLock{Owner: "client2", Ino: 1, Start: 200, End: 300, Type: LockWrite}
	conflicting, err = lm.TestLock(noConflict)
	if err != nil {
		t.Fatalf("unexpected error from TestLock: %v", err)
	}
	if conflicting != nil {
		t.Fatal("expected no conflict for non-overlapping range")
	}
}

func TestReleaseAll(t *testing.T) {
	lm := NewLockManager()

	// Client1 holds multiple locks on different inodes.
	lock1 := &FileLock{Owner: "client1", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	lock2 := &FileLock{Owner: "client1", Ino: 2, Start: 0, End: 50, Type: LockRead}
	lock3 := &FileLock{Owner: "client2", Ino: 1, Start: 200, End: 300, Type: LockWrite}

	for _, l := range []*FileLock{lock1, lock2, lock3} {
		if err := lm.Lock(l); err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
	}

	lm.ReleaseAll("client1")

	// Client2 should now be able to lock the range previously held by client1.
	probe := &FileLock{Owner: "client2", Ino: 1, Start: 0, End: 100, Type: LockWrite}
	if err := lm.Lock(probe); err != nil {
		t.Fatalf("expected lock to succeed after ReleaseAll: %v", err)
	}

	// Client2's original lock should still be in place.
	probe2 := &FileLock{Owner: "client3", Ino: 1, Start: 200, End: 300, Type: LockWrite}
	if err := lm.Lock(probe2); err == nil {
		t.Fatal("expected conflict with client2's remaining lock")
	}
}
