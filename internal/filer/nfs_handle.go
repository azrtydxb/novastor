package filer

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
)

// nfsHandleSize is the size of NFS v3 file handles in bytes.
// We use 32 bytes: 8 bytes inode + 8 bytes generation + 16 bytes random.
const nfsHandleSize = 32

// handleManager maps opaque NFS file handles to inodes and vice versa.
// File handles are 32-byte opaque values containing the inode number,
// a generation counter, and random bytes for security.
type handleManager struct {
	mu         sync.RWMutex
	byIno      map[uint64][]byte   // inode -> handle bytes
	byHandle   map[string]uint64   // hex(handle) -> inode
	generation uint64
}

func newHandleManager() *handleManager {
	hm := &handleManager{
		byIno:    make(map[uint64][]byte),
		byHandle: make(map[string]uint64),
	}
	// Pre-create the root handle.
	hm.getOrCreateHandle(RootIno)
	return hm
}

// handleKey returns a string key for map lookup from a handle byte slice.
func handleKey(h []byte) string {
	return string(h)
}

// getOrCreateHandle returns the file handle for an inode, creating one if needed.
func (hm *handleManager) getOrCreateHandle(ino uint64) []byte {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if h, ok := hm.byIno[ino]; ok {
		result := make([]byte, len(h))
		copy(result, h)
		return result
	}

	handle := make([]byte, nfsHandleSize)
	binary.BigEndian.PutUint64(handle[0:8], ino)
	hm.generation++
	binary.BigEndian.PutUint64(handle[8:16], hm.generation)
	// Fill remaining 16 bytes with random data for handle unpredictability.
	_, _ = rand.Read(handle[16:32])

	stored := make([]byte, nfsHandleSize)
	copy(stored, handle)
	hm.byIno[ino] = stored
	hm.byHandle[handleKey(stored)] = ino

	result := make([]byte, nfsHandleSize)
	copy(result, handle)
	return result
}

// lookupHandle resolves a file handle to an inode number.
// Returns the inode and true if found, or 0 and false if not.
func (hm *handleManager) lookupHandle(handle []byte) (uint64, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	ino, ok := hm.byHandle[handleKey(handle)]
	return ino, ok
}

// removeHandle removes the handle mapping for an inode (used after delete).
func (hm *handleManager) removeHandle(ino uint64) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if h, ok := hm.byIno[ino]; ok {
		delete(hm.byHandle, handleKey(h))
		delete(hm.byIno, ino)
	}
}

// inodeFromHandle extracts the inode number embedded in a handle.
// This is used as a fast path when the handle is not in the cache
// (e.g., after server restart). The caller must verify the inode exists.
func inodeFromHandle(handle []byte) uint64 {
	if len(handle) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(handle[0:8])
}
