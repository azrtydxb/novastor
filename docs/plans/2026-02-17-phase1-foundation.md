# Phase 1: Foundation — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build NovaStor's foundation — Go module, chunk storage engine (with replication + erasure coding), disk management, node agent with gRPC chunk server, Raft-based metadata service, round-robin placement engine, CRD types, and operator skeleton.

**Architecture:** Shared chunk engine at the bottom, metadata service for coordination, node agent as the DaemonSet that manages disks and serves chunks over gRPC, placement engine for chunk-to-node mapping, operator for K8s reconciliation. All components communicate via gRPC. Data protection (replication + erasure coding) is built into the chunk engine from day one.

**Tech Stack:** Go 1.24, hashicorp/raft, BadgerDB v4, gRPC/protobuf, klauspost/reedsolomon, controller-runtime, zap logging, cobra CLI, prometheus metrics.

---

## Task 1: Initialize Go Module and Dependencies

**Files:**
- Create: `go.mod`
- Create: `go.sum`

**Step 1: Initialize Go module**

Run:
```bash
cd /Users/pascal/Documents/git/novastor
go mod init github.com/piwi3910/novastor
```

**Step 2: Add core dependencies**

Run:
```bash
go get go.uber.org/zap@latest
go get github.com/spf13/cobra@latest
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
go get github.com/hashicorp/raft@latest
go get github.com/hashicorp/raft-boltdb/v2@latest
go get github.com/dgraph-io/badger/v4@latest
go get github.com/klauspost/reedsolomon@latest
go get github.com/prometheus/client_golang@latest
go get github.com/google/uuid@latest
go get hash/crc32
go get sigs.k8s.io/controller-runtime@latest
go get k8s.io/apimachinery@latest
go get k8s.io/client-go@latest
```

**Step 3: Tidy the module**

Run: `go mod tidy`

**Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "[Chore] Initialize Go module with core dependencies

Set up github.com/piwi3910/novastor Go module with dependencies
for chunk engine, Raft consensus, gRPC, erasure coding, K8s operator,
structured logging, and CLI framework."
```

---

## Task 2: Chunk Engine — Core Types and Store Interface

**Files:**
- Create: `internal/chunk/chunk.go`
- Create: `internal/chunk/store.go`
- Create: `internal/chunk/chunk_test.go`

**Step 1: Write the test for chunk ID generation and validation**

Create `internal/chunk/chunk_test.go`:
```go
package chunk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestNewChunkID(t *testing.T) {
	data := []byte("hello world")
	id := NewChunkID(data)

	expected := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(expected[:])

	if string(id) != expectedHex {
		t.Errorf("NewChunkID = %s, want %s", id, expectedHex)
	}
}

func TestNewChunkID_DifferentData(t *testing.T) {
	id1 := NewChunkID([]byte("data1"))
	id2 := NewChunkID([]byte("data2"))

	if id1 == id2 {
		t.Error("different data should produce different chunk IDs")
	}
}

func TestNewChunkID_SameData(t *testing.T) {
	data := []byte("same data")
	id1 := NewChunkID(data)
	id2 := NewChunkID(data)

	if id1 != id2 {
		t.Error("same data should produce same chunk ID")
	}
}

func TestChunkChecksum(t *testing.T) {
	data := []byte("test data for checksum")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := c.VerifyChecksum(); err != nil {
		t.Errorf("valid checksum should verify: %v", err)
	}
}

func TestChunkChecksum_Corrupted(t *testing.T) {
	data := []byte("test data for checksum")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	// Corrupt the data
	c.Data = []byte("corrupted data")

	if err := c.VerifyChecksum(); err == nil {
		t.Error("corrupted data should fail checksum verification")
	}
}

func TestChunkSize(t *testing.T) {
	if ChunkSize != 4*1024*1024 {
		t.Errorf("ChunkSize = %d, want %d", ChunkSize, 4*1024*1024)
	}
}

func TestSplitData(t *testing.T) {
	tests := []struct {
		name       string
		dataLen    int
		wantChunks int
	}{
		{"empty", 0, 0},
		{"one byte", 1, 1},
		{"exact chunk", ChunkSize, 1},
		{"chunk plus one", ChunkSize + 1, 2},
		{"two chunks", ChunkSize * 2, 2},
		{"two and a half", ChunkSize*2 + ChunkSize/2, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataLen)
			for i := range data {
				data[i] = byte(i % 256)
			}

			chunks := SplitData(data)
			if len(chunks) != tt.wantChunks {
				t.Errorf("SplitData(%d bytes) = %d chunks, want %d", tt.dataLen, len(chunks), tt.wantChunks)
			}

			// Verify reassembly
			if tt.dataLen > 0 {
				var reassembled []byte
				for _, c := range chunks {
					reassembled = append(reassembled, c.Data...)
				}
				if !bytes.Equal(reassembled, data) {
					t.Error("reassembled data does not match original")
				}
			}
		})
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -v`
Expected: FAIL — types not defined

**Step 3: Implement chunk types**

Create `internal/chunk/chunk.go`:
```go
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
)

// ChunkSize is the fixed size of each chunk in bytes (4MB).
const ChunkSize = 4 * 1024 * 1024

// ChunkID uniquely identifies a chunk by its content hash (SHA-256).
type ChunkID string

// NewChunkID computes the content-addressed ID for the given data.
func NewChunkID(data []byte) ChunkID {
	h := sha256.Sum256(data)
	return ChunkID(hex.EncodeToString(h[:]))
}

// Chunk represents a single immutable data chunk.
type Chunk struct {
	ID       ChunkID
	Data     []byte
	Checksum uint32
}

// ComputeChecksum calculates the CRC-32C checksum for the chunk data.
func (c *Chunk) ComputeChecksum() uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(c.Data, table)
}

// VerifyChecksum checks that the stored checksum matches the data.
func (c *Chunk) VerifyChecksum() error {
	actual := c.ComputeChecksum()
	if actual != c.Checksum {
		return fmt.Errorf("checksum mismatch for chunk %s: stored=%d computed=%d", c.ID, c.Checksum, actual)
	}
	return nil
}

// SplitData splits arbitrary data into content-addressed chunks.
func SplitData(data []byte) []*Chunk {
	if len(data) == 0 {
		return nil
	}

	var chunks []*Chunk
	for offset := 0; offset < len(data); offset += ChunkSize {
		end := offset + ChunkSize
		if end > len(data) {
			end = len(data)
		}
		slice := data[offset:end]
		c := &Chunk{
			ID:   NewChunkID(slice),
			Data: slice,
		}
		c.Checksum = c.ComputeChecksum()
		chunks = append(chunks, c)
	}
	return chunks
}
```

Create `internal/chunk/store.go`:
```go
package chunk

import (
	"context"
)

// Store defines the interface for chunk storage operations.
type Store interface {
	// Put writes a chunk to the store. Returns error if chunk already exists with different data.
	Put(ctx context.Context, c *Chunk) error

	// Get retrieves a chunk by ID. Returns error if not found or checksum fails.
	Get(ctx context.Context, id ChunkID) (*Chunk, error)

	// Delete removes a chunk by ID. No error if chunk doesn't exist.
	Delete(ctx context.Context, id ChunkID) error

	// Has returns true if the chunk exists in the store.
	Has(ctx context.Context, id ChunkID) (bool, error)

	// List returns all chunk IDs in the store.
	List(ctx context.Context) ([]ChunkID, error)
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/chunk/
git commit -m "[Feature] Add chunk core types, ID generation, and checksum

Content-addressed 4MB chunks with SHA-256 IDs, CRC-32C checksums,
data splitting, and Store interface definition."
```

---

## Task 3: Chunk Engine — Local Disk Store

**Files:**
- Create: `internal/chunk/localstore.go`
- Create: `internal/chunk/localstore_test.go`

**Step 1: Write the test**

Create `internal/chunk/localstore_test.go`:
```go
package chunk

import (
	"context"
	"os"
	"testing"
)

func setupLocalStore(t *testing.T) (*LocalStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "novastor-chunk-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	store, err := NewLocalStore(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create local store: %v", err)
	}
	return store, func() { os.RemoveAll(dir) }
}

func TestLocalStore_PutGet(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("hello chunk world")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got.Data) != string(data) {
		t.Errorf("Get data = %q, want %q", got.Data, data)
	}
}

func TestLocalStore_GetNotFound(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()
	_, err := store.Get(ctx, ChunkID("nonexistent"))
	if err == nil {
		t.Error("Get should fail for nonexistent chunk")
	}
}

func TestLocalStore_Delete(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("delete me")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	_ = store.Put(ctx, c)

	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	has, _ := store.Has(ctx, c.ID)
	if has {
		t.Error("chunk should not exist after delete")
	}
}

func TestLocalStore_DeleteNonexistent(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.Delete(ctx, ChunkID("nonexistent")); err != nil {
		t.Errorf("Delete nonexistent should not error, got: %v", err)
	}
}

func TestLocalStore_Has(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("exists")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	has, _ := store.Has(ctx, c.ID)
	if has {
		t.Error("Has should return false before Put")
	}

	_ = store.Put(ctx, c)

	has, _ = store.Has(ctx, c.ID)
	if !has {
		t.Error("Has should return true after Put")
	}
}

func TestLocalStore_List(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()

	ids, _ := store.List(ctx)
	if len(ids) != 0 {
		t.Errorf("List empty store = %d, want 0", len(ids))
	}

	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		c := &Chunk{ID: NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		_ = store.Put(ctx, c)
	}

	ids, _ = store.List(ctx)
	if len(ids) != 3 {
		t.Errorf("List = %d, want 3", len(ids))
	}
}

func TestLocalStore_ChecksumVerifiedOnGet(t *testing.T) {
	store, cleanup := setupLocalStore(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("integrity check")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)

	// Corrupt the file on disk
	path := store.chunkPath(c.ID)
	_ = os.WriteFile(path, []byte("corrupted"), 0o644)

	_, err := store.Get(ctx, c.ID)
	if err == nil {
		t.Error("Get should fail on corrupted chunk")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -run TestLocalStore -v`
Expected: FAIL — LocalStore not defined

**Step 3: Implement LocalStore**

Create `internal/chunk/localstore.go`:
```go
package chunk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LocalStore stores chunks as files on a local filesystem.
// File format: [4 bytes checksum (big-endian)] [chunk data]
type LocalStore struct {
	dir string
}

// NewLocalStore creates a new local chunk store at the given directory.
func NewLocalStore(dir string) (*LocalStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating chunk store directory: %w", err)
	}
	return &LocalStore{dir: dir}, nil
}

func (s *LocalStore) chunkPath(id ChunkID) string {
	// Use first 2 chars as subdirectory for fan-out
	idStr := string(id)
	if len(idStr) < 2 {
		return filepath.Join(s.dir, idStr)
	}
	return filepath.Join(s.dir, idStr[:2], idStr)
}

func (s *LocalStore) Put(_ context.Context, c *Chunk) error {
	path := s.chunkPath(c.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating chunk subdirectory: %w", err)
	}

	// Encode: [4 bytes checksum] [data]
	buf := make([]byte, 4+len(c.Data))
	binary.BigEndian.PutUint32(buf[:4], c.Checksum)
	copy(buf[4:], c.Data)

	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return fmt.Errorf("writing chunk %s: %w", c.ID, err)
	}
	return nil
}

func (s *LocalStore) Get(_ context.Context, id ChunkID) (*Chunk, error) {
	path := s.chunkPath(id)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk %s not found", id)
		}
		return nil, fmt.Errorf("reading chunk %s: %w", id, err)
	}

	if len(raw) < 4 {
		return nil, fmt.Errorf("chunk %s: file too small", id)
	}

	checksum := binary.BigEndian.Uint32(raw[:4])
	data := raw[4:]

	c := &Chunk{
		ID:       id,
		Data:     data,
		Checksum: checksum,
	}

	if err := c.VerifyChecksum(); err != nil {
		return nil, fmt.Errorf("chunk %s integrity check failed: %w", id, err)
	}

	return c, nil
}

func (s *LocalStore) Delete(_ context.Context, id ChunkID) error {
	path := s.chunkPath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting chunk %s: %w", id, err)
	}
	return nil
}

func (s *LocalStore) Has(_ context.Context, id ChunkID) (bool, error) {
	path := s.chunkPath(id)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking chunk %s: %w", id, err)
	}
	return true, nil
}

func (s *LocalStore) List(_ context.Context) ([]ChunkID, error) {
	var ids []ChunkID

	err := filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Extract chunk ID from path: dir/ab/abcdef... -> abcdef...
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		// Remove subdirectory prefix
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) == 2 {
			ids = append(ids, ChunkID(parts[1]))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing chunks: %w", err)
	}
	return ids, nil
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -run TestLocalStore -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/chunk/localstore.go internal/chunk/localstore_test.go
git commit -m "[Feature] Add local filesystem chunk store

File-based chunk store with fan-out directories, CRC-32C integrity
verification on read, and content-addressed storage."
```

---

## Task 4: Chunk Engine — Replication Manager

**Files:**
- Create: `internal/chunk/replication.go`
- Create: `internal/chunk/replication_test.go`

**Step 1: Write the test**

Create `internal/chunk/replication_test.go`:
```go
package chunk

import (
	"context"
	"os"
	"testing"
)

func setupReplicationTest(t *testing.T, nodeCount int) ([]*LocalStore, *ReplicationManager, func()) {
	t.Helper()
	dirs := make([]string, nodeCount)
	stores := make([]*LocalStore, nodeCount)
	nodeStores := make(map[string]Store)

	for i := 0; i < nodeCount; i++ {
		dir, err := os.MkdirTemp("", "novastor-repl-test-*")
		if err != nil {
			t.Fatal(err)
		}
		dirs[i] = dir
		store, err := NewLocalStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
		nodeStores[fmt.Sprintf("node-%d", i)] = store
	}

	rm := NewReplicationManager(nodeStores)
	cleanup := func() {
		for _, d := range dirs {
			os.RemoveAll(d)
		}
	}
	return stores, rm, cleanup
}

func TestReplicationManager_WriteQuorum(t *testing.T) {
	_, rm, cleanup := setupReplicationTest(t, 3)
	defer cleanup()

	ctx := context.Background()
	data := []byte("replicated data")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	nodes := []string{"node-0", "node-1", "node-2"}
	err := rm.Replicate(ctx, c, nodes, 2) // quorum=2
	if err != nil {
		t.Fatalf("Replicate failed: %v", err)
	}

	// Verify chunk exists on all 3 nodes
	for _, node := range nodes {
		has, _ := rm.stores[node].Has(ctx, c.ID)
		if !has {
			t.Errorf("chunk should exist on %s", node)
		}
	}
}

func TestReplicationManager_ReadFromAny(t *testing.T) {
	_, rm, cleanup := setupReplicationTest(t, 3)
	defer cleanup()

	ctx := context.Background()
	data := []byte("read from any replica")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	nodes := []string{"node-0", "node-1", "node-2"}
	_ = rm.Replicate(ctx, c, nodes, 2)

	got, err := rm.ReadFromAny(ctx, c.ID, nodes)
	if err != nil {
		t.Fatalf("ReadFromAny failed: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", got.Data, data)
	}
}

func TestReplicationManager_ReadFromAny_SkipsCorrupted(t *testing.T) {
	stores, rm, cleanup := setupReplicationTest(t, 3)
	defer cleanup()

	ctx := context.Background()
	data := []byte("only one good copy")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	nodes := []string{"node-0", "node-1", "node-2"}
	_ = rm.Replicate(ctx, c, nodes, 2)

	// Corrupt data on nodes 0 and 1
	_ = os.WriteFile(stores[0].chunkPath(c.ID), []byte("bad"), 0o644)
	_ = os.WriteFile(stores[1].chunkPath(c.ID), []byte("bad"), 0o644)

	got, err := rm.ReadFromAny(ctx, c.ID, nodes)
	if err != nil {
		t.Fatalf("ReadFromAny should succeed from node-2: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("data mismatch from healthy replica")
	}
}

func TestReplicationManager_ReadFromAny_AllCorrupted(t *testing.T) {
	stores, rm, cleanup := setupReplicationTest(t, 2)
	defer cleanup()

	ctx := context.Background()
	data := []byte("all bad")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	nodes := []string{"node-0", "node-1"}
	_ = rm.Replicate(ctx, c, nodes, 2)

	_ = os.WriteFile(stores[0].chunkPath(c.ID), []byte("bad"), 0o644)
	_ = os.WriteFile(stores[1].chunkPath(c.ID), []byte("bad"), 0o644)

	_, err := rm.ReadFromAny(ctx, c.ID, nodes)
	if err == nil {
		t.Error("ReadFromAny should fail when all replicas are corrupted")
	}
}

func TestReplicationManager_QuorumNotMet(t *testing.T) {
	_, rm, cleanup := setupReplicationTest(t, 1)
	defer cleanup()

	ctx := context.Background()
	data := []byte("need more nodes")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	// Only 1 node but quorum requires 2
	err := rm.Replicate(ctx, c, []string{"node-0"}, 2)
	if err == nil {
		t.Error("Replicate should fail when quorum cannot be met")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -run TestReplication -v`
Expected: FAIL — ReplicationManager not defined

**Step 3: Implement ReplicationManager**

Create `internal/chunk/replication.go`:
```go
package chunk

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ReplicationManager handles synchronous N-way chunk replication.
type ReplicationManager struct {
	stores map[string]Store
}

// NewReplicationManager creates a replication manager with the given node stores.
func NewReplicationManager(stores map[string]Store) *ReplicationManager {
	return &ReplicationManager{stores: stores}
}

// Replicate writes a chunk to the specified nodes, requiring writeQuorum successes.
func (rm *ReplicationManager) Replicate(ctx context.Context, c *Chunk, nodes []string, writeQuorum int) error {
	if len(nodes) < writeQuorum {
		return fmt.Errorf("not enough nodes (%d) to meet write quorum (%d)", len(nodes), writeQuorum)
	}

	var (
		wg       sync.WaitGroup
		successes atomic.Int32
		lastErr  atomic.Value
	)

	for _, node := range nodes {
		store, ok := rm.stores[node]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(s Store, n string) {
			defer wg.Done()
			if err := s.Put(ctx, c); err != nil {
				lastErr.Store(fmt.Errorf("replication to %s failed: %w", n, err))
				return
			}
			successes.Add(1)
		}(store, node)
	}

	wg.Wait()

	if int(successes.Load()) < writeQuorum {
		errVal := lastErr.Load()
		if errVal != nil {
			return fmt.Errorf("write quorum not met (%d/%d): %w", successes.Load(), writeQuorum, errVal.(error))
		}
		return fmt.Errorf("write quorum not met (%d/%d)", successes.Load(), writeQuorum)
	}

	return nil
}

// ReadFromAny reads a chunk from the first healthy replica. Skips corrupted replicas.
func (rm *ReplicationManager) ReadFromAny(ctx context.Context, id ChunkID, nodes []string) (*Chunk, error) {
	var lastErr error

	for _, node := range nodes {
		store, ok := rm.stores[node]
		if !ok {
			continue
		}
		c, err := store.Get(ctx, id)
		if err != nil {
			lastErr = fmt.Errorf("node %s: %w", node, err)
			continue
		}
		return c, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all replicas failed for chunk %s: %w", id, lastErr)
	}
	return nil, fmt.Errorf("no nodes available for chunk %s", id)
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -run TestReplication -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/chunk/replication.go internal/chunk/replication_test.go
git commit -m "[Feature] Add synchronous N-way chunk replication

Write quorum enforcement, parallel replication to N nodes,
read-from-any with automatic skip of corrupted replicas."
```

---

## Task 5: Chunk Engine — Erasure Coding

**Files:**
- Create: `internal/chunk/erasure.go`
- Create: `internal/chunk/erasure_test.go`

**Step 1: Write the test**

Create `internal/chunk/erasure_test.go`:
```go
package chunk

import (
	"bytes"
	"testing"
)

func TestErasureCoder_EncodeDecodeNoFailure(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatalf("NewErasureCoder failed: %v", err)
	}

	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if len(shards) != 6 {
		t.Fatalf("expected 6 shards, got %d", len(shards))
	}

	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Error("decoded data does not match original")
	}
}

func TestErasureCoder_RecoverFromTwoShardLoss(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 251) // prime to avoid patterns
	}

	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Lose 2 shards (up to parity count)
	shards[0] = nil
	shards[3] = nil

	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode with 2 lost shards failed: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Error("recovered data does not match original after 2 shard loss")
	}
}

func TestErasureCoder_TooManyLostShards(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, ChunkSize)
	shards, _ := ec.Encode(data)

	// Lose 3 shards (more than parity count of 2)
	shards[0] = nil
	shards[1] = nil
	shards[2] = nil

	_, err = ec.Decode(shards)
	if err == nil {
		t.Error("Decode should fail with 3 lost shards (parity=2)")
	}
}

func TestErasureCoder_SmallData(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("small")
	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode small data failed: %v", err)
	}

	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode small data failed: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded = %q, want %q", decoded, data)
	}
}

func TestNewErasureCoder_InvalidParams(t *testing.T) {
	tests := []struct {
		name   string
		data   int
		parity int
	}{
		{"zero data", 0, 2},
		{"zero parity", 4, 0},
		{"negative data", -1, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewErasureCoder(tt.data, tt.parity)
			if err == nil {
				t.Error("expected error for invalid params")
			}
		})
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -run TestErasure -v`
Expected: FAIL — ErasureCoder not defined

**Step 3: Implement ErasureCoder**

Create `internal/chunk/erasure.go`:
```go
package chunk

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// ErasureCoder encodes and decodes data using Reed-Solomon erasure coding.
type ErasureCoder struct {
	dataShards   int
	parityShards int
	encoder      reedsolomon.Encoder
}

// NewErasureCoder creates a new erasure coder with the given data and parity shard counts.
func NewErasureCoder(dataShards, parityShards int) (*ErasureCoder, error) {
	if dataShards <= 0 || parityShards <= 0 {
		return nil, fmt.Errorf("data shards (%d) and parity shards (%d) must be positive", dataShards, parityShards)
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("creating reed-solomon encoder: %w", err)
	}
	return &ErasureCoder{
		dataShards:   dataShards,
		parityShards: parityShards,
		encoder:      enc,
	}, nil
}

// Encode splits data into data+parity shards.
// Format: [8 bytes original length] [data padded to shard alignment] -> sharded
func (ec *ErasureCoder) Encode(data []byte) ([][]byte, error) {
	// Prepend original data length for accurate reconstruction
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[:8], uint64(len(data)))
	copy(buf[8:], data)

	shards, err := ec.encoder.Split(buf)
	if err != nil {
		return nil, fmt.Errorf("splitting data into shards: %w", err)
	}

	if err := ec.encoder.Encode(shards); err != nil {
		return nil, fmt.Errorf("encoding parity shards: %w", err)
	}

	return shards, nil
}

// Decode reconstructs the original data from shards. Nil shards are treated as lost.
func (ec *ErasureCoder) Decode(shards [][]byte) ([]byte, error) {
	if err := ec.encoder.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstructing shards: %w", err)
	}

	var buf bytes.Buffer
	if err := ec.encoder.Join(&buf, shards, 0); err != nil {
		return nil, fmt.Errorf("joining shards: %w", err)
	}

	joined := buf.Bytes()
	if len(joined) < 8 {
		return nil, fmt.Errorf("reconstructed data too small")
	}

	origLen := binary.BigEndian.Uint64(joined[:8])
	payload := joined[8:]

	if int(origLen) > len(payload) {
		return nil, fmt.Errorf("original length %d exceeds available data %d", origLen, len(payload))
	}

	return payload[:origLen], nil
}

// ShardCount returns the total number of shards (data + parity).
func (ec *ErasureCoder) ShardCount() int {
	return ec.dataShards + ec.parityShards
}

// DataShards returns the number of data shards.
func (ec *ErasureCoder) DataShards() int {
	return ec.dataShards
}

// ParityShards returns the number of parity shards.
func (ec *ErasureCoder) ParityShards() int {
	return ec.parityShards
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/chunk/ -run TestErasure -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/chunk/erasure.go internal/chunk/erasure_test.go
git commit -m "[Feature] Add Reed-Solomon erasure coding

4+2 default scheme, encode/decode with automatic reconstruction
of lost shards, original data length preserved for exact recovery."
```

---

## Task 6: Placement Engine — Round-Robin

**Files:**
- Create: `internal/placement/placement.go`
- Create: `internal/placement/placement_test.go`

**Step 1: Write the test**

Create `internal/placement/placement_test.go`:
```go
package placement

import (
	"testing"
)

func TestRoundRobin_PlaceReplicated(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2", "node-3"}
	rr := NewRoundRobin(nodes)

	targets := rr.Place(3)
	if len(targets) != 3 {
		t.Fatalf("Place(3) = %d nodes, want 3", len(targets))
	}

	// All targets should be unique
	seen := make(map[string]bool)
	for _, n := range targets {
		if seen[n] {
			t.Errorf("duplicate node: %s", n)
		}
		seen[n] = true
	}
}

func TestRoundRobin_PlaceDistributes(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2"}
	rr := NewRoundRobin(nodes)

	counts := make(map[string]int)
	for i := 0; i < 30; i++ {
		targets := rr.Place(1)
		counts[targets[0]]++
	}

	// Each node should get ~10 placements (round-robin is deterministic)
	for _, node := range nodes {
		if counts[node] != 10 {
			t.Errorf("node %s got %d placements, want 10", node, counts[node])
		}
	}
}

func TestRoundRobin_PlaceMoreThanAvailable(t *testing.T) {
	nodes := []string{"node-0", "node-1"}
	rr := NewRoundRobin(nodes)

	targets := rr.Place(5)
	if len(targets) != 2 {
		t.Errorf("Place(5) with 2 nodes = %d, want 2 (capped)", len(targets))
	}
}

func TestRoundRobin_PlaceZero(t *testing.T) {
	nodes := []string{"node-0"}
	rr := NewRoundRobin(nodes)

	targets := rr.Place(0)
	if len(targets) != 0 {
		t.Errorf("Place(0) = %d, want 0", len(targets))
	}
}

func TestRoundRobin_NoNodes(t *testing.T) {
	rr := NewRoundRobin(nil)
	targets := rr.Place(3)
	if len(targets) != 0 {
		t.Errorf("Place with no nodes = %d, want 0", len(targets))
	}
}

func TestRoundRobin_AddRemoveNode(t *testing.T) {
	rr := NewRoundRobin([]string{"node-0", "node-1"})

	rr.AddNode("node-2")
	targets := rr.Place(3)
	if len(targets) != 3 {
		t.Fatalf("Place(3) after add = %d, want 3", len(targets))
	}

	rr.RemoveNode("node-1")
	targets = rr.Place(3)
	if len(targets) != 2 {
		t.Errorf("Place(3) after remove = %d, want 2", len(targets))
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/placement/ -v`
Expected: FAIL — package not found

**Step 3: Implement RoundRobin placement**

Create `internal/placement/placement.go`:
```go
package placement

import (
	"sync"
)

// Placer determines which nodes should store a given number of replicas/shards.
type Placer interface {
	// Place returns up to count unique node IDs for storing data.
	Place(count int) []string

	// AddNode registers a new storage node.
	AddNode(nodeID string)

	// RemoveNode unregisters a storage node.
	RemoveNode(nodeID string)
}

// RoundRobin distributes chunks across nodes in a round-robin fashion.
type RoundRobin struct {
	mu    sync.Mutex
	nodes []string
	index int
}

// NewRoundRobin creates a new round-robin placer with the given initial nodes.
func NewRoundRobin(nodes []string) *RoundRobin {
	cp := make([]string, len(nodes))
	copy(cp, nodes)
	return &RoundRobin{nodes: cp}
}

func (rr *RoundRobin) Place(count int) []string {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if len(rr.nodes) == 0 || count <= 0 {
		return nil
	}

	// Cap at available nodes
	if count > len(rr.nodes) {
		count = len(rr.nodes)
	}

	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = rr.nodes[(rr.index+i)%len(rr.nodes)]
	}
	rr.index = (rr.index + count) % len(rr.nodes)

	return result
}

func (rr *RoundRobin) AddNode(nodeID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	// Avoid duplicates
	for _, n := range rr.nodes {
		if n == nodeID {
			return
		}
	}
	rr.nodes = append(rr.nodes, nodeID)
}

func (rr *RoundRobin) RemoveNode(nodeID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	for i, n := range rr.nodes {
		if n == nodeID {
			rr.nodes = append(rr.nodes[:i], rr.nodes[i+1:]...)
			if rr.index >= len(rr.nodes) && len(rr.nodes) > 0 {
				rr.index = rr.index % len(rr.nodes)
			}
			return
		}
	}
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/placement/ -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/placement/
git commit -m "[Feature] Add round-robin placement engine

Distributes chunks evenly across nodes with thread-safe
add/remove node operations. Capped to available node count."
```

---

## Task 7: Protobuf Definitions for Chunk Service

**Files:**
- Create: `api/proto/chunk.proto`

**Step 1: Create proto directory and definition**

Create `api/proto/chunk.proto`:
```protobuf
syntax = "proto3";

package novastor.chunk.v1;

option go_package = "github.com/piwi3910/novastor/internal/proto/gen/chunkpb";

service ChunkService {
  rpc PutChunk(PutChunkRequest) returns (PutChunkResponse);
  rpc GetChunk(GetChunkRequest) returns (GetChunkResponse);
  rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);
  rpc HasChunk(HasChunkRequest) returns (HasChunkResponse);
  rpc ListChunks(ListChunksRequest) returns (ListChunksResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

message PutChunkRequest {
  string chunk_id = 1;
  bytes data = 2;
  uint32 checksum = 3;
}

message PutChunkResponse {}

message GetChunkRequest {
  string chunk_id = 1;
}

message GetChunkResponse {
  string chunk_id = 1;
  bytes data = 2;
  uint32 checksum = 3;
}

message DeleteChunkRequest {
  string chunk_id = 1;
}

message DeleteChunkResponse {}

message HasChunkRequest {
  string chunk_id = 1;
}

message HasChunkResponse {
  bool exists = 1;
}

message ListChunksRequest {}

message ListChunksResponse {
  repeated string chunk_ids = 1;
}

message HealthCheckRequest {}

message HealthCheckResponse {
  string node_id = 1;
  string status = 2;
  uint64 total_bytes = 3;
  uint64 used_bytes = 4;
  uint64 chunk_count = 5;
}
```

**Step 2: Generate Go code**

Run: `cd /Users/pascal/Documents/git/novastor && make generate-proto`

If `protoc` is not installed, install it first:
```bash
brew install protobuf
```

Then run: `make generate-proto`
Expected: Files generated in `internal/proto/gen/chunkpb/`

**Step 3: Commit**

```bash
git add api/proto/chunk.proto internal/proto/gen/
git commit -m "[Feature] Add chunk service protobuf definition

gRPC service for PutChunk, GetChunk, DeleteChunk, HasChunk,
ListChunks, and HealthCheck operations."
```

---

## Task 8: Node Agent — gRPC Chunk Server

**Files:**
- Create: `internal/agent/server.go`
- Create: `internal/agent/server_test.go`

**Step 1: Write the test**

Create `internal/agent/server_test.go`:
```go
package agent

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/piwi3910/novastor/internal/chunk"
	pb "github.com/piwi3910/novastor/internal/proto/gen/chunkpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupGRPCServer(t *testing.T) (pb.ChunkServiceClient, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "novastor-agent-test-*")
	if err != nil {
		t.Fatal(err)
	}

	store, err := chunk.NewLocalStore(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	chunkServer := NewChunkServer("test-node", store)
	pb.RegisterChunkServiceServer(srv, chunkServer)

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	client := pb.NewChunkServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		os.RemoveAll(dir)
	}
	return client, cleanup
}

func TestChunkServer_PutGet(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("grpc test data")
	c := &chunk.Chunk{ID: chunk.NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	_, err := client.PutChunk(ctx, &pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum,
	})
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	resp, err := client.GetChunk(ctx, &pb.GetChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if string(resp.Data) != string(data) {
		t.Errorf("data = %q, want %q", resp.Data, data)
	}
}

func TestChunkServer_Has(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	resp, err := client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: "nonexistent"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Exists {
		t.Error("HasChunk should return false for nonexistent")
	}

	data := []byte("exists")
	c := &chunk.Chunk{ID: chunk.NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_, _ = client.PutChunk(ctx, &pb.PutChunkRequest{
		ChunkId: string(c.ID), Data: c.Data, Checksum: c.Checksum,
	})

	resp, err = client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Exists {
		t.Error("HasChunk should return true after PutChunk")
	}
}

func TestChunkServer_Delete(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("deleteme")
	c := &chunk.Chunk{ID: chunk.NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_, _ = client.PutChunk(ctx, &pb.PutChunkRequest{
		ChunkId: string(c.ID), Data: c.Data, Checksum: c.Checksum,
	})

	_, err := client.DeleteChunk(ctx, &pb.DeleteChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("DeleteChunk failed: %v", err)
	}

	resp, _ := client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: string(c.ID)})
	if resp.Exists {
		t.Error("chunk should not exist after delete")
	}
}

func TestChunkServer_ListChunks(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 10), byte(i + 20)}
		c := &chunk.Chunk{ID: chunk.NewChunkID(data), Data: data}
		c.Checksum = c.ComputeChecksum()
		_, _ = client.PutChunk(ctx, &pb.PutChunkRequest{
			ChunkId: string(c.ID), Data: c.Data, Checksum: c.Checksum,
		})
	}

	resp, err := client.ListChunks(ctx, &pb.ListChunksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ChunkIds) != 3 {
		t.Errorf("ListChunks = %d, want 3", len(resp.ChunkIds))
	}
}

func TestChunkServer_HealthCheck(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	resp, err := client.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeId != "test-node" {
		t.Errorf("node_id = %q, want %q", resp.NodeId, "test-node")
	}
	if resp.Status != "healthy" {
		t.Errorf("status = %q, want %q", resp.Status, "healthy")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/agent/ -v`
Expected: FAIL — ChunkServer not defined

**Step 3: Implement ChunkServer**

Create `internal/agent/server.go`:
```go
package agent

import (
	"context"

	"github.com/piwi3910/novastor/internal/chunk"
	pb "github.com/piwi3910/novastor/internal/proto/gen/chunkpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChunkServer implements the gRPC ChunkService.
type ChunkServer struct {
	pb.UnimplementedChunkServiceServer
	nodeID string
	store  chunk.Store
}

// NewChunkServer creates a new chunk gRPC server.
func NewChunkServer(nodeID string, store chunk.Store) *ChunkServer {
	return &ChunkServer{
		nodeID: nodeID,
		store:  store,
	}
}

func (s *ChunkServer) PutChunk(ctx context.Context, req *pb.PutChunkRequest) (*pb.PutChunkResponse, error) {
	c := &chunk.Chunk{
		ID:       chunk.ChunkID(req.ChunkId),
		Data:     req.Data,
		Checksum: req.Checksum,
	}

	if err := c.VerifyChecksum(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "checksum verification failed: %v", err)
	}

	if err := s.store.Put(ctx, c); err != nil {
		return nil, status.Errorf(codes.Internal, "storing chunk: %v", err)
	}

	return &pb.PutChunkResponse{}, nil
}

func (s *ChunkServer) GetChunk(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	c, err := s.store.Get(ctx, chunk.ChunkID(req.ChunkId))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "chunk not found: %v", err)
	}

	return &pb.GetChunkResponse{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum,
	}, nil
}

func (s *ChunkServer) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	if err := s.store.Delete(ctx, chunk.ChunkID(req.ChunkId)); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting chunk: %v", err)
	}
	return &pb.DeleteChunkResponse{}, nil
}

func (s *ChunkServer) HasChunk(ctx context.Context, req *pb.HasChunkRequest) (*pb.HasChunkResponse, error) {
	has, err := s.store.Has(ctx, chunk.ChunkID(req.ChunkId))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "checking chunk: %v", err)
	}
	return &pb.HasChunkResponse{Exists: has}, nil
}

func (s *ChunkServer) ListChunks(ctx context.Context, _ *pb.ListChunksRequest) (*pb.ListChunksResponse, error) {
	ids, err := s.store.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing chunks: %v", err)
	}

	strIDs := make([]string, len(ids))
	for i, id := range ids {
		strIDs[i] = string(id)
	}
	return &pb.ListChunksResponse{ChunkIds: strIDs}, nil
}

func (s *ChunkServer) HealthCheck(_ context.Context, _ *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		NodeId: s.nodeID,
		Status: "healthy",
	}, nil
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/agent/ -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/agent/ api/proto/
git commit -m "[Feature] Add gRPC chunk server for node agent

Implements ChunkService with Put, Get, Delete, Has, List, and
HealthCheck operations. Verifies checksums on write."
```

---

## Task 9: Disk Discovery

**Files:**
- Create: `internal/disk/discovery.go`
- Create: `internal/disk/discovery_test.go`

**Step 1: Write the test**

Create `internal/disk/discovery_test.go`:
```go
package disk

import (
	"testing"
)

func TestDeviceInfo_String(t *testing.T) {
	d := DeviceInfo{
		Path:       "/dev/sda",
		SizeBytes:  1024 * 1024 * 1024 * 100, // 100GB
		DeviceType: TypeSSD,
		Model:      "Samsung 980 Pro",
	}

	s := d.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestDeviceType_String(t *testing.T) {
	tests := []struct {
		dt   DeviceType
		want string
	}{
		{TypeNVMe, "nvme"},
		{TypeSSD, "ssd"},
		{TypeHDD, "hdd"},
		{TypeUnknown, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.dt.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFilterDevices(t *testing.T) {
	devices := []DeviceInfo{
		{Path: "/dev/nvme0n1", SizeBytes: 500e9, DeviceType: TypeNVMe},
		{Path: "/dev/sda", SizeBytes: 100e9, DeviceType: TypeSSD},
		{Path: "/dev/sdb", SizeBytes: 1e12, DeviceType: TypeHDD},
		{Path: "/dev/nvme1n1", SizeBytes: 250e9, DeviceType: TypeNVMe},
	}

	t.Run("filter by type", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{DeviceType: TypeNVMe})
		if len(filtered) != 2 {
			t.Errorf("expected 2 NVMe devices, got %d", len(filtered))
		}
	})

	t.Run("filter by min size", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{MinSizeBytes: 200e9})
		if len(filtered) != 3 {
			t.Errorf("expected 3 devices >= 200GB, got %d", len(filtered))
		}
	})

	t.Run("filter by type and size", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{DeviceType: TypeNVMe, MinSizeBytes: 400e9})
		if len(filtered) != 1 {
			t.Errorf("expected 1 NVMe >= 400GB, got %d", len(filtered))
		}
	})

	t.Run("no filter", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{})
		if len(filtered) != 4 {
			t.Errorf("expected all 4 devices, got %d", len(filtered))
		}
	})
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/disk/ -v`
Expected: FAIL — types not defined

**Step 3: Implement disk discovery types**

Create `internal/disk/discovery.go`:
```go
package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DeviceType represents the type of storage device.
type DeviceType int

const (
	TypeUnknown DeviceType = iota
	TypeNVMe
	TypeSSD
	TypeHDD
)

func (dt DeviceType) String() string {
	switch dt {
	case TypeNVMe:
		return "nvme"
	case TypeSSD:
		return "ssd"
	case TypeHDD:
		return "hdd"
	default:
		return "unknown"
	}
}

// DeviceInfo describes a discovered storage device.
type DeviceInfo struct {
	Path       string
	SizeBytes  uint64
	DeviceType DeviceType
	Model      string
	Serial     string
	Rotational bool
}

func (d DeviceInfo) String() string {
	return fmt.Sprintf("%s (%s, %.1f GB, %s)", d.Path, d.DeviceType, float64(d.SizeBytes)/1e9, d.Model)
}

// FilterOptions specifies criteria for filtering discovered devices.
type FilterOptions struct {
	DeviceType   DeviceType
	MinSizeBytes uint64
}

// FilterDevices returns devices matching the given filter options.
func FilterDevices(devices []DeviceInfo, opts FilterOptions) []DeviceInfo {
	var result []DeviceInfo
	for _, d := range devices {
		if opts.DeviceType != TypeUnknown && d.DeviceType != opts.DeviceType {
			continue
		}
		if opts.MinSizeBytes > 0 && d.SizeBytes < opts.MinSizeBytes {
			continue
		}
		result = append(result, d)
	}
	return result
}

// DiscoverDevices scans the system for available block devices.
// This reads from /sys/block/ on Linux.
func DiscoverDevices() ([]DeviceInfo, error) {
	sysBlock := "/sys/block"
	entries, err := os.ReadDir(sysBlock)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", sysBlock, err)
	}

	var devices []DeviceInfo
	for _, entry := range entries {
		name := entry.Name()

		// Skip loop, ram, and dm devices
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") || strings.HasPrefix(name, "dm-") {
			continue
		}

		dev := DeviceInfo{
			Path: filepath.Join("/dev", name),
		}

		// Read size (in 512-byte sectors)
		sizeData, err := os.ReadFile(filepath.Join(sysBlock, name, "size"))
		if err == nil {
			var sectors uint64
			fmt.Sscanf(strings.TrimSpace(string(sizeData)), "%d", &sectors)
			dev.SizeBytes = sectors * 512
		}

		// Determine device type
		if strings.HasPrefix(name, "nvme") {
			dev.DeviceType = TypeNVMe
		} else {
			rotData, err := os.ReadFile(filepath.Join(sysBlock, name, "queue", "rotational"))
			if err == nil {
				if strings.TrimSpace(string(rotData)) == "1" {
					dev.DeviceType = TypeHDD
					dev.Rotational = true
				} else {
					dev.DeviceType = TypeSSD
				}
			}
		}

		// Read model
		modelData, err := os.ReadFile(filepath.Join(sysBlock, name, "device", "model"))
		if err == nil {
			dev.Model = strings.TrimSpace(string(modelData))
		}

		devices = append(devices, dev)
	}

	return devices, nil
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/disk/ -v -race`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/disk/
git commit -m "[Feature] Add disk discovery and filtering

Device type detection (NVMe/SSD/HDD), size reading from sysfs,
model detection, and filter by type and minimum size."
```

---

## Task 10: CRD Type Definitions

**Files:**
- Create: `api/v1alpha1/types.go`
- Create: `api/v1alpha1/groupversion_info.go`
- Create: `api/v1alpha1/zz_generated.deepcopy.go` (generated)

**Step 1: Create the GroupVersion info**

Create `api/v1alpha1/groupversion_info.go`:
```go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion = schema.GroupVersion{Group: "novastor.io", Version: "v1alpha1"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&StoragePool{},
		&StoragePoolList{},
		&BlockVolume{},
		&BlockVolumeList{},
		&SharedFilesystem{},
		&SharedFilesystemList{},
		&ObjectStore{},
		&ObjectStoreList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
```

**Step 2: Create the CRD types**

Create `api/v1alpha1/types.go`:
```go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.dataProtection.mode`
// +kubebuilder:printcolumn:name="Nodes",type=integer,JSONPath=`.status.nodeCount`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.totalCapacity`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// StoragePool defines a set of storage nodes and their data protection policy.
type StoragePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StoragePoolSpec   `json:"spec,omitempty"`
	Status StoragePoolStatus `json:"status,omitempty"`
}

type StoragePoolSpec struct {
	// NodeSelector selects which nodes belong to this pool.
	NodeSelector *metav1.LabelSelector `json:"nodeSelector,omitempty"`

	// DeviceFilter specifies which devices on selected nodes to use.
	DeviceFilter *DeviceFilter `json:"deviceFilter,omitempty"`

	// DataProtection configures how data is protected in this pool.
	DataProtection DataProtectionSpec `json:"dataProtection"`
}

type DeviceFilter struct {
	// Type of device to include: nvme, ssd, hdd.
	// +kubebuilder:validation:Enum=nvme;ssd;hdd
	Type string `json:"type,omitempty"`

	// MinSize is the minimum device size to include (e.g., "100Gi").
	MinSize string `json:"minSize,omitempty"`
}

type DataProtectionSpec struct {
	// Mode is the data protection strategy: replication or erasureCoding.
	// +kubebuilder:validation:Enum=replication;erasureCoding
	Mode string `json:"mode"`

	// Replication settings (used when mode=replication).
	Replication *ReplicationSpec `json:"replication,omitempty"`

	// ErasureCoding settings (used when mode=erasureCoding).
	ErasureCoding *ErasureCodingSpec `json:"erasureCoding,omitempty"`
}

type ReplicationSpec struct {
	// Factor is the number of replicas (default 3).
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Factor int `json:"factor"`

	// WriteQuorum is the number of replicas that must confirm a write (default: majority).
	// +kubebuilder:validation:Minimum=1
	WriteQuorum int `json:"writeQuorum,omitempty"`
}

type ErasureCodingSpec struct {
	// DataShards is the number of data shards (default 4).
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=2
	DataShards int `json:"dataShards"`

	// ParityShards is the number of parity shards (default 2).
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	ParityShards int `json:"parityShards"`
}

type StoragePoolStatus struct {
	// Phase is the current state of the pool.
	Phase string `json:"phase,omitempty"`

	// NodeCount is the number of active nodes in the pool.
	NodeCount int `json:"nodeCount,omitempty"`

	// TotalCapacity is the aggregate capacity across all nodes.
	TotalCapacity string `json:"totalCapacity,omitempty"`

	// UsedCapacity is the aggregate used capacity.
	UsedCapacity string `json:"usedCapacity,omitempty"`

	// Conditions represent the latest observations of the pool's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type StoragePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StoragePool `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="Access",type=string,JSONPath=`.spec.accessMode`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// BlockVolume represents a block storage volume.
type BlockVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BlockVolumeSpec   `json:"spec,omitempty"`
	Status BlockVolumeStatus `json:"status,omitempty"`
}

type BlockVolumeSpec struct {
	// Pool references the StoragePool to use.
	Pool string `json:"pool"`

	// Size is the requested volume size (e.g., "100Gi").
	Size string `json:"size"`

	// AccessMode is the volume access mode.
	// +kubebuilder:validation:Enum=ReadWriteOnce;ReadOnlyMany
	AccessMode string `json:"accessMode"`
}

type BlockVolumeStatus struct {
	Phase      string             `json:"phase,omitempty"`
	NodeID     string             `json:"nodeID,omitempty"`
	ChunkCount int                `json:"chunkCount,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type BlockVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BlockVolume `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.spec.capacity`
// +kubebuilder:printcolumn:name="Protocol",type=string,JSONPath=`.spec.export.protocol`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// SharedFilesystem represents a shared filesystem.
type SharedFilesystem struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SharedFilesystemSpec   `json:"spec,omitempty"`
	Status SharedFilesystemStatus `json:"status,omitempty"`
}

type SharedFilesystemSpec struct {
	Pool     string `json:"pool"`
	Capacity string `json:"capacity"`

	// +kubebuilder:validation:Enum=ReadWriteMany;ReadOnlyMany
	AccessMode string       `json:"accessMode"`
	Export     *ExportSpec  `json:"export,omitempty"`
}

type ExportSpec struct {
	// +kubebuilder:validation:Enum=nfs
	Protocol string `json:"protocol"`
}

type SharedFilesystemStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Endpoint   string             `json:"endpoint,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type SharedFilesystemList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SharedFilesystem `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Port",type=integer,JSONPath=`.spec.endpoint.service.port`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// ObjectStore represents an S3-compatible object store.
type ObjectStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjectStoreSpec   `json:"spec,omitempty"`
	Status ObjectStoreStatus `json:"status,omitempty"`
}

type ObjectStoreSpec struct {
	Pool         string              `json:"pool"`
	Endpoint     ObjectEndpointSpec  `json:"endpoint"`
	BucketPolicy *BucketPolicySpec   `json:"bucketPolicy,omitempty"`
}

type ObjectEndpointSpec struct {
	Service ObjectServiceSpec `json:"service"`
}

type ObjectServiceSpec struct {
	Port int32 `json:"port"`
}

type BucketPolicySpec struct {
	MaxBuckets int    `json:"maxBuckets,omitempty"`
	// +kubebuilder:validation:Enum=enabled;disabled;suspended
	Versioning string `json:"versioning,omitempty"`
}

type ObjectStoreStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Endpoint   string             `json:"endpoint,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type ObjectStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ObjectStore `json:"items"`
}
```

**Step 3: Generate deepcopy and CRD manifests**

Run:
```bash
cd /Users/pascal/Documents/git/novastor
make generate
make manifests
```

Expected: `api/v1alpha1/zz_generated.deepcopy.go` created, CRD YAML files in `config/crd/`

**Step 4: Verify it compiles**

Run: `cd /Users/pascal/Documents/git/novastor && go build ./api/...`
Expected: Success

**Step 5: Commit**

```bash
git add api/v1alpha1/ config/crd/
git commit -m "[Feature] Add CRD types for StoragePool, BlockVolume, SharedFilesystem, ObjectStore

Four CRDs with data protection config (replication + erasure coding),
kubebuilder markers for validation and printer columns, and deepcopy generation."
```

---

## Task 11: Metadata Service — Raft-Based Store

**Files:**
- Create: `internal/metadata/store.go`
- Create: `internal/metadata/fsm.go`
- Create: `internal/metadata/store_test.go`

**Step 1: Write the test**

Create `internal/metadata/store_test.go`:
```go
package metadata

import (
	"context"
	"os"
	"testing"
	"time"
)

func setupTestStore(t *testing.T) (*RaftStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "novastor-meta-test-*")
	if err != nil {
		t.Fatal(err)
	}

	store, err := NewRaftStore("test-node", dir, "127.0.0.1:0", true)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewRaftStore failed: %v", err)
	}

	// Wait for leader election
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsLeader() {
		store.Close()
		os.RemoveAll(dir)
		t.Fatal("node did not become leader")
	}

	return store, func() {
		store.Close()
		os.RemoveAll(dir)
	}
}

func TestRaftStore_PutGetVolumeMeta(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	meta := &VolumeMeta{
		VolumeID: "vol-1",
		Pool:     "fast-pool",
		SizeBytes: 1024 * 1024 * 1024,
		ChunkIDs: []string{"chunk-a", "chunk-b"},
	}

	if err := store.PutVolumeMeta(ctx, meta); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	got, err := store.GetVolumeMeta(ctx, "vol-1")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}

	if got.VolumeID != "vol-1" || got.Pool != "fast-pool" || len(got.ChunkIDs) != 2 {
		t.Errorf("unexpected volume meta: %+v", got)
	}
}

func TestRaftStore_GetVolumeMetaNotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	_, err := store.GetVolumeMeta(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent volume")
	}
}

func TestRaftStore_DeleteVolumeMeta(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	meta := &VolumeMeta{VolumeID: "vol-del", Pool: "pool"}
	_ = store.PutVolumeMeta(ctx, meta)

	if err := store.DeleteVolumeMeta(ctx, "vol-del"); err != nil {
		t.Fatalf("DeleteVolumeMeta failed: %v", err)
	}

	_, err := store.GetVolumeMeta(ctx, "vol-del")
	if err == nil {
		t.Error("volume should not exist after delete")
	}
}

func TestRaftStore_PutGetPlacementMap(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	pm := &PlacementMap{
		ChunkID: "chunk-1",
		Nodes:   []string{"node-a", "node-b", "node-c"},
	}

	if err := store.PutPlacementMap(ctx, pm); err != nil {
		t.Fatalf("PutPlacementMap failed: %v", err)
	}

	got, err := store.GetPlacementMap(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("GetPlacementMap failed: %v", err)
	}

	if len(got.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(got.Nodes))
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/metadata/ -v`
Expected: FAIL — types not defined

**Step 3: Implement metadata types and FSM**

Create `internal/metadata/store.go`:
```go
package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// VolumeMeta stores block volume chunk mappings.
type VolumeMeta struct {
	VolumeID  string   `json:"volumeID"`
	Pool      string   `json:"pool"`
	SizeBytes uint64   `json:"sizeBytes"`
	ChunkIDs  []string `json:"chunkIDs"`
}

// PlacementMap stores which nodes hold a given chunk.
type PlacementMap struct {
	ChunkID string   `json:"chunkID"`
	Nodes   []string `json:"nodes"`
}

// RaftStore is a Raft-backed metadata store.
type RaftStore struct {
	raft *raft.Raft
	fsm  *FSM
}

// NewRaftStore creates and starts a Raft node.
func NewRaftStore(nodeID, dataDir, bindAddr string, bootstrap bool) (*RaftStore, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 1024

	// Resolve bind address
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(addr.String(), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating transport: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot store: %w", err)
	}

	logStorePath := filepath.Join(dataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}

	fsm := NewFSM()

	r, err := raft.NewRaft(config, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("creating raft: %w", err)
	}

	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(cfg)
	}

	return &RaftStore{raft: r, fsm: fsm}, nil
}

// IsLeader returns true if this node is the Raft leader.
func (s *RaftStore) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// Close shuts down the Raft node.
func (s *RaftStore) Close() error {
	return s.raft.Shutdown().Error()
}

func (s *RaftStore) apply(op *fsmOp) error {
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshaling operation: %w", err)
	}

	f := s.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("applying raft log: %w", err)
	}

	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

func (s *RaftStore) PutVolumeMeta(_ context.Context, meta *VolumeMeta) error {
	data, _ := json.Marshal(meta)
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketVolumes, Key: meta.VolumeID, Value: data})
}

func (s *RaftStore) GetVolumeMeta(_ context.Context, volumeID string) (*VolumeMeta, error) {
	data, err := s.fsm.Get(bucketVolumes, volumeID)
	if err != nil {
		return nil, err
	}
	var meta VolumeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling volume meta: %w", err)
	}
	return &meta, nil
}

func (s *RaftStore) DeleteVolumeMeta(_ context.Context, volumeID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketVolumes, Key: volumeID})
}

func (s *RaftStore) PutPlacementMap(_ context.Context, pm *PlacementMap) error {
	data, _ := json.Marshal(pm)
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketPlacements, Key: pm.ChunkID, Value: data})
}

func (s *RaftStore) GetPlacementMap(_ context.Context, chunkID string) (*PlacementMap, error) {
	data, err := s.fsm.Get(bucketPlacements, chunkID)
	if err != nil {
		return nil, err
	}
	var pm PlacementMap
	if err := json.Unmarshal(data, &pm); err != nil {
		return nil, fmt.Errorf("unmarshaling placement map: %w", err)
	}
	return &pm, nil
}
```

Create `internal/metadata/fsm.go`:
```go
package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const (
	opPut    = "put"
	opDelete = "delete"

	bucketVolumes    = "volumes"
	bucketPlacements = "placements"
)

type fsmOp struct {
	Op     string `json:"op"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"`
}

// FSM implements raft.FSM with an in-memory key-value store organized by buckets.
type FSM struct {
	mu      sync.RWMutex
	buckets map[string]map[string][]byte
}

// NewFSM creates a new finite state machine.
func NewFSM() *FSM {
	return &FSM{
		buckets: map[string]map[string][]byte{
			bucketVolumes:    {},
			bucketPlacements: {},
		},
	}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var op fsmOp
	if err := json.Unmarshal(log.Data, &op); err != nil {
		return fmt.Errorf("unmarshaling fsm op: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	bucket, ok := f.buckets[op.Bucket]
	if !ok {
		f.buckets[op.Bucket] = make(map[string][]byte)
		bucket = f.buckets[op.Bucket]
	}

	switch op.Op {
	case opPut:
		bucket[op.Key] = op.Value
	case opDelete:
		delete(bucket, op.Key)
	default:
		return fmt.Errorf("unknown op: %s", op.Op)
	}
	return nil
}

// Get reads a value from the FSM (local read, no Raft consensus).
func (f *FSM) Get(bucket, key string) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	b, ok := f.buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("bucket %s not found", bucket)
	}
	data, ok := b[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in bucket %s", key, bucket)
	}
	return data, nil
}

type fsmSnapshot struct {
	data map[string]map[string][]byte
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	cp := make(map[string]map[string][]byte)
	for bk, bv := range f.buckets {
		bucket := make(map[string][]byte)
		for k, v := range bv {
			val := make([]byte, len(v))
			copy(val, v)
			bucket[k] = val
		}
		cp[bk] = bucket
	}
	return &fsmSnapshot{data: cp}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string]map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("decoding snapshot: %w", err)
	}

	f.mu.Lock()
	f.buckets = data
	f.mu.Unlock()
	return nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.data)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("marshaling snapshot: %w", err)
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("writing snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/pascal/Documents/git/novastor && go test ./internal/metadata/ -v -race -timeout 30s`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/metadata/
git commit -m "[Feature] Add Raft-based metadata service

Raft consensus with in-memory FSM, volume and placement metadata,
snapshot/restore support, leader-only writes with local reads."
```

---

## Task 12: Operator Skeleton — Controller and Agent Entry Points

**Files:**
- Create: `cmd/controller/main.go`
- Create: `cmd/agent/main.go`
- Create: `cmd/meta/main.go`
- Create: `cmd/csi/main.go`
- Create: `cmd/filer/main.go`
- Create: `cmd/s3gw/main.go`
- Create: `cmd/cli/main.go`

**Step 1: Create all entry points**

Create `cmd/controller/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastor-controller %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "Controller not yet implemented")
	os.Exit(0)
}
```

Create `cmd/agent/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastor-agent %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "Agent not yet implemented")
	os.Exit(0)
}
```

Create `cmd/meta/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastor-meta %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "Metadata service not yet implemented")
	os.Exit(0)
}
```

Create `cmd/csi/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastor-csi %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "CSI driver not yet implemented")
	os.Exit(0)
}
```

Create `cmd/filer/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastor-filer %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "File gateway not yet implemented")
	os.Exit(0)
}
```

Create `cmd/s3gw/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastor-s3gw %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "S3 gateway not yet implemented")
	os.Exit(0)
}
```

Create `cmd/cli/main.go`:
```go
package main

import (
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "novastorctl %s (commit: %s, built: %s)\n", version, commit, date)
	fmt.Fprintln(os.Stderr, "CLI not yet implemented")
	os.Exit(0)
}
```

**Step 2: Verify all binaries build**

Run: `cd /Users/pascal/Documents/git/novastor && make build-all`
Expected: All 7 binaries build successfully in `bin/`

**Step 3: Verify all tests pass**

Run: `cd /Users/pascal/Documents/git/novastor && go test -race ./...`
Expected: All tests pass

**Step 4: Commit**

```bash
git add cmd/
git commit -m "[Feature] Add entry points for all 7 binaries

Skeleton main.go for controller, agent, meta, csi, filer, s3gw,
and cli. All binaries compile with version/commit/date ldflags."
```

---

## Task 13: Sample CRD Manifests

**Files:**
- Create: `config/samples/storagepool-replicated.yaml`
- Create: `config/samples/storagepool-erasurecoded.yaml`
- Create: `config/samples/blockvolume.yaml`
- Create: `config/samples/sharedfilesystem.yaml`
- Create: `config/samples/objectstore.yaml`

**Step 1: Create sample manifests**

Create `config/samples/storagepool-replicated.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: nvme-replicated
spec:
  nodeSelector:
    matchLabels:
      storage-tier: nvme
  deviceFilter:
    type: nvme
    minSize: "100Gi"
  dataProtection:
    mode: replication
    replication:
      factor: 3
      writeQuorum: 2
```

Create `config/samples/storagepool-erasurecoded.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: ssd-erasurecoded
spec:
  nodeSelector:
    matchLabels:
      storage-tier: ssd
  deviceFilter:
    type: ssd
    minSize: "200Gi"
  dataProtection:
    mode: erasureCoding
    erasureCoding:
      dataShards: 4
      parityShards: 2
```

Create `config/samples/blockvolume.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: BlockVolume
metadata:
  name: postgres-data
spec:
  pool: nvme-replicated
  size: "100Gi"
  accessMode: ReadWriteOnce
```

Create `config/samples/sharedfilesystem.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: SharedFilesystem
metadata:
  name: ml-datasets
spec:
  pool: ssd-erasurecoded
  capacity: "1Ti"
  accessMode: ReadWriteMany
  export:
    protocol: nfs
```

Create `config/samples/objectstore.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: ObjectStore
metadata:
  name: app-assets
spec:
  pool: ssd-erasurecoded
  endpoint:
    service:
      port: 9000
  bucketPolicy:
    maxBuckets: 100
    versioning: enabled
```

**Step 2: Commit**

```bash
git add config/samples/
git commit -m "[Docs] Add sample CRD manifests

Example StoragePools (replicated + erasure-coded), BlockVolume,
SharedFilesystem, and ObjectStore configurations."
```

---

## Task 14: Final Verification and Cleanup

**Step 1: Run full test suite**

Run: `cd /Users/pascal/Documents/git/novastor && go test -race -coverprofile=coverage.out ./...`
Expected: All tests pass

**Step 2: Run linting**

Run: `cd /Users/pascal/Documents/git/novastor && make check`
Expected: No lint errors

**Step 3: Verify all binaries build**

Run: `cd /Users/pascal/Documents/git/novastor && make build-all`
Expected: All 7 binaries in `bin/`

**Step 4: Run go mod tidy**

Run: `cd /Users/pascal/Documents/git/novastor && go mod tidy && git diff --quiet go.mod go.sum`
Expected: No changes (already tidy)

**Step 5: Final commit if any cleanup needed**

Only if previous steps revealed issues that needed fixing.
