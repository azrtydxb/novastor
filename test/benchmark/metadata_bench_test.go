package benchmark

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
)

// getFreePort returns an available TCP port on localhost.
func getFreePort(b *testing.B) string {
	b.Helper()
	lc := net.ListenConfig{}
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// newTestRaftStore creates a single-node bootstrapped RaftStore for benchmarking.
// It waits until the node becomes leader before returning.
func newTestRaftStore(b *testing.B) *metadata.RaftStore {
	b.Helper()
	dir := b.TempDir()
	bindAddr := getFreePort(b)

	store, err := metadata.NewRaftStore(metadata.RaftConfig{
		NodeID:   "bench-node",
		DataDir:  dir,
		RaftAddr: bindAddr,
	})
	if err != nil {
		b.Fatal(err)
	}

	// Wait for leader election (single-node cluster should be fast).
	deadline := time.Now().Add(10 * time.Second)
	for !store.IsLeader() {
		if time.Now().After(deadline) {
			b.Fatal("timed out waiting for Raft leader election")
		}
		time.Sleep(50 * time.Millisecond)
	}

	b.Cleanup(func() {
		store.Close()
	})

	return store
}

func BenchmarkMetadataPut(b *testing.B) {
	store := newTestRaftStore(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta := &metadata.VolumeMeta{
			VolumeID:  fmt.Sprintf("vol-%d", i),
			Pool:      "default",
			SizeBytes: 1024 * 1024 * 1024,
			ChunkIDs:  []string{"chunk-a", "chunk-b", "chunk-c"},
		}
		if err := store.PutVolumeMeta(ctx, meta); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetadataGet(b *testing.B) {
	store := newTestRaftStore(b)
	ctx := context.Background()

	// Pre-put a volume meta entry.
	meta := &metadata.VolumeMeta{
		VolumeID:  "vol-bench",
		Pool:      "default",
		SizeBytes: 1024 * 1024 * 1024,
		ChunkIDs:  []string{"chunk-a", "chunk-b", "chunk-c"},
	}
	if err := store.PutVolumeMeta(ctx, meta); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.GetVolumeMeta(ctx, "vol-bench"); err != nil {
			b.Fatal(err)
		}
	}
}
