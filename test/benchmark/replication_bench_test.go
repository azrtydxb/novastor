package benchmark

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/azrtydxb/novastor/internal/chunk"
)

func BenchmarkReplicatedWrite(b *testing.B) {
	stores := make(map[string]chunk.Store, 3)
	nodes := make([]string, 0, 3)
	for i := range 3 {
		dir := b.TempDir()
		s, err := chunk.NewLocalStore(dir)
		if err != nil {
			b.Fatal(err)
		}
		name := "node-" + string(rune('0'+i))
		stores[name] = s
		nodes = append(nodes, name)
	}
	rm := chunk.NewReplicationManager(stores)
	ctx := context.Background()
	writeQuorum := 2

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := make([]byte, chunk.ChunkSize)
		if _, err := rand.Read(data); err != nil {
			b.Fatal(err)
		}
		c := &chunk.Chunk{
			ID:   chunk.NewChunkID(data),
			Data: data,
		}
		c.Checksum = c.ComputeChecksum()
		if err := rm.Replicate(ctx, c, nodes, writeQuorum); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicatedRead(b *testing.B) {
	stores := make(map[string]chunk.Store, 3)
	nodes := make([]string, 0, 3)
	for i := range 3 {
		dir := b.TempDir()
		s, err := chunk.NewLocalStore(dir)
		if err != nil {
			b.Fatal(err)
		}
		name := "node-" + string(rune('0'+i))
		stores[name] = s
		nodes = append(nodes, name)
	}
	rm := chunk.NewReplicationManager(stores)
	ctx := context.Background()

	// Pre-write a chunk across replicas.
	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}
	c := &chunk.Chunk{
		ID:   chunk.NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()
	if err := rm.Replicate(ctx, c, nodes, 2); err != nil {
		b.Fatal(err)
	}
	chunkID := c.ID

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rm.ReadFromAny(ctx, chunkID, nodes); err != nil {
			b.Fatal(err)
		}
	}
}
