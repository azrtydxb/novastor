package benchmark

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/azrtydxb/novastor/internal/chunk"
)

func BenchmarkChunkWrite(b *testing.B) {
	dir := b.TempDir()
	store, err := chunk.NewLocalStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

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
		if err := store.Put(ctx, c); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChunkRead(b *testing.B) {
	dir := b.TempDir()
	store, err := chunk.NewLocalStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	// Pre-write a chunk to read back.
	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}
	c := &chunk.Chunk{
		ID:   chunk.NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()
	if err := store.Put(ctx, c); err != nil {
		b.Fatal(err)
	}
	chunkID := c.ID

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(ctx, chunkID); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChunkChecksum(b *testing.B) {
	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}
	c := &chunk.Chunk{Data: data}

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.ComputeChecksum()
	}
}

func BenchmarkChunkID(b *testing.B) {
	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunk.NewChunkID(data)
	}
}

func BenchmarkSplitData(b *testing.B) {
	const totalSize = 16 * 1024 * 1024 // 16 MB
	data := make([]byte, totalSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(totalSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunk.SplitData(data)
	}
}

func BenchmarkVerifyChecksum(b *testing.B) {
	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}
	c := &chunk.Chunk{Data: data}
	c.Checksum = c.ComputeChecksum()

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.VerifyChecksum(); err != nil {
			b.Fatal(err)
		}
	}
}
