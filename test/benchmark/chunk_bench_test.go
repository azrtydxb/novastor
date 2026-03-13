package benchmark

import (
	"crypto/rand"
	"testing"

	"github.com/azrtydxb/novastor/internal/chunk"
)

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
