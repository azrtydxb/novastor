package benchmark

import (
	"crypto/rand"
	"testing"

	"github.com/piwi3910/novastor/internal/chunk"
)

func BenchmarkErasureEncode(b *testing.B) {
	ec, err := chunk.NewErasureCoder(4, 2)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ec.Encode(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkErasureDecode(b *testing.B) {
	ec, err := chunk.NewErasureCoder(4, 2)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	shards, err := ec.Encode(data)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Make a copy of shards so Decode can be called repeatedly.
		shardsCopy := make([][]byte, len(shards))
		for j, s := range shards {
			sc := make([]byte, len(s))
			copy(sc, s)
			shardsCopy[j] = sc
		}
		if _, err := ec.Decode(shardsCopy); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkErasureDecodeWithLoss(b *testing.B) {
	ec, err := chunk.NewErasureCoder(4, 2)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, chunk.ChunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	shards, err := ec.Encode(data)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(chunk.ChunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Copy shards and nil out two parity shards to force reconstruction.
		shardsCopy := make([][]byte, len(shards))
		for j, s := range shards {
			sc := make([]byte, len(s))
			copy(sc, s)
			shardsCopy[j] = sc
		}
		// Simulate loss of 2 shards (the parity shards).
		shardsCopy[4] = nil
		shardsCopy[5] = nil
		if _, err := ec.Decode(shardsCopy); err != nil {
			b.Fatal(err)
		}
	}
}
