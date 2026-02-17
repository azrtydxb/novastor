package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
)

const ChunkSize = 4 * 1024 * 1024

type ChunkID string

func NewChunkID(data []byte) ChunkID {
	h := sha256.Sum256(data)
	return ChunkID(hex.EncodeToString(h[:]))
}

type Chunk struct {
	ID       ChunkID
	Data     []byte
	Checksum uint32
}

func (c *Chunk) ComputeChecksum() uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(c.Data, table)
}

func (c *Chunk) VerifyChecksum() error {
	actual := c.ComputeChecksum()
	if actual != c.Checksum {
		return fmt.Errorf("checksum mismatch for chunk %s: stored=%d computed=%d", c.ID, c.Checksum, actual)
	}
	return nil
}

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
