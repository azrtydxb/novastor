package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"github.com/piwi3910/novastor/internal/metadata"
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

	// ProtectionProfile specifies the data protection settings for this chunk.
	ProtectionProfile *metadata.ProtectionProfile `json:"protectionProfile,omitempty"`

	// ComplianceInfo tracks the current compliance state of this chunk.
	ComplianceInfo *metadata.ComplianceInfo `json:"complianceInfo,omitempty"`
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
