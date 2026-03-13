// Package chunk provides chunk type definitions and utilities for the NovaStor
// storage system. All actual chunk I/O is handled by the Rust SPDK data-plane;
// this package provides only Go-side type definitions used by management-plane
// components (CSI controller, metadata, operator).
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"github.com/azrtydxb/novastor/internal/metadata"
)

// ChunkSize is the fixed size (4 MB) for all data chunks.
const ChunkSize = 4 * 1024 * 1024

// ChunkID uniquely identifies a chunk in storage via its SHA-256 hash.
type ChunkID string

// NewChunkID computes a content-addressed chunk ID from data using SHA-256.
// The same data always produces the same chunk ID, enabling deduplication.
func NewChunkID(data []byte) ChunkID {
	h := sha256.Sum256(data)
	return ChunkID(hex.EncodeToString(h[:]))
}

// Chunk represents a single unit of storage (4 MB) with its metadata.
// Chunks are immutable and content-addressed for deduplication.
type Chunk struct {
	ID       ChunkID
	Data     []byte
	Checksum uint32

	// ProtectionProfile specifies the data protection settings for this chunk.
	ProtectionProfile *metadata.ProtectionProfile `json:"protectionProfile,omitempty"`

	// ComplianceInfo tracks the current compliance state of this chunk.
	ComplianceInfo *metadata.ComplianceInfo `json:"complianceInfo,omitempty"`
}

// ComputeChecksum calculates the CRC-32C checksum of the chunk data.
func (c *Chunk) ComputeChecksum() uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(c.Data, table)
}

// VerifyChecksum checks that the stored checksum matches the computed checksum.
// Returns an error if they differ, indicating data corruption.
func (c *Chunk) VerifyChecksum() error {
	actual := c.ComputeChecksum()
	if actual != c.Checksum {
		return fmt.Errorf("checksum mismatch for chunk %s: stored=%d computed=%d", c.ID, c.Checksum, actual)
	}
	return nil
}

// SplitData divides data into fixed-size 4 MB chunks.
// Each chunk is assigned a content-addressed ID and checksum.
// Empty input returns nil.
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
