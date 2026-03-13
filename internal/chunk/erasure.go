package chunk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/klauspost/reedsolomon"
)

const shardSeparator = ":shard:"

// ShardID returns the chunk ID for a specific shard of an erasure-coded chunk.
func ShardID(chunkID string, shardIndex int) ChunkID {
	if strings.Contains(chunkID, shardSeparator) {
		panic(fmt.Sprintf("chunk ID %q contains shard separator %q", chunkID, shardSeparator))
	}
	return ChunkID(fmt.Sprintf("%s%s%d", chunkID, shardSeparator, shardIndex))
}

// ParseShardID extracts the original chunk ID and shard index from a shard ID.
func ParseShardID(id ChunkID) (chunkID string, shardIndex int, err error) {
	s := string(id)
	idx := strings.LastIndex(s, shardSeparator)
	if idx < 0 {
		return "", 0, fmt.Errorf("not a shard ID: missing %q separator", shardSeparator)
	}
	chunkID = s[:idx]
	if chunkID == "" {
		return "", 0, fmt.Errorf("not a shard ID: empty chunk ID")
	}
	shardIndex, err = strconv.Atoi(s[idx+len(shardSeparator):])
	if err != nil {
		return "", 0, fmt.Errorf("not a shard ID: invalid shard index: %w", err)
	}
	if shardIndex < 0 {
		return "", 0, fmt.Errorf("not a shard ID: negative shard index %d", shardIndex)
	}
	return chunkID, shardIndex, nil
}

// IsShardID returns true if the given ID is a shard ID.
func IsShardID(id ChunkID) bool {
	return strings.Contains(string(id), shardSeparator)
}

// ErasureCoder provides Reed-Solomon erasure coding for chunk protection.
type ErasureCoder struct {
	dataShards   int
	parityShards int
	encoder      reedsolomon.Encoder
}

// NewErasureCoder creates a new erasure coder with the given shard counts.
func NewErasureCoder(dataShards, parityShards int) (*ErasureCoder, error) {
	if dataShards <= 0 || parityShards <= 0 {
		return nil, fmt.Errorf("data shards (%d) and parity shards (%d) must be positive", dataShards, parityShards)
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("creating reed-solomon encoder: %w", err)
	}
	return &ErasureCoder{dataShards: dataShards, parityShards: parityShards, encoder: enc}, nil
}

// Encode splits data into shards and generates parity shards.
func (ec *ErasureCoder) Encode(data []byte) ([][]byte, error) {
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

// Decode reconstructs the original data from a set of shards.
func (ec *ErasureCoder) Decode(shards [][]byte) ([]byte, error) {
	if err := ec.encoder.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstructing shards: %w", err)
	}
	shardSize := 0
	for _, s := range shards {
		if s != nil {
			shardSize = len(s)
			break
		}
	}
	totalDataSize := shardSize * ec.dataShards

	var buf bytes.Buffer
	if err := ec.encoder.Join(&buf, shards, totalDataSize); err != nil {
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
func (ec *ErasureCoder) ShardCount() int { return ec.dataShards + ec.parityShards }

// DataShards returns the number of data shards.
func (ec *ErasureCoder) DataShards() int { return ec.dataShards }

// ParityShards returns the number of parity shards.
func (ec *ErasureCoder) ParityShards() int { return ec.parityShards }

// Reconstruct reconstructs missing shards in-place. Missing shards should be nil.
func (ec *ErasureCoder) Reconstruct(shards [][]byte) error {
	return ec.encoder.Reconstruct(shards)
}
