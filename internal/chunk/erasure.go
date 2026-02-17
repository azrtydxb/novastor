package chunk

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

type ErasureCoder struct {
	dataShards   int
	parityShards int
	encoder      reedsolomon.Encoder
}

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

func (ec *ErasureCoder) Decode(shards [][]byte) ([]byte, error) {
	if err := ec.encoder.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstructing shards: %w", err)
	}
	// Calculate total size of data shards for Join
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

func (ec *ErasureCoder) ShardCount() int  { return ec.dataShards + ec.parityShards }
func (ec *ErasureCoder) DataShards() int   { return ec.dataShards }
func (ec *ErasureCoder) ParityShards() int { return ec.parityShards }
