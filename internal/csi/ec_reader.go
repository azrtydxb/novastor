package csi

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/chunk"
	"github.com/azrtydxb/novastor/internal/logging"
)

// ECReader reconstructs chunks from erasure-coded shards.
type ECReader struct {
	chunkClient ChunkClient
}

// NewECReader creates a new ECReader.
func NewECReader(client ChunkClient) *ECReader {
	return &ECReader{chunkClient: client}
}

// ReadChunk reads shards from their assigned nodes and reconstructs
// the original chunk data. Nodes with empty string entries are treated
// as unavailable. At least dataShards shards must be readable.
func (r *ECReader) ReadChunk(
	ctx context.Context,
	ec *chunk.ErasureCoder,
	chunkID string,
	shardNodes []string,
) ([]byte, error) {
	totalShards := ec.ShardCount()
	if len(shardNodes) != totalShards {
		return nil, fmt.Errorf("shard nodes count %d does not match total shards %d", len(shardNodes), totalShards)
	}

	// Read all available shards concurrently.
	shards := make([][]byte, totalShards)
	var mu sync.Mutex
	var readErrors int

	var wg sync.WaitGroup
	for i := range totalShards {
		nodeID := shardNodes[i]
		if nodeID == "" {
			mu.Lock()
			readErrors++
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(idx int, node string) {
			defer wg.Done()
			shardID := string(chunk.ShardID(chunkID, idx))
			data, err := r.chunkClient.GetChunk(ctx, node, shardID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				readErrors++
				logging.L.Debug("failed to read shard",
					zap.String("chunkID", chunkID),
					zap.Int("shardIndex", idx),
					zap.String("nodeID", node),
					zap.Error(err))
				return
			}
			shards[idx] = data
		}(i, nodeID)
	}
	wg.Wait()

	available := totalShards - readErrors
	if available < ec.DataShards() {
		return nil, fmt.Errorf("insufficient shards for chunk %s: have %d, need %d", chunkID, available, ec.DataShards())
	}

	data, err := ec.Decode(shards)
	if err != nil {
		return nil, fmt.Errorf("decoding chunk %s: %w", chunkID, err)
	}

	if readErrors > 0 {
		logging.L.Warn("degraded read for EC chunk",
			zap.String("chunkID", chunkID),
			zap.Int("availableShards", available),
			zap.Int("totalShards", totalShards),
			zap.Int("missingShards", readErrors))
	}

	return data, nil
}
