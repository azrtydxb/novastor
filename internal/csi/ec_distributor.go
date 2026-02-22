package csi

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
)

// ECDistributor encodes chunks into Reed-Solomon shards and distributes
// them to assigned storage nodes.
type ECDistributor struct {
	chunkClient ChunkClient
	meta        MetadataStore
}

// NewECDistributor creates a new ECDistributor.
func NewECDistributor(client ChunkClient, meta MetadataStore) *ECDistributor {
	return &ECDistributor{
		chunkClient: client,
		meta:        meta,
	}
}

// DistributeChunk reads a chunk from its source node, encodes it into
// Reed-Solomon shards, and distributes each shard to its assigned node.
// Shard placement metadata is persisted for later reconstruction.
func (d *ECDistributor) DistributeChunk(
	ctx context.Context,
	ec *chunk.ErasureCoder,
	chunkID string,
	sourceNode string,
	shardNodes []string,
	volumeID string,
) error {
	totalShards := ec.ShardCount()
	if len(shardNodes) != totalShards {
		return fmt.Errorf("shard nodes count %d does not match total shards %d", len(shardNodes), totalShards)
	}

	// Read the full chunk from the source node.
	data, err := d.chunkClient.GetChunk(ctx, sourceNode, chunkID)
	if err != nil {
		return fmt.Errorf("reading chunk %s from node %s: %w", chunkID, sourceNode, err)
	}

	// Encode into data + parity shards.
	shards, err := ec.Encode(data)
	if err != nil {
		return fmt.Errorf("encoding chunk %s: %w", chunkID, err)
	}

	// Distribute shards to nodes concurrently.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	for i := range totalShards {
		shardIndex := i
		shardData := shards[i]
		nodeID := shardNodes[i]
		shardID := string(chunk.ShardID(chunkID, shardIndex))

		g.Go(func() error {
			if err := d.chunkClient.PutChunk(gCtx, nodeID, shardID, shardData); err != nil {
				return fmt.Errorf("writing shard %d of chunk %s to node %s: %w", shardIndex, chunkID, nodeID, err)
			}

			// Persist shard placement metadata.
			sp := &metadata.ShardPlacement{
				ChunkID:    chunkID,
				VolumeID:   volumeID,
				ShardIndex: shardIndex,
				NodeID:     nodeID,
			}
			if err := d.meta.PutShardPlacement(gCtx, sp); err != nil {
				return fmt.Errorf("writing shard placement for chunk %s shard %d: %w", chunkID, shardIndex, err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logging.L.Debug("distributed EC shards",
		zap.String("chunkID", chunkID),
		zap.String("volumeID", volumeID),
		zap.Int("totalShards", totalShards),
		zap.Int("dataShards", ec.DataShards()),
		zap.Int("parityShards", ec.ParityShards()))

	return nil
}
