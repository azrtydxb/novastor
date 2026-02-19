package policy

import (
	"context"
	"fmt"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/datamover"
	"github.com/piwi3910/novastor/internal/metadata"
)

// ECShardReplicator implements the policy.ShardReplicator interface using
// the datamover package's EC reconstruction functionality.
type ECShardReplicator struct {
	metaStore    datamover.MetadataStore
	agentClient  datamover.AgentClient
	ecFactory    datamover.ErasureCoderFactory
}

// NewECShardReplicator creates a new shard replicator for erasure-coded chunks.
func NewECShardReplicator(
	metaStore datamover.MetadataStore,
	agentClient datamover.AgentClient,
	ecFactory datamover.ErasureCoderFactory,
) *ECShardReplicator {
	return &ECShardReplicator{
		metaStore:   metaStore,
		agentClient: agentClient,
		ecFactory:   ecFactory,
	}
}

// RegenerateShard recreates a lost shard from available shards and stores it on the destination node.
//
// This is the policy layer implementation which:
// 1. Queries the metadata service to get current shard placements
// 2. Determines which shard is missing (based on available nodes vs expected configuration)
// 3. Uses the datamover.ECReconstructor to reconstruct the missing shard
// 4. Writes the reconstructed shard to the destination node
// 5. Updates metadata with the new shard placement
func (r *ECShardReplicator) RegenerateShard(
	ctx context.Context,
	chunkID string,
	sourceNodes []string,
	destNode string,
	dataShards,
	parityShards int,
) error {
	// Get current shard placements from metadata
	placements, err := r.metaStore.GetShardPlacements(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("getting shard placements for %s: %w", chunkID, err)
	}

	// Group placements by node for quick lookup
	nodePlacements := make(map[string][]*metadata.ShardPlacement)
	for _, p := range placements {
		nodePlacements[p.NodeID] = append(nodePlacements[p.NodeID], p)
	}

	// Determine which shard index to regenerate
	// We need to find a shard that exists on source nodes but not on the destination node
	missingShardIndex := -1
	totalShards := dataShards + parityShards

	// Check each shard index to find one that's missing
	for shardIdx := 0; shardIdx < totalShards; shardIdx++ {
		hasShard := false
		for _, p := range placements {
			if p.ShardIndex == shardIdx && p.NodeID == destNode {
				hasShard = true
				break
			}
		}
		// Also check if any source node has this shard
		sourceHasShard := false
		for _, node := range sourceNodes {
			for _, p := range nodePlacements[node] {
				if p.ShardIndex == shardIdx {
					sourceHasShard = true
					break
				}
			}
		}
		// Found a shard that source has but destination doesn't
		if !hasShard && sourceHasShard {
			missingShardIndex = shardIdx
			break
		}
	}

	if missingShardIndex == -1 {
		// No missing shard found - this might be a new node receiving all shards
		// Default to regenerating the first data shard
		missingShardIndex = 0
	}

	// Create erasure coder for this configuration
	ec, err := r.ecFactory.GetErasureCoder(dataShards, parityShards)
	if err != nil {
		return fmt.Errorf("creating erasure coder: %w", err)
	}

	// Build source placements for reconstruction (filter to only source nodes)
	var sourcePlacements []*metadata.ShardPlacement
	for _, p := range placements {
		// Include if from a source node and matches our EC configuration
		isFromSource := false
		for _, srcNode := range sourceNodes {
			if p.NodeID == srcNode {
				isFromSource = true
				break
			}
		}
		if isFromSource && p.ShardIndex >= 0 && p.ShardIndex < totalShards {
			sourcePlacements = append(sourcePlacements, p)
		}
	}

	// Use the ECReconstructor from datamover package
	reconstructor := datamover.NewECReconstructor(r.agentClient)

	if err := reconstructor.ReconstructShard(ctx, chunkID, missingShardIndex, sourcePlacements, destNode, ec); err != nil {
		return fmt.Errorf("reconstructing shard %d: %w", missingShardIndex, err)
	}

	// Create new shard placement metadata
	newPlacement := &metadata.ShardPlacement{
		ChunkID:   chunkID,
		ShardIndex: missingShardIndex,
		NodeID:     destNode,
		State:      metadata.ShardStateHealthy,
	}

	// Add shard placement to metadata
	if err := r.metaStore.PutShardPlacement(ctx, newPlacement); err != nil {
		return fmt.Errorf("adding shard placement metadata: %w", err)
	}

	return nil
}

// Ensure ECShardReplicator implements policy.ShardReplicator
var _ ShardReplicator = (*ECShardReplicator)(nil)
