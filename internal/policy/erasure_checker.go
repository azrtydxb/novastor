package policy

import (
	"context"
	"fmt"

	"github.com/azrtydxb/novastor/api/v1alpha1"
)

// ErasureCodingChecker verifies compliance for erasure-coded chunks.
type ErasureCodingChecker struct {
	metaClient  MetadataClient
	nodeChecker NodeAvailabilityChecker
}

// NewErasureCodingChecker creates a new ErasureCodingChecker.
func NewErasureCodingChecker(metaClient MetadataClient, nodeChecker NodeAvailabilityChecker) *ErasureCodingChecker {
	return &ErasureCodingChecker{
		metaClient:  metaClient,
		nodeChecker: nodeChecker,
	}
}

// RequiredReplicas returns the total number of shards (data + parity).
func (c *ErasureCodingChecker) RequiredReplicas(volume *VolumeMeta) int {
	if volume == nil || volume.DataProtection == nil || volume.DataProtection.ErasureCoding == nil {
		return 6 // Default fallback (4 data + 2 parity)
	}

	ecSpec := volume.DataProtection.ErasureCoding
	dataShards := ecSpec.DataShards
	if dataShards == 0 {
		dataShards = 4 // Default
	}
	parityShards := ecSpec.ParityShards
	if parityShards == 0 {
		parityShards = 2 // Default
	}

	return dataShards + parityShards
}

// CheckChunk verifies that an erasure-coded chunk has sufficient available shards for recovery.
func (c *ErasureCodingChecker) CheckChunk(ctx context.Context, chunkID string, volume *VolumeMeta) (*ChunkComplianceResult, error) {
	if volume.DataProtection == nil || volume.DataProtection.Mode != "erasureCoding" {
		return nil, fmt.Errorf("volume %s is not in erasure coding mode", volume.VolumeID)
	}

	ecSpec := volume.DataProtection.ErasureCoding
	if ecSpec == nil {
		return nil, fmt.Errorf("volume %s has nil erasure coding spec", volume.VolumeID)
	}

	dataShards := ecSpec.DataShards
	if dataShards == 0 {
		dataShards = 4 // Default
	}
	parityShards := ecSpec.ParityShards
	if parityShards == 0 {
		parityShards = 2 // Default
	}

	totalShards := dataShards + parityShards
	minShardsForRecovery := dataShards

	// Get the placement map for this chunk.
	placement, err := c.metaClient.GetPlacementMap(ctx, chunkID)
	if err != nil {
		return nil, fmt.Errorf("getting placement map for chunk %s: %w", chunkID, err)
	}

	result := &ChunkComplianceResult{
		ChunkID:        chunkID,
		VolumeID:       volume.VolumeID,
		Pool:           volume.Pool,
		ProtectionMode: "erasure_coding",
		ExpectedCount:  totalShards,
	}

	// Check availability of each node in the placement map.
	for _, nodeID := range placement.Nodes {
		if c.nodeChecker.IsNodeAvailable(ctx, nodeID) {
			result.AvailableNodes = append(result.AvailableNodes, nodeID)
		} else {
			result.FailedNodes = append(result.FailedNodes, nodeID)
		}
	}

	result.ActualCount = len(result.AvailableNodes)

	// Determine compliance status.
	// For erasure coding:
	// - Compliant: all shards available
	// - UnderReplicated: some shards lost but still recoverable (>= dataShards available)
	// - Unavailable: insufficient shards for recovery (< dataShards available)
	if result.ActualCount == 0 {
		result.Status = StatusUnavailable
	} else if result.ActualCount < minShardsForRecovery {
		result.Status = StatusUnavailable
	} else if result.ActualCount < totalShards {
		result.Status = StatusUnderReplicated
	} else {
		result.Status = StatusCompliant
	}

	return result, nil
}

// CheckChunkWithIntegrity verifies chunk compliance including data integrity checks.
// It attempts to decode the erasure-coded data to verify it can be reconstructed.
func (c *ErasureCodingChecker) CheckChunkWithIntegrity(ctx context.Context, chunkID string, volume *VolumeMeta, shardReader ShardReader) (*ChunkComplianceResult, error) {
	result, err := c.CheckChunk(ctx, chunkID, volume)
	if err != nil {
		return nil, err
	}

	// If we don't have enough shards to potentially reconstruct, skip integrity check.
	if volume.DataProtection == nil || volume.DataProtection.ErasureCoding == nil {
		return result, nil
	}

	ecSpec := volume.DataProtection.ErasureCoding
	dataShards := ecSpec.DataShards
	if dataShards == 0 {
		dataShards = 4
	}

	if len(result.AvailableNodes) < dataShards {
		return result, nil
	}

	// Try to read and verify the chunk can be reconstructed.
	if shardReader != nil {
		corrupted, verifyErr := shardReader.VerifyErasureChunk(ctx, chunkID, result.AvailableNodes, dataShards)
		if verifyErr != nil {
			// Log the error but don't fail the compliance check entirely.
			// Return the error so caller knows verification failed.
			return result, fmt.Errorf("verifying erasure chunk: %w", verifyErr)
		}
		if corrupted {
			result.Status = StatusCorrupted
		}
	}

	return result, nil
}

// ShardReader can read erasure-coded shards and verify data integrity.
type ShardReader interface {
	// VerifyErasureChunk reads shards from the given nodes and attempts reconstruction.
	// Returns true if the data is corrupted (cannot be reconstructed or checksum fails).
	VerifyErasureChunk(ctx context.Context, chunkID string, nodes []string, minShards int) (corrupted bool, err error)
}

// ShardReplicator can regenerate and replicate a lost shard.
type ShardReplicator interface {
	// RegenerateShard recreates a lost shard from available shards and stores it on the destination node.
	RegenerateShard(ctx context.Context, chunkID string, sourceNodes []string, destNode string, dataShards, parityShards int) error
}

// RepairChunk regenerates a lost shard from available shards.
func (c *ErasureCodingChecker) RepairChunk(ctx context.Context, chunkID string, sourceNodes []string, destNode string, dataShards, parityShards int, replicator ShardReplicator) error {
	if replicator == nil {
		return fmt.Errorf("shard replicator is nil")
	}
	return replicator.RegenerateShard(ctx, chunkID, sourceNodes, destNode, dataShards, parityShards)
}

// GetMinShardsForRecovery returns the minimum number of shards needed to recover data.
func GetMinShardsForRecovery(ecSpec *v1alpha1.ErasureCodingSpec) int {
	if ecSpec == nil {
		return 4 // Default
	}
	if ecSpec.DataShards > 0 {
		return ecSpec.DataShards
	}
	return 4 // Default
}

// ValidateErasureCodingConfig checks if the erasure coding configuration is valid.
func ValidateErasureCodingConfig(dataShards, parityShards int) error {
	if dataShards <= 0 {
		return fmt.Errorf("data shards must be positive, got %d", dataShards)
	}
	if parityShards <= 0 {
		return fmt.Errorf("parity shards must be positive, got %d", parityShards)
	}
	return nil
}
