package policy

import (
	"context"
	"fmt"

	"github.com/azrtydxb/novastor/api/v1alpha1"
)

// MetadataClient is the interface for accessing metadata.
// This allows the policy engine to work with different implementations.
type MetadataClient interface {
	GetPlacementMap(ctx context.Context, chunkID string) (*PlacementMap, error)
	PutPlacementMap(ctx context.Context, pm *PlacementMap) error
	ListPlacementMaps(ctx context.Context) ([]*PlacementMap, error)
	ListNodeMetas(ctx context.Context) ([]*NodeMeta, error)
	GetVolumeMeta(ctx context.Context, volumeID string) (*VolumeMeta, error)
	ListVolumesMeta(ctx context.Context) ([]*VolumeMeta, error)
}

// PlacementMap represents where chunks are stored.
type PlacementMap struct {
	ChunkID string   `json:"chunkID"`
	Nodes   []string `json:"nodes"`
}

// NodeMeta represents metadata about a storage node.
type NodeMeta struct {
	NodeID        string `json:"nodeID"`
	Address       string `json:"address"`
	LastHeartbeat int64  `json:"lastHeartbeat"`
}

// VolumeMeta represents metadata about a volume.
type VolumeMeta struct {
	VolumeID       string                       `json:"volumeID"`
	Pool           string                       `json:"pool"`
	SizeBytes      uint64                       `json:"sizeBytes"`
	ChunkIDs       []string                     `json:"chunkIDs"`
	DataProtection *v1alpha1.DataProtectionSpec `json:"dataProtection,omitempty"`
}

// ReplicationChecker verifies compliance for replication-mode chunks.
type ReplicationChecker struct {
	metaClient  MetadataClient
	nodeChecker NodeAvailabilityChecker
}

// NewReplicationChecker creates a new ReplicationChecker.
func NewReplicationChecker(metaClient MetadataClient, nodeChecker NodeAvailabilityChecker) *ReplicationChecker {
	return &ReplicationChecker{
		metaClient:  metaClient,
		nodeChecker: nodeChecker,
	}
}

// RequiredReplicas returns the replication factor from the volume's data protection config.
func (c *ReplicationChecker) RequiredReplicas(volume *VolumeMeta) int {
	if volume == nil || volume.DataProtection == nil || volume.DataProtection.Replication == nil {
		return 3 // Default fallback
	}

	factor := volume.DataProtection.Replication.Factor
	if factor == 0 {
		return 3 // Default fallback
	}

	return factor
}

// CheckChunk verifies that a replicated chunk has the required number of healthy replicas.
func (c *ReplicationChecker) CheckChunk(ctx context.Context, chunkID string, volume *VolumeMeta) (*ChunkComplianceResult, error) {
	if volume.DataProtection == nil || volume.DataProtection.Mode != "replication" {
		return nil, fmt.Errorf("volume %s is not in replication mode", volume.VolumeID)
	}

	replicationSpec := volume.DataProtection.Replication
	if replicationSpec == nil {
		return nil, fmt.Errorf("volume %s has nil replication spec", volume.VolumeID)
	}

	expectedCount := replicationSpec.Factor
	if expectedCount == 0 {
		expectedCount = 3 // Default
	}

	// Get the placement map for this chunk.
	placement, err := c.metaClient.GetPlacementMap(ctx, chunkID)
	if err != nil {
		return nil, fmt.Errorf("getting placement map for chunk %s: %w", chunkID, err)
	}

	result := &ChunkComplianceResult{
		ChunkID:        chunkID,
		VolumeID:       volume.VolumeID,
		Pool:           volume.Pool,
		ProtectionMode: "replication",
		ExpectedCount:  expectedCount,
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
	if result.ActualCount == 0 {
		result.Status = StatusUnavailable
	} else if result.ActualCount < expectedCount {
		result.Status = StatusUnderReplicated
	} else {
		result.Status = StatusCompliant
	}

	return result, nil
}

// CheckChunkWithChecksum verifies chunk compliance including checksum validation.
// It attempts to read the chunk from an available node and verify its checksum.
func (c *ReplicationChecker) CheckChunkWithChecksum(ctx context.Context, chunkID string, volume *VolumeMeta, chunkReader ChunkReader) (*ChunkComplianceResult, error) {
	result, err := c.CheckChunk(ctx, chunkID, volume)
	if err != nil {
		return nil, err
	}

	// If no replicas are available, we can't verify checksum.
	if len(result.AvailableNodes) == 0 {
		return result, nil
	}

	// Try to read and verify the chunk from an available node.
	for _, nodeID := range result.AvailableNodes {
		corrupted, readErr := chunkReader.VerifyChunkChecksum(ctx, nodeID, chunkID)
		if readErr != nil {
			// If we can't read from this node, consider it failed for checksum purposes.
			continue
		}
		if corrupted {
			result.Status = StatusCorrupted
			return result, nil
		}
		// Found a valid replica with correct checksum.
		break
	}

	return result, nil
}

// ChunkReader can read chunks and verify their checksums.
type ChunkReader interface {
	// VerifyChunkChecksum reads the chunk and returns true if the checksum is invalid.
	// Returns an error if the chunk cannot be read.
	VerifyChunkChecksum(ctx context.Context, nodeID, chunkID string) (corrupted bool, err error)
}

// RepairChunk replicates a chunk to a new node to restore compliance.
func (c *ReplicationChecker) RepairChunk(ctx context.Context, chunkID string, sourceNode, destNode string, replicator ChunkReplicator) error {
	if replicator == nil {
		return fmt.Errorf("chunk replicator is nil")
	}
	return replicator.ReplicateChunk(ctx, chunkID, sourceNode, destNode)
}

// ChunkReplicator can replicate a chunk from one node to another.
type ChunkReplicator interface {
	ReplicateChunk(ctx context.Context, chunkID, sourceNode, destNode string) error
}
