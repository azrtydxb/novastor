package policy

import (
	"context"
	"fmt"

	"github.com/piwi3910/novastor/api/v1alpha1"
)

// ComplianceStatus represents the compliance state of a chunk.
type ComplianceStatus int

const (
	// StatusCompliant indicates the chunk meets all data protection requirements.
	StatusCompliant ComplianceStatus = iota
	// StatusUnderReplicated indicates the chunk has fewer replicas/shards than required.
	StatusUnderReplicated
	// StatusUnavailable indicates the chunk has no available replicas.
	StatusUnavailable
	// StatusCorrupted indicates the chunk has checksum mismatches.
	StatusCorrupted
)

// String returns a human-readable representation of the compliance status.
func (s ComplianceStatus) String() string {
	switch s {
	case StatusCompliant:
		return "compliant"
	case StatusUnderReplicated:
		return "under_replicated"
	case StatusUnavailable:
		return "unavailable"
	case StatusCorrupted:
		return "corrupted"
	default:
		return "unknown"
	}
}

// ChunkComplianceResult contains the compliance state and metadata for a single chunk.
type ChunkComplianceResult struct {
	ChunkID        string           `json:"chunkID"`
	Status         ComplianceStatus `json:"status"`
	ExpectedCount  int              `json:"expectedCount"`
	ActualCount    int              `json:"actualCount"`
	AvailableNodes []string         `json:"availableNodes"`
	FailedNodes    []string         `json:"failedNodes,omitempty"`
	VolumeID       string           `json:"volumeID"`
	Pool           string           `json:"pool"`
	ProtectionMode string           `json:"protectionMode"`
}

// VolumeComplianceReport contains the compliance summary for a single volume.
type VolumeComplianceReport struct {
	VolumeID              string                   `json:"volumeID"`
	Pool                  string                   `json:"pool"`
	TotalChunks           int                      `json:"totalChunks"`
	CompliantChunks       int                      `json:"compliantChunks"`
	UnderReplicatedChunks int                      `json:"underReplicatedChunks"`
	UnavailableChunks     int                      `json:"unavailableChunks"`
	CorruptedChunks       int                      `json:"corruptedChunks"`
	ChunkResults          []*ChunkComplianceResult `json:"chunkResults,omitempty"`
}

// PoolComplianceReport contains the compliance summary for a storage pool.
type PoolComplianceReport struct {
	PoolName              string                    `json:"poolName"`
	ProtectionMode        string                    `json:"protectionMode"`
	ReplicationFactor     int                       `json:"replicationFactor,omitempty"`
	DataShards            int                       `json:"dataShards,omitempty"`
	ParityShards          int                       `json:"parityShards,omitempty"`
	TotalVolumes          int                       `json:"totalVolumes"`
	TotalChunks           int                       `json:"totalChunks"`
	CompliantChunks       int                       `json:"compliantChunks"`
	UnderReplicatedChunks int                       `json:"underReplicatedChunks"`
	UnavailableChunks     int                       `json:"unavailableChunks"`
	CorruptedChunks       int                       `json:"corruptedChunks"`
	IsCompliant           bool                      `json:"isCompliant"`
	VolumeReports         []*VolumeComplianceReport `json:"volumeReports,omitempty"`
}

// ComplianceChecker defines the interface for checking chunk compliance.
type ComplianceChecker interface {
	// CheckChunk verifies if a chunk meets the data protection requirements.
	CheckChunk(ctx context.Context, chunkID string, volume *VolumeMeta, pool *v1alpha1.StoragePool) (*ChunkComplianceResult, error)
	// RequiredReplicas returns the number of replicas required for compliance.
	RequiredReplicas() int
}

// NodeAvailabilityChecker checks if a node is available.
type NodeAvailabilityChecker interface {
	IsNodeAvailable(ctx context.Context, nodeID string) bool
}

// PolicyEngine coordinates compliance checking across all volumes and pools.
type PolicyEngine struct {
	metaClient         MetadataClient
	nodeChecker        NodeAvailabilityChecker
	replicationChecker *ReplicationChecker
	erasureChecker     *ErasureCodingChecker
}

// NewPolicyEngine creates a new PolicyEngine with the given dependencies.
func NewPolicyEngine(metaClient MetadataClient, nodeChecker NodeAvailabilityChecker) *PolicyEngine {
	return &PolicyEngine{
		metaClient:         metaClient,
		nodeChecker:        nodeChecker,
		replicationChecker: NewReplicationChecker(metaClient, nodeChecker),
		erasureChecker:     NewErasureCodingChecker(metaClient, nodeChecker),
	}
}

// GetChecker returns the appropriate compliance checker for the given pool.
func (e *PolicyEngine) GetChecker(pool *v1alpha1.StoragePool) (ComplianceChecker, error) {
	switch pool.Spec.DataProtection.Mode {
	case "replication":
		return e.replicationChecker, nil
	case "erasureCoding":
		return e.erasureChecker, nil
	default:
		return nil, fmt.Errorf("unknown data protection mode: %s", pool.Spec.DataProtection.Mode)
	}
}

// CheckVolumeCompliance generates a compliance report for a single volume.
func (e *PolicyEngine) CheckVolumeCompliance(ctx context.Context, volume *VolumeMeta, pool *v1alpha1.StoragePool) (*VolumeComplianceReport, error) {
	report := &VolumeComplianceReport{
		VolumeID: volume.VolumeID,
		Pool:     volume.Pool,
	}

	checker, err := e.GetChecker(pool)
	if err != nil {
		return nil, fmt.Errorf("getting compliance checker for volume %s: %w", volume.VolumeID, err)
	}

	for _, chunkID := range volume.ChunkIDs {
		result, err := checker.CheckChunk(ctx, chunkID, volume, pool)
		if err != nil {
			return nil, fmt.Errorf("checking chunk %s: %w", chunkID, err)
		}

		report.TotalChunks++
		switch result.Status {
		case StatusCompliant:
			report.CompliantChunks++
		case StatusUnderReplicated:
			report.UnderReplicatedChunks++
		case StatusUnavailable:
			report.UnavailableChunks++
		case StatusCorrupted:
			report.CorruptedChunks++
		}
		report.ChunkResults = append(report.ChunkResults, result)
	}

	return report, nil
}

// CheckPoolCompliance generates a compliance report for a storage pool.
func (e *PolicyEngine) CheckPoolCompliance(ctx context.Context, pool *v1alpha1.StoragePool) (*PoolComplianceReport, error) {
	report := &PoolComplianceReport{
		PoolName:       pool.Name,
		ProtectionMode: pool.Spec.DataProtection.Mode,
	}

	switch pool.Spec.DataProtection.Mode {
	case "replication":
		if pool.Spec.DataProtection.Replication != nil {
			report.ReplicationFactor = pool.Spec.DataProtection.Replication.Factor
		}
	case "erasureCoding":
		if pool.Spec.DataProtection.ErasureCoding != nil {
			report.DataShards = pool.Spec.DataProtection.ErasureCoding.DataShards
			report.ParityShards = pool.Spec.DataProtection.ErasureCoding.ParityShards
		}
	}

	volumes, err := e.metaClient.ListVolumesMeta(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}

	for _, volume := range volumes {
		if volume.Pool != pool.Name {
			continue
		}
		report.TotalVolumes++

		volumeReport, err := e.CheckVolumeCompliance(ctx, volume, pool)
		if err != nil {
			return nil, fmt.Errorf("checking volume %s: %w", volume.VolumeID, err)
		}

		report.TotalChunks += volumeReport.TotalChunks
		report.CompliantChunks += volumeReport.CompliantChunks
		report.UnderReplicatedChunks += volumeReport.UnderReplicatedChunks
		report.UnavailableChunks += volumeReport.UnavailableChunks
		report.CorruptedChunks += volumeReport.CorruptedChunks
		report.VolumeReports = append(report.VolumeReports, volumeReport)
	}

	// Pool is compliant if all chunks are compliant and there are no unavailable chunks.
	report.IsCompliant = report.UnderReplicatedChunks == 0 &&
		report.UnavailableChunks == 0 &&
		report.CorruptedChunks == 0

	return report, nil
}

// CheckAllPoolsCompliance generates compliance reports for all storage pools.
// It fetches pools from Kubernetes API via the poolLookup.
func (e *PolicyEngine) CheckAllPoolsCompliance(ctx context.Context, poolLookup PoolLookup) (map[string]*PoolComplianceReport, error) {
	pools, err := poolLookup.ListPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing pools: %w", err)
	}

	reports := make(map[string]*PoolComplianceReport)
	for _, pool := range pools {
		report, err := e.CheckPoolCompliance(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("checking pool %s: %w", pool.Name, err)
		}
		reports[pool.Name] = report
	}

	return reports, nil
}

// PoolLookup retrieves storage pool configurations.
type PoolLookup interface {
	ListPools(ctx context.Context) ([]*v1alpha1.StoragePool, error)
	GetPool(ctx context.Context, name string) (*v1alpha1.StoragePool, error)
}
