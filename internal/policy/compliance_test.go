package policy

import (
	"context"
	"testing"

	"github.com/azrtydxb/novastor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// mockNodeChecker is a test double for NodeAvailabilityChecker.
type mockNodeChecker struct {
	availableNodes map[string]bool
}

func (m *mockNodeChecker) IsNodeAvailable(_ context.Context, nodeID string) bool {
	if m.availableNodes == nil {
		return true
	}
	return m.availableNodes[nodeID]
}

// mockMetadataClient is a test double for metadata.GRPCClient.
type mockMetadataClient struct {
	volumes       map[string]*VolumeMeta
	placementMaps map[string]*PlacementMap
}

func (m *mockMetadataClient) GetVolumeMeta(_ context.Context, volumeID string) (*VolumeMeta, error) {
	if m.volumes == nil {
		return nil, nil
	}
	return m.volumes[volumeID], nil
}

func (m *mockMetadataClient) ListVolumesMeta(_ context.Context) ([]*VolumeMeta, error) {
	if m.volumes == nil {
		return nil, nil
	}
	var result []*VolumeMeta
	for _, v := range m.volumes {
		result = append(result, v)
	}
	return result, nil
}

func (m *mockMetadataClient) GetPlacementMap(_ context.Context, chunkID string) (*PlacementMap, error) {
	if m.placementMaps == nil {
		return nil, nil
	}
	return m.placementMaps[chunkID], nil
}

func (m *mockMetadataClient) PutPlacementMap(_ context.Context, pm *PlacementMap) error {
	if m.placementMaps == nil {
		m.placementMaps = make(map[string]*PlacementMap)
	}
	m.placementMaps[pm.ChunkID] = pm
	return nil
}

func (m *mockMetadataClient) ListPlacementMaps(_ context.Context) ([]*PlacementMap, error) {
	if m.placementMaps == nil {
		return nil, nil
	}
	var result []*PlacementMap
	for _, pm := range m.placementMaps {
		result = append(result, pm)
	}
	return result, nil
}

func (m *mockMetadataClient) ListNodeMetas(_ context.Context) ([]*NodeMeta, error) {
	// Return empty list for now
	return []*NodeMeta{}, nil
}

// mockPoolLookup is a test double for PoolLookup.
type mockPoolLookup struct {
	pools map[string]*v1alpha1.StoragePool
}

func (m *mockPoolLookup) ListPools(_ context.Context) ([]*v1alpha1.StoragePool, error) {
	if m.pools == nil {
		return nil, nil
	}
	var result []*v1alpha1.StoragePool
	for _, p := range m.pools {
		result = append(result, p)
	}
	return result, nil
}

func (m *mockPoolLookup) GetPool(_ context.Context, name string) (*v1alpha1.StoragePool, error) {
	if m.pools == nil {
		return nil, nil
	}
	return m.pools[name], nil
}

func TestReplicationChecker_CheckChunk(t *testing.T) {
	tests := []struct {
		name           string
		volume         *VolumeMeta
		placement      *PlacementMap
		availableNodes map[string]bool
		wantStatus     ComplianceStatus
		wantAvailable  int
		wantFailed     int
	}{
		{
			name: "fully compliant - all replicas available",
			volume: &VolumeMeta{
				VolumeID: "vol1",
				Pool:     "test-pool",
				ChunkIDs: []string{"chunk1"},
				DataProtection: &v1alpha1.DataProtectionSpec{
					Mode: "replication",
					Replication: &v1alpha1.ReplicationSpec{
						Factor: 3,
					},
				},
			},
			placement: &PlacementMap{
				ChunkID: "chunk1",
				Nodes:   []string{"node1", "node2", "node3"},
			},
			availableNodes: map[string]bool{
				"node1": true,
				"node2": true,
				"node3": true,
			},
			wantStatus:    StatusCompliant,
			wantAvailable: 3,
			wantFailed:    0,
		},
		{
			name: "under replicated - one replica down",
			volume: &VolumeMeta{
				VolumeID: "vol1",
				Pool:     "test-pool",
				ChunkIDs: []string{"chunk1"},
				DataProtection: &v1alpha1.DataProtectionSpec{
					Mode: "replication",
					Replication: &v1alpha1.ReplicationSpec{
						Factor: 3,
					},
				},
			},
			placement: &PlacementMap{
				ChunkID: "chunk1",
				Nodes:   []string{"node1", "node2", "node3"},
			},
			availableNodes: map[string]bool{
				"node1": true,
				"node2": true,
				"node3": false,
			},
			wantStatus:    StatusUnderReplicated,
			wantAvailable: 2,
			wantFailed:    1,
		},
		{
			name: "unavailable - all replicas down",
			volume: &VolumeMeta{
				VolumeID: "vol1",
				Pool:     "test-pool",
				ChunkIDs: []string{"chunk1"},
				DataProtection: &v1alpha1.DataProtectionSpec{
					Mode: "replication",
					Replication: &v1alpha1.ReplicationSpec{
						Factor: 3,
					},
				},
			},
			placement: &PlacementMap{
				ChunkID: "chunk1",
				Nodes:   []string{"node1", "node2", "node3"},
			},
			availableNodes: map[string]bool{
				"node1": false,
				"node2": false,
				"node3": false,
			},
			wantStatus:    StatusUnavailable,
			wantAvailable: 0,
			wantFailed:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			nodeChecker := &mockNodeChecker{availableNodes: tt.availableNodes}
			metaClient := &mockMetadataClient{
				placementMaps: map[string]*PlacementMap{
					tt.placement.ChunkID: tt.placement,
				},
			}

			checker := NewReplicationChecker(metaClient, nodeChecker)

			result, err := checker.CheckChunk(ctx, tt.placement.ChunkID, tt.volume)
			if err != nil {
				t.Fatalf("CheckChunk failed: %v", err)
			}

			if result.Status != tt.wantStatus {
				t.Errorf("Status = %v, want %v", result.Status, tt.wantStatus)
			}
			if len(result.AvailableNodes) != tt.wantAvailable {
				t.Errorf("AvailableNodes count = %d, want %d", len(result.AvailableNodes), tt.wantAvailable)
			}
			if len(result.FailedNodes) != tt.wantFailed {
				t.Errorf("FailedNodes count = %d, want %d", len(result.FailedNodes), tt.wantFailed)
			}
		})
	}
}

func TestErasureCodingChecker_CheckChunk(t *testing.T) {
	tests := []struct {
		name           string
		volume         *VolumeMeta
		placement      *PlacementMap
		availableNodes map[string]bool
		wantStatus     ComplianceStatus
		wantAvailable  int
	}{
		{
			name: "fully compliant - all shards available",
			volume: &VolumeMeta{
				VolumeID: "vol1",
				Pool:     "ec-pool",
				ChunkIDs: []string{"chunk1"},
				DataProtection: &v1alpha1.DataProtectionSpec{
					Mode: "erasureCoding",
					ErasureCoding: &v1alpha1.ErasureCodingSpec{
						DataShards:   4,
						ParityShards: 2,
					},
				},
			},
			placement: &PlacementMap{
				ChunkID: "chunk1",
				Nodes:   []string{"node1", "node2", "node3", "node4", "node5", "node6"},
			},
			availableNodes: map[string]bool{
				"node1": true,
				"node2": true,
				"node3": true,
				"node4": true,
				"node5": true,
				"node6": true,
			},
			wantStatus:    StatusCompliant,
			wantAvailable: 6,
		},
		{
			name: "under replicated - some shards lost but still recoverable",
			volume: &VolumeMeta{
				VolumeID: "vol1",
				Pool:     "ec-pool",
				ChunkIDs: []string{"chunk1"},
				DataProtection: &v1alpha1.DataProtectionSpec{
					Mode: "erasureCoding",
					ErasureCoding: &v1alpha1.ErasureCodingSpec{
						DataShards:   4,
						ParityShards: 2,
					},
				},
			},
			placement: &PlacementMap{
				ChunkID: "chunk1",
				Nodes:   []string{"node1", "node2", "node3", "node4", "node5", "node6"},
			},
			availableNodes: map[string]bool{
				"node1": true,
				"node2": true,
				"node3": true,
				"node4": true,
				"node5": false,
				"node6": false,
			},
			wantStatus:    StatusUnderReplicated,
			wantAvailable: 4,
		},
		{
			name: "unavailable - insufficient shards for recovery",
			volume: &VolumeMeta{
				VolumeID: "vol1",
				Pool:     "ec-pool",
				ChunkIDs: []string{"chunk1"},
				DataProtection: &v1alpha1.DataProtectionSpec{
					Mode: "erasureCoding",
					ErasureCoding: &v1alpha1.ErasureCodingSpec{
						DataShards:   4,
						ParityShards: 2,
					},
				},
			},
			placement: &PlacementMap{
				ChunkID: "chunk1",
				Nodes:   []string{"node1", "node2", "node3", "node4", "node5", "node6"},
			},
			availableNodes: map[string]bool{
				"node1": true,
				"node2": true,
				"node3": false,
				"node4": false,
				"node5": false,
				"node6": false,
			},
			wantStatus:    StatusUnavailable,
			wantAvailable: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			nodeChecker := &mockNodeChecker{availableNodes: tt.availableNodes}
			metaClient := &mockMetadataClient{
				placementMaps: map[string]*PlacementMap{
					tt.placement.ChunkID: tt.placement,
				},
			}

			checker := NewErasureCodingChecker(metaClient, nodeChecker)

			result, err := checker.CheckChunk(ctx, tt.placement.ChunkID, tt.volume)
			if err != nil {
				t.Fatalf("CheckChunk failed: %v", err)
			}

			if result.Status != tt.wantStatus {
				t.Errorf("Status = %v, want %v", result.Status, tt.wantStatus)
			}
			if len(result.AvailableNodes) != tt.wantAvailable {
				t.Errorf("AvailableNodes count = %d, want %d", len(result.AvailableNodes), tt.wantAvailable)
			}
		})
	}
}

func TestPolicyEngine_CheckVolumeCompliance(t *testing.T) {
	ctx := context.Background()

	volume := &VolumeMeta{
		VolumeID: "vol1",
		Pool:     "replica-pool",
		ChunkIDs: []string{"chunk1", "chunk2", "chunk3"},
		DataProtection: &v1alpha1.DataProtectionSpec{
			Mode: "replication",
			Replication: &v1alpha1.ReplicationSpec{
				Factor: 3,
			},
		},
	}

	nodeChecker := &mockNodeChecker{
		availableNodes: map[string]bool{
			"node1": true,
			"node2": true,
			"node3": false, // chunk1 has a failed replica
		},
	}

	metaClient := &mockMetadataClient{
		volumes: map[string]*VolumeMeta{
			volume.VolumeID: volume,
		},
		placementMaps: map[string]*PlacementMap{
			"chunk1": {ChunkID: "chunk1", Nodes: []string{"node1", "node2", "node3"}},
			"chunk2": {ChunkID: "chunk2", Nodes: []string{"node1", "node2", "node3"}},
			"chunk3": {ChunkID: "chunk3", Nodes: []string{"node1", "node2", "node3"}},
		},
	}

	engine := NewPolicyEngine(metaClient, nodeChecker)

	report, err := engine.CheckVolumeCompliance(ctx, volume)
	if err != nil {
		t.Fatalf("CheckVolumeCompliance failed: %v", err)
	}

	if report.VolumeID != volume.VolumeID {
		t.Errorf("VolumeID = %s, want %s", report.VolumeID, volume.VolumeID)
	}
	if report.Pool != volume.Pool {
		t.Errorf("Pool = %s, want %s", report.Pool, volume.Pool)
	}
	if report.TotalChunks != 3 {
		t.Errorf("TotalChunks = %d, want 3", report.TotalChunks)
	}
	// All 3 chunks have node3 unavailable, so all are under-replicated
	if report.CompliantChunks != 0 {
		t.Errorf("CompliantChunks = %d, want 0", report.CompliantChunks)
	}
	if report.UnderReplicatedChunks != 3 {
		t.Errorf("UnderReplicatedChunks = %d, want 3", report.UnderReplicatedChunks)
	}
}

func TestPolicyEngine_CheckPoolCompliance(t *testing.T) {
	ctx := context.Background()

	pool := &v1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pool"},
		Spec:       v1alpha1.StoragePoolSpec{},
	}

	volumes := map[string]*VolumeMeta{
		"vol1": {
			VolumeID: "vol1",
			Pool:     "test-pool",
			ChunkIDs: []string{"chunk1", "chunk2"},
			DataProtection: &v1alpha1.DataProtectionSpec{
				Mode: "replication",
				Replication: &v1alpha1.ReplicationSpec{
					Factor: 3,
				},
			},
		},
		"vol2": {
			VolumeID: "vol2",
			Pool:     "test-pool",
			ChunkIDs: []string{"chunk3"},
			DataProtection: &v1alpha1.DataProtectionSpec{
				Mode: "replication",
				Replication: &v1alpha1.ReplicationSpec{
					Factor: 3,
				},
			},
		},
	}

	nodeChecker := &mockNodeChecker{
		availableNodes: map[string]bool{
			"node1": true,
			"node2": true,
			"node3": true,
		},
	}

	metaClient := &mockMetadataClient{
		volumes: volumes,
		placementMaps: map[string]*PlacementMap{
			"chunk1": {ChunkID: "chunk1", Nodes: []string{"node1", "node2", "node3"}},
			"chunk2": {ChunkID: "chunk2", Nodes: []string{"node1", "node2", "node3"}},
			"chunk3": {ChunkID: "chunk3", Nodes: []string{"node1", "node2", "node3"}},
		},
	}

	engine := NewPolicyEngine(metaClient, nodeChecker)

	report, err := engine.CheckPoolCompliance(ctx, pool)
	if err != nil {
		t.Fatalf("CheckPoolCompliance failed: %v", err)
	}

	if report.PoolName != pool.Name {
		t.Errorf("PoolName = %s, want %s", report.PoolName, pool.Name)
	}
	if report.ProtectionMode != "replication" {
		t.Errorf("ProtectionMode = %s, want replication", report.ProtectionMode)
	}
	if report.TotalVolumes != 2 {
		t.Errorf("TotalVolumes = %d, want 2", report.TotalVolumes)
	}
	if report.TotalChunks != 3 {
		t.Errorf("TotalChunks = %d, want 3", report.TotalChunks)
	}
	if !report.IsCompliant {
		t.Errorf("IsCompliant = false, want true (all chunks should be compliant)")
	}
}

func TestReconciler_ScanAndEnqueue(t *testing.T) {
	ctx := context.Background()

	pool := &v1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pool"},
		Spec:       v1alpha1.StoragePoolSpec{},
	}

	volume := &VolumeMeta{
		VolumeID: "vol1",
		Pool:     "test-pool",
		ChunkIDs: []string{"chunk1", "chunk2"},
		DataProtection: &v1alpha1.DataProtectionSpec{
			Mode: "replication",
			Replication: &v1alpha1.ReplicationSpec{
				Factor: 3,
			},
		},
	}

	nodeChecker := &mockNodeChecker{
		availableNodes: map[string]bool{
			"node1": true,
			"node2": true,
			"node3": false, // One replica is down
		},
	}

	poolLookup := &mockPoolLookup{
		pools: map[string]*v1alpha1.StoragePool{
			"test-pool": pool,
		},
	}

	metaClient := &mockMetadataClient{
		volumes: map[string]*VolumeMeta{
			"vol1": volume,
		},
		placementMaps: map[string]*PlacementMap{
			"chunk1": {ChunkID: "chunk1", Nodes: []string{"node1", "node2", "node3"}},
			"chunk2": {ChunkID: "chunk2", Nodes: []string{"node1", "node2", "node3"}},
		},
	}

	reconciler := NewReconciler(metaClient, nodeChecker, poolLookup)

	summary, err := reconciler.ScanAndEnqueue(ctx)
	if err != nil {
		t.Fatalf("ScanAndEnqueue failed: %v", err)
	}

	if summary.TasksQueued != 2 {
		t.Errorf("TasksQueued = %d, want 2 (both chunks need repair)", summary.TasksQueued)
	}

	if reconciler.QueueSize() != 2 {
		t.Errorf("QueueSize = %d, want 2", reconciler.QueueSize())
	}
}

func TestComplianceStatus_String(t *testing.T) {
	tests := []struct {
		status ComplianceStatus
		want   string
	}{
		{StatusCompliant, "compliant"},
		{StatusUnderReplicated, "under_replicated"},
		{StatusUnavailable, "unavailable"},
		{StatusCorrupted, "corrupted"},
		{ComplianceStatus(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("String() = %s, want %s", got, tt.want)
			}
		})
	}
}
