// Package csi provides tests for the CSI driver implementation.
// This file tests topology-aware volume provisioning.
package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// topologyPlacer is a mock placement engine that returns specific nodes.
// It can be configured to return all available nodes (for topology filtering)
// or a specific number of nodes (for regular placement).
type topologyPlacer struct {
	nodes                 []string
	returnAllAsCandidates bool // If true, returns all nodes regardless of count
}

func (p *topologyPlacer) Place(count int) []string {
	if len(p.nodes) == 0 {
		return nil
	}
	if p.returnAllAsCandidates {
		// Return all available nodes as candidates for topology filtering.
		// In production, the placement engine would be topology-aware and
		// only return matching nodes directly.
		// We return max(count, len(nodes)) to provide all unique nodes for filtering.
		resultLen := count
		if resultLen < len(p.nodes) {
			resultLen = len(p.nodes)
		}
		result := make([]string, 0, resultLen)
		for len(result) < resultLen {
			result = append(result, p.nodes...)
		}
		return result
	}
	// Standard round-robin placement.
	result := make([]string, count)
	for i := range count {
		result[i] = p.nodes[i%len(p.nodes)]
	}
	return result
}

func (p *topologyPlacer) PlaceKey(_ string, count int) []string {
	// For testing, we use the same behavior as Place.
	// Deterministic per key is not required for topology tests.
	return p.Place(count)
}

// setupTopologyController creates a controller with a topology-aware placement engine.
func setupTopologyController(nodes []string) (*ControllerServer, *mockMetadataStore) {
	store := newMockMetadataStore()
	placer := &topologyPlacer{nodes: nodes, returnAllAsCandidates: true}
	return NewControllerServer(store, placer, nil, nil), store
}

// TestExtractTopologyRequirement tests extracting topology requirements from requests.
func TestExtractTopologyRequirement(t *testing.T) {
	tests := []struct {
		name     string
		req      *csi.CreateVolumeRequest
		expected int
	}{
		{
			name: "no accessibility requirements",
			req:  &csi.CreateVolumeRequest{Name: "test-vol"},
		},
		{
			name: "with accessibility requirements",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				AccessibilityRequirements: &csi.TopologyRequirement{
					Preferred: []*csi.Topology{
						{
							Segments: map[string]string{
								TopologyDomain: "node-1",
							},
						},
					},
				},
			},
			expected: 1,
		},
		{
			name: "multiple preferred segments",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				AccessibilityRequirements: &csi.TopologyRequirement{
					Preferred: []*csi.Topology{
						{
							Segments: map[string]string{
								TopologyDomain: "node-1",
							},
						},
						{
							Segments: map[string]string{
								TopologyDomain: "node-2",
							},
						},
					},
				},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTopologyRequirement(tt.req)
			if len(result) != tt.expected {
				t.Errorf("expected %d topology requirements, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestFilterNodesByTopology tests filtering nodes based on topology requirements.
func TestFilterNodesByTopology(t *testing.T) {
	tests := []struct {
		name          string
		nodes         []string
		required      []*csi.Topology
		expectedLen   int
		expectedNodes []string
	}{
		{
			name:        "no topology requirement returns all nodes",
			nodes:       []string{"node-1", "node-2", "node-3"},
			required:    nil,
			expectedLen: 3,
		},
		{
			name:  "filter by single topology segment",
			nodes: []string{"node-1", "node-2", "node-3"},
			required: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-2",
					},
				},
			},
			expectedLen:   1,
			expectedNodes: []string{"node-2"},
		},
		{
			name:  "filter by multiple topology segments",
			nodes: []string{"node-1", "node-2", "node-3", "node-4"},
			required: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-1",
					},
				},
				{
					Segments: map[string]string{
						TopologyDomain: "node-3",
					},
				},
			},
			expectedLen:   2,
			expectedNodes: []string{"node-1", "node-3"},
		},
		{
			name:  "no matching nodes returns all nodes (fallback)",
			nodes: []string{"node-1", "node-2", "node-3"},
			required: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-99",
					},
				},
			},
			expectedLen: 3, // Fallback: all nodes when no match
		},
		{
			name:        "empty nodes list",
			nodes:       []string{},
			required:    []*csi.Topology{},
			expectedLen: 0,
		},
		{
			name:  "topology with different domain key is ignored",
			nodes: []string{"node-1", "node-2"},
			required: []*csi.Topology{
				{
					Segments: map[string]string{
						"other.domain.com/node": "node-1",
					},
				},
			},
			expectedLen: 2, // No match on TopologyDomain, return all
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterNodesByTopology(tt.nodes, tt.required)
			if len(result) != tt.expectedLen {
				t.Errorf("expected %d nodes, got %d", tt.expectedLen, len(result))
			}
			if tt.expectedNodes != nil {
				for i, expected := range tt.expectedNodes {
					if i >= len(result) || result[i] != expected {
						t.Errorf("expected node %q at index %d, got %q", expected, i, result[i])
					}
				}
			}
		})
	}
}

// TestBuildVolumeTopology tests building volume topology from node IDs.
func TestBuildVolumeTopology(t *testing.T) {
	tests := []struct {
		name          string
		nodes         []string
		expectedLen   int
		containsNodes []string
	}{
		{
			name:        "empty nodes returns nil",
			nodes:       []string{},
			expectedLen: 0,
		},
		{
			name:          "single node",
			nodes:         []string{"node-1"},
			expectedLen:   1,
			containsNodes: []string{"node-1"},
		},
		{
			name:          "multiple unique nodes",
			nodes:         []string{"node-1", "node-2", "node-3"},
			expectedLen:   3,
			containsNodes: []string{"node-1", "node-2", "node-3"},
		},
		{
			name:          "duplicate nodes are deduplicated",
			nodes:         []string{"node-1", "node-1", "node-2", "node-2"},
			expectedLen:   2,
			containsNodes: []string{"node-1", "node-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &ControllerServer{}
			result := cs.buildVolumeTopology(tt.nodes)
			if len(result) != tt.expectedLen {
				t.Errorf("expected %d topologies, got %d", tt.expectedLen, len(result))
			}
			for _, expectedNode := range tt.containsNodes {
				found := false
				for _, topo := range result {
					if topo.Segments[TopologyDomain] == expectedNode {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected to find node %q in topology", expectedNode)
				}
			}
		})
	}
}

// TestCreateVolume_WithTopologyRequirement tests CreateVolume with topology requirements.
func TestCreateVolume_WithTopologyRequirement(t *testing.T) {
	cs, _ := setupTopologyController([]string{"node-1", "node-2", "node-3"})

	req := &csi.CreateVolumeRequest{
		Name: "topo-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024, // 1 chunk
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-2",
					},
				},
			},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume with topology failed: %v", err)
	}

	vol := resp.GetVolume()
	if vol.GetVolumeId() == "" {
		t.Fatal("expected non-empty volume ID")
	}

	// Verify accessible topology is set.
	topologies := vol.GetAccessibleTopology()
	if topologies == nil {
		t.Fatal("expected accessible topology to be set")
	}

	// With a single chunk and topology requirement for node-2,
	// we should have exactly one topology segment.
	if len(topologies) != 1 {
		t.Errorf("expected 1 topology segment, got %d", len(topologies))
	}

	// The volume should be accessible from node-2.
	nodeID, ok := topologies[0].Segments[TopologyDomain]
	if !ok {
		t.Errorf("expected topology key %q not found", TopologyDomain)
	}
	if nodeID != "node-2" {
		t.Errorf("expected volume on node-2, got %q", nodeID)
	}
}

// TestCreateVolume_WithMultipleTopologyRequirements tests CreateVolume with multiple topology preferences.
func TestCreateVolume_WithMultipleTopologyRequirements(t *testing.T) {
	cs, _ := setupTopologyController([]string{"node-1", "node-2", "node-3", "node-4"})

	req := &csi.CreateVolumeRequest{
		Name: "multi-topo-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 8 * 1024 * 1024, // 2 chunks
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-1",
					},
				},
				{
					Segments: map[string]string{
						TopologyDomain: "node-3",
					},
				},
			},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume with multiple topology requirements failed: %v", err)
	}

	vol := resp.GetVolume()
	topologies := vol.GetAccessibleTopology()
	if topologies == nil {
		t.Fatal("expected accessible topology to be set")
	}

	// Should have topology segments for node-1 and node-3 (the filtered nodes).
	if len(topologies) != 2 {
		t.Errorf("expected 2 topology segments, got %d", len(topologies))
	}

	// Verify we have the expected nodes.
	nodes := make(map[string]bool)
	for _, topo := range topologies {
		nodeID := topo.Segments[TopologyDomain]
		nodes[nodeID] = true
	}

	if !nodes["node-1"] {
		t.Error("expected node-1 in accessible topology")
	}
	if !nodes["node-3"] {
		t.Error("expected node-3 in accessible topology")
	}
}

// TestCreateVolume_WithoutTopologyRequirement tests CreateVolume without topology requirements.
func TestCreateVolume_WithoutTopologyRequirement(t *testing.T) {
	cs, _ := setupTopologyController([]string{"node-1", "node-2"})

	req := &csi.CreateVolumeRequest{
		Name: "no-topo-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		// No AccessibilityRequirements
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume without topology failed: %v", err)
	}

	vol := resp.GetVolume()
	if vol.GetVolumeId() == "" {
		t.Fatal("expected non-empty volume ID")
	}

	// Accessible topology should still be populated with the placement nodes.
	topologies := vol.GetAccessibleTopology()
	if topologies == nil {
		t.Fatal("expected accessible topology to be set")
	}

	if len(topologies) == 0 {
		t.Error("expected at least one topology segment")
	}
}

// TestCreateVolume_TopologyNoMatchFallback tests that CreateVolume falls back to all nodes
// when topology requirement cannot be satisfied.
func TestCreateVolume_TopologyNoMatchFallback(t *testing.T) {
	cs, _ := setupTopologyController([]string{"node-1", "node-2"})

	req := &csi.CreateVolumeRequest{
		Name: "fallback-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-99", // Non-existent node
					},
				},
			},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume with non-matching topology failed: %v", err)
	}

	// Should succeed by falling back to available nodes.
	vol := resp.GetVolume()
	if vol.GetVolumeId() == "" {
		t.Fatal("expected non-empty volume ID")
	}

	// Volume should be accessible from the available nodes.
	topologies := vol.GetAccessibleTopology()
	if len(topologies) == 0 {
		t.Error("expected accessible topology from fallback nodes")
	}
}

// TestCreateVolume_TopologyWithRWX tests that topology works with RWX volumes.
func TestCreateVolume_TopologyWithRWX(t *testing.T) {
	cs, _ := setupTopologyController([]string{"node-1", "node-2"})

	req := &csi.CreateVolumeRequest{
		Name: "rwx-topo-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-1",
					},
				},
			},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume RWX with topology failed: %v", err)
	}

	vol := resp.GetVolume()
	volCtx := vol.GetVolumeContext()

	// Verify NFS context is set.
	if volCtx["accessMode"] != "RWX" {
		t.Errorf("expected accessMode RWX, got %q", volCtx["accessMode"])
	}

	// Verify topology is respected.
	topologies := vol.GetAccessibleTopology()
	if len(topologies) == 0 {
		t.Fatal("expected accessible topology for RWX volume")
	}

	nodeID := topologies[0].Segments[TopologyDomain]
	if nodeID != "node-1" {
		t.Errorf("expected RWX volume on node-1, got %q", nodeID)
	}
}

// TestCreateVolume_TopologyDeduplication tests that duplicate node placements
// result in a single topology segment.
func TestCreateVolume_TopologyDeduplication(t *testing.T) {
	cs, _ := setupTopologyController([]string{"node-1", "node-2"})

	// Request a large volume that results in multiple chunks.
	// With only 2 nodes, chunks will be distributed across them.
	req := &csi.CreateVolumeRequest{
		Name: "dedup-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 16 * 1024 * 1024, // 4 chunks
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume with multiple chunks failed: %v", err)
	}

	vol := resp.GetVolume()
	topologies := vol.GetAccessibleTopology()

	// Should have exactly 2 unique topology segments (one per node).
	if len(topologies) != 2 {
		t.Errorf("expected 2 unique topology segments, got %d", len(topologies))
	}

	// Verify deduplication: no duplicate node IDs.
	nodes := make(map[string]bool)
	for _, topo := range topologies {
		nodeID := topo.Segments[TopologyDomain]
		if nodes[nodeID] {
			t.Errorf("duplicate node ID in topology: %s", nodeID)
		}
		nodes[nodeID] = true
	}
}

// TestCreateVolume_TopologyNoNodesAvailable tests error when no nodes are available.
func TestCreateVolume_TopologyNoNodesAvailable(t *testing.T) {
	store := newMockMetadataStore()
	placer := &topologyPlacer{nodes: []string{}} // No nodes
	cs := NewControllerServer(store, placer, nil, nil)

	req := &csi.CreateVolumeRequest{
		Name: "no-nodes-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyDomain: "node-1",
					},
				},
			},
		},
	}

	_, err := cs.CreateVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when no nodes available")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}

	if st.Code() != codes.ResourceExhausted {
		t.Errorf("expected ResourceExhausted, got %v", st.Code())
	}
}
