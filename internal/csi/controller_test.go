package csi

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/piwi3910/novastor/internal/metadata"
)

const (
	accessTypeBlock  = "block"
	accessTypeNVMeoF = "nvmeof"
)

// --- mock metadata store ---

type mockMetadataStore struct {
	mu      sync.Mutex
	volumes map[string]*metadata.VolumeMeta
}

func newMockMetadataStore() *mockMetadataStore {
	return &mockMetadataStore{volumes: make(map[string]*metadata.VolumeMeta)}
}

func (m *mockMetadataStore) PutVolumeMeta(_ context.Context, meta *metadata.VolumeMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volumes[meta.VolumeID] = meta
	return nil
}

func (m *mockMetadataStore) GetVolumeMeta(_ context.Context, volumeID string) (*metadata.VolumeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vm, ok := m.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("volume %s not found", volumeID)
	}
	return vm, nil
}

func (m *mockMetadataStore) DeleteVolumeMeta(_ context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.volumes, volumeID)
	return nil
}

func (m *mockMetadataStore) ListVolumesMeta(_ context.Context) ([]*metadata.VolumeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*metadata.VolumeMeta, 0, len(m.volumes))
	for _, v := range m.volumes {
		result = append(result, v)
	}
	return result, nil
}

func (m *mockMetadataStore) PutPlacementMap(_ context.Context, _ *metadata.PlacementMap) error {
	// No-op for tests: placement maps are not tested in the mock.
	return nil
}

func (m *mockMetadataStore) DeletePlacementMap(_ context.Context, _ string) error {
	// No-op for tests: placement maps are not tested in the mock.
	return nil
}

// --- mock placement engine ---

type mockPlacer struct {
	nodes []string
}

func (p *mockPlacer) Place(count int) []string {
	if len(p.nodes) == 0 {
		return nil
	}
	result := make([]string, count)
	for i := range count {
		result[i] = p.nodes[i%len(p.nodes)]
	}
	return result
}

func (p *mockPlacer) PlaceKey(_ string, count int) []string {
	// For testing, we use the same behavior as Place.
	// Deterministic per key is not required for these tests.
	return p.Place(count)
}

// --- helpers ---

func setupController() (*ControllerServer, *mockMetadataStore) {
	store := newMockMetadataStore()
	placer := &mockPlacer{nodes: []string{"node-1", "node-2", "node-3"}}
	return NewControllerServer(store, placer, nil, nil), store
}

// --- CreateVolume tests ---

func TestCreateVolume(t *testing.T) {
	cs, store := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "test-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 8 * 1024 * 1024, // 8 MiB = 2 chunks
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vol := resp.GetVolume()
	if vol.GetVolumeId() == "" {
		t.Fatal("expected non-empty volume ID")
	}
	if vol.GetCapacityBytes() != 8*1024*1024 {
		t.Errorf("expected capacity 8MiB, got %d", vol.GetCapacityBytes())
	}
	// Verify metadata was stored.
	vm, err := store.GetVolumeMeta(context.Background(), vol.GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found after create: %v", err)
	}
	if len(vm.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(vm.ChunkIDs))
	}
}

func TestCreateVolumeNoName(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{})
	if err == nil {
		t.Fatal("expected error for missing volume name")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateVolumeDefaultSize(t *testing.T) {
	cs, store := setupController()
	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "default-size"})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vm, err := store.GetVolumeMeta(context.Background(), resp.GetVolume().GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	expectedChunks := int((defaultVolumeSize + chunkSize - 1) / chunkSize)
	if len(vm.ChunkIDs) != expectedChunks {
		t.Errorf("expected %d chunks for default 1GiB volume, got %d", expectedChunks, len(vm.ChunkIDs))
	}
}

func TestCreateVolume_RWX(t *testing.T) {
	cs, _ := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "rwx-vol",
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
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume RWX failed: %v", err)
	}

	vol := resp.GetVolume()
	volCtx := vol.GetVolumeContext()
	if volCtx["accessMode"] != "RWX" {
		t.Errorf("expected accessMode RWX in volume context, got %q", volCtx["accessMode"])
	}
	if volCtx["nfsServer"] == "" {
		t.Error("expected nfsServer in volume context for RWX volume")
	}
	if volCtx["nfsShare"] == "" {
		t.Error("expected nfsShare in volume context for RWX volume")
	}
}

func TestCreateVolumeNoNodes(t *testing.T) {
	store := newMockMetadataStore()
	placer := &mockPlacer{nodes: nil}
	cs := NewControllerServer(store, placer, nil, nil)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "no-nodes",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
	})
	if err == nil {
		t.Fatal("expected error when no nodes available")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.ResourceExhausted {
		t.Errorf("expected ResourceExhausted, got %v", err)
	}
}

// --- Data protection parameter tests ---

func TestCreateVolume_DefaultProtection(t *testing.T) {
	cs, store := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "default-protection",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vm, err := store.GetVolumeMeta(context.Background(), resp.GetVolume().GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	if vm.DataProtection == nil {
		t.Fatal("expected DataProtection to be set")
	}
	if vm.DataProtection.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected default mode replication, got %s", vm.DataProtection.Mode)
	}
	if vm.DataProtection.Replication == nil {
		t.Fatal("expected Replication config to be set")
	}
	if vm.DataProtection.Replication.Factor != 3 {
		t.Errorf("expected default replication factor 3, got %d", vm.DataProtection.Replication.Factor)
	}
	if vm.DataProtection.Replication.WriteQuorum != 2 {
		t.Errorf("expected default write quorum 2, got %d", vm.DataProtection.Replication.WriteQuorum)
	}
}

func TestCreateVolume_ReplicationParams(t *testing.T) {
	cs, store := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "replication-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection":  "replication",
			"replicas":    "5",
			"writeQuorum": "3",
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vm, err := store.GetVolumeMeta(context.Background(), resp.GetVolume().GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	if vm.DataProtection.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected mode replication, got %s", vm.DataProtection.Mode)
	}
	if vm.DataProtection.Replication.Factor != 5 {
		t.Errorf("expected replication factor 5, got %d", vm.DataProtection.Replication.Factor)
	}
	if vm.DataProtection.Replication.WriteQuorum != 3 {
		t.Errorf("expected write quorum 3, got %d", vm.DataProtection.Replication.WriteQuorum)
	}
}

func TestCreateVolume_ErasureCodingDefaults(t *testing.T) {
	cs, store := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "ec-default-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection": "erasure-coding",
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vm, err := store.GetVolumeMeta(context.Background(), resp.GetVolume().GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	if vm.DataProtection.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected mode erasure-coding, got %s", vm.DataProtection.Mode)
	}
	if vm.DataProtection.ErasureCoding == nil {
		t.Fatal("expected ErasureCoding config to be set")
	}
	if vm.DataProtection.ErasureCoding.DataShards != 4 {
		t.Errorf("expected default dataShards 4, got %d", vm.DataProtection.ErasureCoding.DataShards)
	}
	if vm.DataProtection.ErasureCoding.ParityShards != 2 {
		t.Errorf("expected default parityShards 2, got %d", vm.DataProtection.ErasureCoding.ParityShards)
	}
}

func TestCreateVolume_ErasureCodingCustom(t *testing.T) {
	cs, store := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "ec-custom-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 8 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection":   "erasure-coding",
			"dataShards":   "8",
			"parityShards": "3",
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vm, err := store.GetVolumeMeta(context.Background(), resp.GetVolume().GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	if vm.DataProtection.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected mode erasure-coding, got %s", vm.DataProtection.Mode)
	}
	if vm.DataProtection.ErasureCoding.DataShards != 8 {
		t.Errorf("expected dataShards 8, got %d", vm.DataProtection.ErasureCoding.DataShards)
	}
	if vm.DataProtection.ErasureCoding.ParityShards != 3 {
		t.Errorf("expected parityShards 3, got %d", vm.DataProtection.ErasureCoding.ParityShards)
	}
	// Verify chunk count is based on volume size, not EC overhead
	// (EC overhead is handled at chunk encoding level)
	if len(vm.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunks for 8MiB volume, got %d", len(vm.ChunkIDs))
	}
}

func TestCreateVolume_InvalidProtectionMode(t *testing.T) {
	cs, _ := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "invalid-mode",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection": "invalid-mode",
		},
	}

	_, err := cs.CreateVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid protection mode")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateVolume_InvalidReplicas(t *testing.T) {
	cs, _ := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "invalid-replicas",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection": "replication",
			"replicas":   "invalid",
		},
	}

	_, err := cs.CreateVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid replicas value")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateVolume_InvalidDataShards(t *testing.T) {
	cs, _ := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "invalid-data-shards",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection": "erasure-coding",
			"dataShards": "1",
		},
	}

	_, err := cs.CreateVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for dataShards < 2")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateVolume_RWXWithErasureCoding(t *testing.T) {
	cs, store := setupController()
	req := &csi.CreateVolumeRequest{
		Name: "rwx-ec-vol",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
		Parameters: map[string]string{
			"protection":   "erasure-coding",
			"dataShards":   "6",
			"parityShards": "2",
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
	}

	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	vm, err := store.GetVolumeMeta(context.Background(), resp.GetVolume().GetVolumeId())
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	// Verify both RWX context and EC config are set
	if vm.DataProtection.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected mode erasure-coding, got %s", vm.DataProtection.Mode)
	}
	if vm.DataProtection.ErasureCoding.DataShards != 6 {
		t.Errorf("expected dataShards 6, got %d", vm.DataProtection.ErasureCoding.DataShards)
	}

	volCtx := resp.GetVolume().GetVolumeContext()
	if volCtx["accessMode"] != "RWX" {
		t.Errorf("expected accessMode RWX in volume context, got %q", volCtx["accessMode"])
	}
}

// --- DeleteVolume tests ---

func TestDeleteVolume(t *testing.T) {
	cs, store := setupController()

	// Create a volume first.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "del-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volID := createResp.GetVolume().GetVolumeId()

	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: volID})
	if err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}

	// Verify removed from store.
	if _, err := store.GetVolumeMeta(context.Background(), volID); err == nil {
		t.Error("expected volume metadata to be deleted")
	}
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	cs, _ := setupController()
	// Deleting a non-existent volume should succeed per CSI spec.
	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "does-not-exist"})
	if err != nil {
		t.Fatalf("expected idempotent delete to succeed, got: %v", err)
	}
}

func TestDeleteVolumeNoID(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

// --- ValidateVolumeCapabilities tests ---

func TestValidateVolumeCapabilities_RWO(t *testing.T) {
	cs, _ := setupController()

	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "validate-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: createResp.GetVolume().GetVolumeId(),
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
	}
	if resp.GetConfirmed() == nil {
		t.Error("expected confirmed capabilities for RWO")
	}
}

func TestValidateVolumeCapabilities_RWX(t *testing.T) {
	cs, _ := setupController()

	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "validate-rwx",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: createResp.GetVolume().GetVolumeId(),
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
	}
	if resp.GetConfirmed() == nil {
		t.Error("expected confirmed capabilities for RWX")
	}
}

func TestValidateVolumeCapabilities_Unsupported(t *testing.T) {
	cs, _ := setupController()

	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "validate-unsupported",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: createResp.GetVolume().GetVolumeId(),
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
	}
	if resp.GetConfirmed() != nil {
		t.Error("expected nil confirmed for unsupported access mode")
	}
	if resp.GetMessage() == "" {
		t.Error("expected non-empty message for unsupported access mode")
	}
}

func TestValidateVolumeCapabilities_NotFound(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "nonexistent",
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for non-existent volume")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

// --- ListVolumes tests ---

func TestListVolumes_Empty(t *testing.T) {
	cs, _ := setupController()
	resp, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Errorf("expected 0 entries for empty store, got %d", len(resp.GetEntries()))
	}
	if resp.GetNextToken() != "" {
		t.Errorf("expected empty next token, got %q", resp.GetNextToken())
	}
}

func TestListVolumes_AllEntries(t *testing.T) {
	cs, _ := setupController()

	for _, name := range []string{"vol-a", "vol-b", "vol-c"} {
		if _, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:          name,
			CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
		}); err != nil {
			t.Fatalf("CreateVolume %s failed: %v", name, err)
		}
	}

	resp, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetEntries()) != 3 {
		t.Errorf("expected 3 entries, got %d", len(resp.GetEntries()))
	}
	if resp.GetNextToken() != "" {
		t.Errorf("expected empty next token for full listing, got %q", resp.GetNextToken())
	}
	for _, entry := range resp.GetEntries() {
		if entry.GetVolume().GetVolumeId() == "" {
			t.Error("expected non-empty volume ID in list entry")
		}
		if entry.GetVolume().GetCapacityBytes() != 4*1024*1024 {
			t.Errorf("expected capacity 4MiB, got %d", entry.GetVolume().GetCapacityBytes())
		}
	}
}

func TestListVolumes_MaxEntries(t *testing.T) {
	cs, _ := setupController()

	for _, name := range []string{"page-a", "page-b", "page-c"} {
		_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:          name,
			CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
		})
		if err != nil {
			t.Fatalf("CreateVolume %s failed: %v", name, err)
		}
	}

	resp, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	if err != nil {
		t.Fatalf("ListVolumes with MaxEntries failed: %v", err)
	}
	if len(resp.GetEntries()) != 2 {
		t.Errorf("expected 2 entries with MaxEntries=2, got %d", len(resp.GetEntries()))
	}
	if resp.GetNextToken() == "" {
		t.Error("expected non-empty next token when page is truncated")
	}
}

func TestListVolumes_InvalidToken(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		StartingToken: "nonexistent-volume-id",
	})
	if err == nil {
		t.Fatal("expected error for invalid starting_token")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Aborted {
		t.Errorf("expected Aborted for invalid token, got %v", err)
	}
}

// --- ControllerPublishVolume tests ---

func TestControllerPublishVolume_Success(t *testing.T) {
	cs, _ := setupController()

	// Create a volume first.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "publish-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volumeID := createResp.GetVolume().GetVolumeId()

	// Publish the volume to a node.
	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume failed: %v", err)
	}

	// Check publish context contains required fields.
	pubCtx := resp.GetPublishContext()
	if pubCtx["volumeId"] != volumeID {
		t.Errorf("expected volumeId %s in publish context, got %q", volumeID, pubCtx["volumeId"])
	}
	if pubCtx["accessType"] != accessTypeBlock {
		t.Errorf("expected accessType block, got %q", pubCtx["accessType"])
	}
}

func TestControllerPublishVolume_RWX(t *testing.T) {
	cs, _ := setupController()

	// Create an RWX volume.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "publish-rwx",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volumeID := createResp.GetVolume().GetVolumeId()

	// Publish the RWX volume.
	// Note: ControllerPublishVolume doesn't need VolumeCapabilities in the request
	// since the access type is determined from the volume metadata.
	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume RWX failed: %v", err)
	}

	// RWX volumes without NVMe-oF targets get accessTypeBlock by default
	// since the node will handle NFS mounting based on volume context.
	pubCtx := resp.GetPublishContext()
	if pubCtx["accessType"] != accessTypeBlock {
		t.Errorf("expected accessType block, got %q", pubCtx["accessType"])
	}
	if pubCtx["volumeId"] != volumeID {
		t.Errorf("expected volumeId %s, got %q", volumeID, pubCtx["volumeId"])
	}
}

func TestControllerPublishVolume_NoVolumeID(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		NodeId: "node-1",
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestControllerPublishVolume_NoNodeID(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "some-volume",
	})
	if err == nil {
		t.Fatal("expected error for missing node ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestControllerPublishVolume_NotFound(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "nonexistent",
		NodeId:   "node-1",
	})
	if err == nil {
		t.Fatal("expected error for non-existent volume")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestControllerPublishVolume_Idempotent(t *testing.T) {
	cs, _ := setupController()

	// Create a volume.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "idempotent-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volumeID := createResp.GetVolume().GetVolumeId()

	// Publish twice - both should succeed.
	_, err = cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}

	_, err = cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("second publish failed: %v", err)
	}
}

// --- ControllerUnpublishVolume tests ---

func TestControllerUnpublishVolume_Success(t *testing.T) {
	cs, _ := setupController()

	// Create a volume first.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "unpublish-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volumeID := createResp.GetVolume().GetVolumeId()

	// Publish first.
	_, err = cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume failed: %v", err)
	}

	// Unpublish should succeed.
	_, err = cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("ControllerUnpublishVolume failed: %v", err)
	}
}

func TestControllerUnpublishVolume_NoVolumeID(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestControllerUnpublishVolume_Idempotent(t *testing.T) {
	cs, _ := setupController()

	// Create a volume.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "unpublish-idempotent",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volumeID := createResp.GetVolume().GetVolumeId()

	// Unpublish multiple times - all should succeed.
	_, err = cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("first unpublish failed: %v", err)
	}

	_, err = cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("second unpublish failed: %v", err)
	}
}

func TestControllerUnpublishVolume_NonexistentVolume(t *testing.T) {
	cs, _ := setupController()
	// Unpublishing a non-existent volume should succeed idempotently.
	_, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "does-not-exist",
	})
	if err != nil {
		t.Fatalf("expected idempotent unpublish to succeed, got: %v", err)
	}
}

// --- ControllerGetCapabilities ---

func TestControllerGetCapabilities(t *testing.T) {
	cs, _ := setupController()
	resp, err := cs.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities failed: %v", err)
	}

	expected := map[csi.ControllerServiceCapability_RPC_Type]bool{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME:     false,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT:   false,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME:            false,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME: false,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY:             false,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES:             false,
	}

	for _, cap := range resp.GetCapabilities() {
		rpc := cap.GetRpc()
		if rpc == nil {
			t.Fatal("expected RPC capability, got nil")
		}
		if _, ok := expected[rpc.GetType()]; !ok {
			t.Errorf("unexpected capability: %v", rpc.GetType())
		}
		expected[rpc.GetType()] = true
	}

	for capType, found := range expected {
		if !found {
			t.Errorf("missing expected capability: %v", capType)
		}
	}
}

func TestControllerGetCapabilities_IncludesGetCapacity(t *testing.T) {
	cs, _ := setupController()

	resp, err := cs.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities failed: %v", err)
	}

	caps := resp.GetCapabilities()
	if len(caps) == 0 {
		t.Fatal("expected at least one capability")
	}

	foundGetCapacity := false
	for _, cap := range caps {
		rpc := cap.GetRpc()
		if rpc == nil {
			continue
		}
		if rpc.GetType() == csi.ControllerServiceCapability_RPC_GET_CAPACITY {
			foundGetCapacity = true
			break
		}
	}
	if !foundGetCapacity {
		t.Error("GET_CAPACITY capability not found")
	}
}

func TestGetCapacity_WithoutNodeMeta(t *testing.T) {
	cs, _ := setupController()

	resp, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if err != nil {
		t.Fatalf("GetCapacity failed: %v", err)
	}

	if resp.AvailableCapacity != 0 {
		t.Errorf("expected 0 capacity without nodeMeta, got %d", resp.AvailableCapacity)
	}
}

func TestGetCapacity_WithNodeMeta(t *testing.T) {
	store := newMockMetadataStore()
	nodeStore := &mockNodeMetaStore{
		nodes: []*metadata.NodeMeta{
			{NodeID: "node-1", AvailableCapacity: 100 * 1024 * 1024 * 1024}, // 100GB
			{NodeID: "node-2", AvailableCapacity: 200 * 1024 * 1024 * 1024}, // 200GB
			{NodeID: "node-3", AvailableCapacity: 50 * 1024 * 1024 * 1024},  // 50GB
		},
	}

	cs := NewControllerServerWithNodeMeta(store, nodeStore, &mockPlacer{}, nil, nil)

	resp, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if err != nil {
		t.Fatalf("GetCapacity failed: %v", err)
	}

	// Total: 350GB, divided by 3 = ~116GB
	expectedCapacity := (100 + 200 + 50) * 1024 * 1024 * 1024 / 3
	if resp.AvailableCapacity < int64(expectedCapacity-1000) || resp.AvailableCapacity > int64(expectedCapacity+1000) {
		t.Errorf("capacity around %d, got %d", expectedCapacity, resp.AvailableCapacity)
	}
}

// --- mock node metadata store ---

type mockNodeMetaStore struct {
	mu    sync.Mutex
	nodes []*metadata.NodeMeta
}

func (m *mockNodeMetaStore) ListLiveNodeMetas(_ context.Context, _ time.Duration) ([]*metadata.NodeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodes, nil
}
