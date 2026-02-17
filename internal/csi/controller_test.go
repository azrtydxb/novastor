package csi

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/piwi3910/novastor/internal/metadata"
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

// --- helpers ---

func setupController() (*ControllerServer, *mockMetadataStore) {
	store := newMockMetadataStore()
	placer := &mockPlacer{nodes: []string{"node-1", "node-2", "node-3"}}
	return NewControllerServer(store, placer), store
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
	if len(vol.GetAccessibleTopology()) == 0 {
		t.Fatal("expected at least one accessible topology segment")
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

func TestCreateVolumeNoNodes(t *testing.T) {
	store := newMockMetadataStore()
	placer := &mockPlacer{nodes: nil}
	cs := NewControllerServer(store, placer)

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
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
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

// --- Unimplemented RPCs ---

func TestListVolumesUnimplemented(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
	}
}

func TestControllerPublishVolumeUnimplemented(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
	}
}

func TestControllerUnpublishVolumeUnimplemented(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
	}
}

func TestGetCapacityUnimplemented(t *testing.T) {
	cs, _ := setupController()
	_, err := cs.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
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
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME:   false,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT: false,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME:         false,
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
