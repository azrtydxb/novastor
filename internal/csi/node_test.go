package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockChunkClient is a test double for ChunkClient.
type mockChunkClient struct{}

func (m *mockChunkClient) GetChunk(ctx context.Context, nodeID string, chunkID string) ([]byte, error) {
	return nil, nil
}

func (m *mockChunkClient) PutChunk(ctx context.Context, nodeID string, chunkID string, data []byte) error {
	return nil
}

// mockMounter is a test double for Mounter.
type mockMounter struct {
	mountErr   error
	unmountErr error

	mountSource string
	mountTarget string
	unmountPath string
}

func (m *mockMounter) Mount(source, target string) error {
	m.mountSource = source
	m.mountTarget = target
	return m.mountErr
}

func (m *mockMounter) Unmount(target string) error {
	m.unmountPath = target
	return m.unmountErr
}

func newTestNodeService(nodeID string, mounter Mounter) *NodeService {
	return NewNodeService(nodeID, &mockChunkClient{}, mounter)
}

func TestNodeGetInfo(t *testing.T) {
	const testNodeID = "node-test-001"
	ns := newTestNodeService(testNodeID, &mockMounter{})

	resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo returned unexpected error: %v", err)
	}

	if resp.GetNodeId() != testNodeID {
		t.Errorf("expected node ID %q, got %q", testNodeID, resp.GetNodeId())
	}

	topo := resp.GetAccessibleTopology()
	if topo == nil {
		t.Fatal("expected accessible topology, got nil")
	}

	val, ok := topo.GetSegments()["novastor.io/node"]
	if !ok {
		t.Fatal("expected topology key 'novastor.io/node' not found")
	}
	if val != testNodeID {
		t.Errorf("expected topology value %q, got %q", testNodeID, val)
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	resp, err := ns.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities returned unexpected error: %v", err)
	}

	caps := resp.GetCapabilities()
	if len(caps) != 3 {
		t.Fatalf("expected 3 capabilities, got %d", len(caps))
	}

	expectedTypes := map[csi.NodeServiceCapability_RPC_Type]bool{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME: false,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS:     false,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME:        false,
	}

	for _, cap := range caps {
		rpc := cap.GetRpc()
		if rpc == nil {
			t.Fatal("expected RPC capability, got nil")
		}
		expectedTypes[rpc.GetType()] = true
	}

	for capType, found := range expectedTypes {
		if !found {
			t.Errorf("missing expected capability: %v", capType)
		}
	}
}

func TestNodeStageVolume_Success(t *testing.T) {
	tmpDir := t.TempDir()
	stagingPath := filepath.Join(tmpDir, "staging")

	ns := newTestNodeService("node-1", &mockMounter{})

	resp, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-001",
		StagingTargetPath: stagingPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Verify staging directory was created.
	if _, err := os.Stat(stagingPath); os.IsNotExist(err) {
		t.Error("expected staging directory to be created")
	}

	// Verify marker file was written.
	markerPath := filepath.Join(stagingPath, "staged")
	data, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("expected staging marker file: %v", err)
	}
	if string(data) != "vol-001" {
		t.Errorf("expected marker content 'vol-001', got %q", string(data))
	}
}

func TestNodeStageVolume_WithInitiator(t *testing.T) {
	tmpDir := t.TempDir()
	stagingPath := filepath.Join(tmpDir, "staging")
	initiatorBase := filepath.Join(tmpDir, "nvme-devices")

	initiator := &StubInitiator{BasePath: initiatorBase}
	ns := NewNodeServiceWithInitiator("node-1", &mockChunkClient{}, &mockMounter{}, initiator)

	resp, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-002",
		StagingTargetPath: stagingPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"targetAddress": "10.0.0.1",
			"targetPort":    "4420",
			"subsystemNQN":  "nqn.2024-01.io.novastor:vol-002",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Verify NVMe device marker was created.
	markerPath := filepath.Join(stagingPath, "nvme-device")
	data, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("expected NVMe device marker: %v", err)
	}
	if string(data) != "nqn.2024-01.io.novastor:vol-002" {
		t.Errorf("expected marker NQN, got %q", string(data))
	}

	// Verify the stub initiator created the device directory.
	deviceDir := filepath.Join(initiatorBase, "nvme-nqn.2024-01.io.novastor:vol-002")
	if _, err := os.Stat(deviceDir); os.IsNotExist(err) {
		t.Error("expected stub NVMe device directory to be created")
	}
}

func TestNodeUnstageVolume_Success(t *testing.T) {
	tmpDir := t.TempDir()
	stagingPath := filepath.Join(tmpDir, "staging")

	// Create the staging directory and marker.
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stagingPath, "staged"), []byte("vol-001"), 0600); err != nil {
		t.Fatal(err)
	}

	ns := newTestNodeService("node-1", &mockMounter{})

	resp, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-001",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Verify staging directory was cleaned up.
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Error("expected staging directory to be removed")
	}
}

func TestNodeUnstageVolume_WithInitiator(t *testing.T) {
	tmpDir := t.TempDir()
	stagingPath := filepath.Join(tmpDir, "staging")
	initiatorBase := filepath.Join(tmpDir, "nvme-devices")

	initiator := &StubInitiator{BasePath: initiatorBase}

	// Simulate a previously staged NVMe device.
	nqn := "nqn.2024-01.io.novastor:vol-003"
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stagingPath, "nvme-device"), []byte(nqn), 0600); err != nil {
		t.Fatal(err)
	}
	// Create the stub device directory.
	deviceDir := filepath.Join(initiatorBase, "nvme-"+nqn)
	if err := os.MkdirAll(deviceDir, 0750); err != nil {
		t.Fatal(err)
	}

	ns := NewNodeServiceWithInitiator("node-1", &mockChunkClient{}, &mockMounter{}, initiator)

	resp, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-003",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Verify device directory was cleaned up by the initiator.
	if _, err := os.Stat(deviceDir); !os.IsNotExist(err) {
		t.Error("expected NVMe device directory to be removed")
	}
}

func TestNodeStageVolume_MissingVolumeID(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		StagingTargetPath: "/staging/path",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestNodeUnstageVolume_MissingVolumeID(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		StagingTargetPath: "/staging/path",
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestNodePublishVolume_Success(t *testing.T) {
	mounter := &mockMounter{}
	ns := newTestNodeService("node-1", mounter)

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-001",
		StagingTargetPath: "/staging/path",
		TargetPath:        "/target/path",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	})
	if err != nil {
		t.Fatalf("NodePublishVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if mounter.mountSource != "/staging/path" {
		t.Errorf("expected mount source %q, got %q", "/staging/path", mounter.mountSource)
	}
	if mounter.mountTarget != "/target/path" {
		t.Errorf("expected mount target %q, got %q", "/target/path", mounter.mountTarget)
	}
}

func TestNodePublishVolume_NFS(t *testing.T) {
	mounter := &mockMounter{}
	ns := newTestNodeService("node-1", mounter)

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-rwx",
		StagingTargetPath: "/staging/path",
		TargetPath:        "/target/path",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"nfsServer": "10.0.0.5",
			"nfsShare":  "/exports/vol-rwx",
		},
	})
	if err != nil {
		t.Fatalf("NodePublishVolume NFS returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Should have mounted via NFS source format.
	expectedSource := "10.0.0.5:/exports/vol-rwx"
	if mounter.mountSource != expectedSource {
		t.Errorf("expected mount source %q, got %q", expectedSource, mounter.mountSource)
	}
	if mounter.mountTarget != "/target/path" {
		t.Errorf("expected mount target %q, got %q", "/target/path", mounter.mountTarget)
	}
}

func TestNodePublishVolume_MountError(t *testing.T) {
	mounter := &mockMounter{mountErr: errors.New("mount failed")}
	ns := newTestNodeService("node-1", mounter)

	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-001",
		StagingTargetPath: "/staging/path",
		TargetPath:        "/target/path",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for mount failure")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Internal {
		t.Errorf("expected Internal error, got %v", err)
	}
}

func TestNodePublishVolume_MissingTargetPath(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-001",
		StagingTargetPath: "/staging/path",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for missing target path")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestNodeUnpublishVolume_Success(t *testing.T) {
	mounter := &mockMounter{}
	ns := newTestNodeService("node-1", mounter)

	resp, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-001",
		TargetPath: "/target/path",
	})
	if err != nil {
		t.Fatalf("NodeUnpublishVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if mounter.unmountPath != "/target/path" {
		t.Errorf("expected unmount path %q, got %q", "/target/path", mounter.unmountPath)
	}
}

func TestNodeUnpublishVolume_UnmountError(t *testing.T) {
	mounter := &mockMounter{unmountErr: errors.New("unmount failed")}
	ns := newTestNodeService("node-1", mounter)

	_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-001",
		TargetPath: "/target/path",
	})
	if err == nil {
		t.Fatal("expected error for unmount failure")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Internal {
		t.Errorf("expected Internal error, got %v", err)
	}
}

func TestNodeUnpublishVolume_MissingVolumeID(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		TargetPath: "/target/path",
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestNodeGetVolumeStats_Success(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	// Use t.TempDir() as the volume path since it exists on the filesystem.
	volumePath := t.TempDir()

	resp, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "vol-001",
		VolumePath: volumePath,
	})
	if err != nil {
		t.Fatalf("NodeGetVolumeStats returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	usage := resp.GetUsage()
	if len(usage) != 2 {
		t.Fatalf("expected 2 usage entries (bytes + inodes), got %d", len(usage))
	}

	// Verify bytes usage entry.
	bytesUsage := usage[0]
	if bytesUsage.GetUnit() != csi.VolumeUsage_BYTES {
		t.Errorf("expected BYTES unit, got %v", bytesUsage.GetUnit())
	}
	if bytesUsage.GetTotal() <= 0 {
		t.Error("expected positive total bytes")
	}

	// Verify inodes usage entry.
	inodesUsage := usage[1]
	if inodesUsage.GetUnit() != csi.VolumeUsage_INODES {
		t.Errorf("expected INODES unit, got %v", inodesUsage.GetUnit())
	}
}

func TestNodeGetVolumeStats_MissingVolumeID(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumePath: "/some/path",
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestNodeExpandVolume_Success(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	resp, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-001",
		VolumePath: "/some/path",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 10 * 1024 * 1024 * 1024, // 10 GiB
		},
	})
	if err != nil {
		t.Fatalf("NodeExpandVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if resp.GetCapacityBytes() != 10*1024*1024*1024 {
		t.Errorf("expected capacity 10GiB, got %d", resp.GetCapacityBytes())
	}
}

func TestNodeExpandVolume_MissingVolumeID(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumePath: "/some/path",
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}
