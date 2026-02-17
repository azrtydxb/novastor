package csi

import (
	"context"
	"errors"
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
	if len(caps) != 1 {
		t.Fatalf("expected 1 capability, got %d", len(caps))
	}

	rpc := caps[0].GetRpc()
	if rpc == nil {
		t.Fatal("expected RPC capability, got nil")
	}

	if rpc.GetType() != csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME {
		t.Errorf("expected STAGE_UNSTAGE_VOLUME capability, got %v", rpc.GetType())
	}
}

func TestNodeStageVolume_Success(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	resp, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
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
	if err != nil {
		t.Fatalf("NodeStageVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
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

func TestNodeUnstageVolume_Success(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	resp, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-001",
		StagingTargetPath: "/staging/path",
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
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

func TestNodeGetVolumeStats_Unimplemented(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "vol-001",
		VolumePath: "/some/path",
	})
	if err == nil {
		t.Fatal("expected error for unimplemented method")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
	}
}

func TestNodeExpandVolume_Unimplemented(t *testing.T) {
	ns := newTestNodeService("node-1", &mockMounter{})

	_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-001",
		VolumePath: "/some/path",
	})
	if err == nil {
		t.Fatal("expected error for unimplemented method")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
	}
}
