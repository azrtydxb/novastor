// Package csi implements the Container Storage Interface (CSI) services
// for the NovaStor unified storage system.
package csi

import (
	"context"
	"log"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChunkClient is the interface for fetching and writing chunks to remote nodes.
type ChunkClient interface {
	GetChunk(ctx context.Context, nodeID string, chunkID string) ([]byte, error)
	PutChunk(ctx context.Context, nodeID string, chunkID string, data []byte) error
}

// Mounter abstracts filesystem mount operations for testability.
type Mounter interface {
	// Mount bind-mounts source to target.
	Mount(source, target string) error
	// Unmount unmounts the given target path.
	Unmount(target string) error
}

// NodeService implements the CSI Node service.
type NodeService struct {
	csi.UnimplementedNodeServer

	nodeID      string
	chunkClient ChunkClient
	mounter     Mounter
}

// NewNodeService creates a new NodeService with the given node ID, chunk client,
// and mounter implementation.
func NewNodeService(nodeID string, chunkClient ChunkClient, mounter Mounter) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		chunkClient: chunkClient,
		mounter:     mounter,
	}
}

// NodeStageVolume is a stub for Phase 2. Real NVMe-oF attach logic will be
// added in a later task.
func (ns *NodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	log.Printf("NodeStageVolume called for volume %s at staging path %s (stub)", req.GetVolumeId(), req.GetStagingTargetPath())
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume is a stub for Phase 2. Returns success.
func (ns *NodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	log.Printf("NodeUnstageVolume called for volume %s at staging path %s (stub)", req.GetVolumeId(), req.GetStagingTargetPath())
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staging path to the target path.
func (ns *NodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	stagingPath := req.GetStagingTargetPath()
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	targetPath := req.GetTargetPath()

	log.Printf("NodePublishVolume: bind-mounting %s to %s for volume %s", stagingPath, targetPath, req.GetVolumeId())

	if err := ns.mounter.Mount(stagingPath, targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind-mount %s to %s: %v", stagingPath, targetPath, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the target path.
func (ns *NodeService) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	targetPath := req.GetTargetPath()

	log.Printf("NodeUnpublishVolume: unmounting %s for volume %s", targetPath, req.GetVolumeId())

	if err := ns.mounter.Unmount(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount %s: %v", targetPath, err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities returns the capabilities supported by this node service.
func (ns *NodeService) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}, nil
}

// NodeGetInfo returns information about the node, including its ID and
// accessible topology.
func (ns *NodeService) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				"novastor.io/node": ns.nodeID,
			},
		},
	}, nil
}

// NodeGetVolumeStats is not implemented in this phase.
func (ns *NodeService) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats is not implemented")
}

// NodeExpandVolume is not implemented in this phase.
func (ns *NodeService) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}
