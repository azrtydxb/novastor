// Package csi implements the Container Storage Interface (CSI) services
// for the NovaStor unified storage system.
package csi

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
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
	initiator   NVMeInitiator
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

// NewNodeServiceWithInitiator creates a new NodeService with an NVMe-oF initiator.
func NewNodeServiceWithInitiator(nodeID string, chunkClient ChunkClient, mounter Mounter, initiator NVMeInitiator) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		chunkClient: chunkClient,
		mounter:     mounter,
		initiator:   initiator,
	}
}

// NodeStageVolume connects to the NVMe-oF target and presents the block device.
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

	stagingPath := req.GetStagingTargetPath()

	// Create staging directory.
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging dir: %v", err)
	}

	// Extract NVMe-oF connection parameters from volume context.
	volCtx := req.GetVolumeContext()
	targetAddr := volCtx["targetAddress"]
	targetPort := volCtx["targetPort"]
	subsystemNQN := volCtx["subsystemNQN"]

	if ns.initiator != nil && targetAddr != "" && subsystemNQN != "" {
		if targetPort == "" {
			targetPort = "4420" // Default NVMe-oF port
		}
		devicePath, err := ns.initiator.Connect(ctx, targetAddr, targetPort, subsystemNQN)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to connect NVMe-oF target: %v", err)
		}
		log.Printf("NodeStageVolume: connected NVMe-oF device at %s for volume %s", devicePath, req.GetVolumeId())

		// Write device path marker so NodeUnstageVolume knows what to disconnect.
		markerPath := filepath.Join(stagingPath, "nvme-device")
		if err := os.WriteFile(markerPath, []byte(subsystemNQN), 0600); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to write device marker: %v", err)
		}
	} else {
		log.Printf("NodeStageVolume: staging volume %s at %s (no NVMe-oF initiator)", req.GetVolumeId(), stagingPath)
		// Write a simple marker to indicate the volume is staged.
		markerPath := filepath.Join(stagingPath, "staged")
		if err := os.WriteFile(markerPath, []byte(req.GetVolumeId()), 0600); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to write staging marker: %v", err)
		}
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume disconnects the NVMe-oF target and cleans up staging.
func (ns *NodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	stagingPath := req.GetStagingTargetPath()

	// Check for NVMe-oF marker and disconnect if present.
	markerPath := filepath.Join(stagingPath, "nvme-device")
	if nqnBytes, err := os.ReadFile(markerPath); err == nil && ns.initiator != nil {
		nqn := string(nqnBytes)
		if disconnErr := ns.initiator.Disconnect(ctx, nqn); disconnErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to disconnect NVMe-oF target %s: %v", nqn, disconnErr)
		}
		log.Printf("NodeUnstageVolume: disconnected NVMe-oF target %s for volume %s", nqn, req.GetVolumeId())
	}

	// Clean up staging directory.
	if err := os.RemoveAll(stagingPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove staging path %s: %v", stagingPath, err)
	}

	log.Printf("NodeUnstageVolume: unstaged volume %s from %s", req.GetVolumeId(), stagingPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staging path to the target path.
// If volume context indicates NFS-backed (RWX), it mounts via NFS instead.
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
	volCtx := req.GetVolumeContext()

	// Check if this is an NFS-backed (RWX) volume.
	if nfsServer, ok := volCtx["nfsServer"]; ok && nfsServer != "" {
		nfsShare := volCtx["nfsShare"]
		nfsSource := fmt.Sprintf("%s:%s", nfsServer, nfsShare)
		log.Printf("NodePublishVolume: NFS mounting %s to %s for volume %s", nfsSource, targetPath, req.GetVolumeId())

		if err := ns.mounter.Mount(nfsSource, targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to NFS mount %s to %s: %v", nfsSource, targetPath, err)
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

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
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
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

// NodeGetVolumeStats returns usage statistics for the volume at the given path.
func (ns *NodeService) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	volumePath := req.GetVolumePath()

	// Stat the volume path to get filesystem usage.
	var statfs unix.Statfs_t
	if err := unix.Statfs(volumePath, &statfs); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to statfs %s: %v", volumePath, err)
	}

	totalBytes := int64(statfs.Blocks) * int64(statfs.Bsize)
	availableBytes := int64(statfs.Bavail) * int64(statfs.Bsize)
	usedBytes := totalBytes - availableBytes

	totalInodes := int64(statfs.Files)
	freeInodes := int64(statfs.Ffree)
	usedInodes := totalInodes - freeInodes

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: availableBytes,
				Total:     totalBytes,
				Used:      usedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: freeInodes,
				Total:     totalInodes,
				Used:      usedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// NodeExpandVolume returns the requested capacity. Actual expansion is handled
// at the controller level.
func (ns *NodeService) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
	}, nil
}
