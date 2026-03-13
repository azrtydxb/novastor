// Package csi implements the Container Storage Interface (CSI) services
// for the NovaStor unified storage system.
package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/azrtydxb/novastor/internal/logging"
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

// DeviceFormatter abstracts block device formatting and mounting so that
// tests can substitute a no-op implementation without needing blkid/mkfs/mount.
type DeviceFormatter interface {
	// IsFormatted returns true if devicePath already contains a filesystem.
	IsFormatted(ctx context.Context, devicePath string) (bool, error)
	// Format formats devicePath with the given filesystem type.
	Format(ctx context.Context, devicePath, fsType string) error
	// Mount mounts devicePath to targetPath using the given filesystem type.
	Mount(ctx context.Context, devicePath, targetPath, fsType string) error
	// Unmount unmounts the filesystem at targetPath.
	Unmount(ctx context.Context, targetPath string) error
}

// RealDeviceFormatter is the production DeviceFormatter that calls blkid,
// mkfs.ext4, mount, and umount via exec.
type RealDeviceFormatter struct{}

// IsFormatted checks if the device has a filesystem.
func (RealDeviceFormatter) IsFormatted(ctx context.Context, devicePath string) (bool, error) {
	return isFormatted(ctx, devicePath)
}

// Format creates a filesystem on the device.
func (RealDeviceFormatter) Format(ctx context.Context, devicePath, fsType string) error {
	return formatDevice(ctx, devicePath, fsType)
}

// Mount mounts the device to the target path.
func (RealDeviceFormatter) Mount(ctx context.Context, devicePath, targetPath, fsType string) error {
	return mountDevice(ctx, devicePath, targetPath, fsType)
}

// Unmount unmounts the filesystem at the target path.
func (RealDeviceFormatter) Unmount(ctx context.Context, targetPath string) error {
	return unmountPath(ctx, targetPath)
}

// NodeService implements the CSI Node service.
type NodeService struct {
	csi.UnimplementedNodeServer

	nodeID      string
	zone        string
	region      string
	chunkClient ChunkClient
	mounter     Mounter
	initiator   NVMeInitiator
	formatter   DeviceFormatter
}

// NewNodeService creates a new NodeService with the given node ID, chunk client,
// and mounter implementation.
func NewNodeService(nodeID string, chunkClient ChunkClient, mounter Mounter) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		chunkClient: chunkClient,
		mounter:     mounter,
		formatter:   RealDeviceFormatter{},
	}
}

// NewNodeServiceWithTopology creates a new NodeService with topology information.
func NewNodeServiceWithTopology(nodeID, zone, region string, chunkClient ChunkClient, mounter Mounter) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		zone:        zone,
		region:      region,
		chunkClient: chunkClient,
		mounter:     mounter,
		formatter:   RealDeviceFormatter{},
	}
}

// NewNodeServiceWithInitiator creates a new NodeService with an NVMe-oF initiator.
func NewNodeServiceWithInitiator(nodeID string, chunkClient ChunkClient, mounter Mounter, initiator NVMeInitiator) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		chunkClient: chunkClient,
		mounter:     mounter,
		initiator:   initiator,
		formatter:   RealDeviceFormatter{},
	}
}

// NewNodeServiceWithInitiatorAndTopology creates a new NodeService with NVMe-oF initiator and topology.
func NewNodeServiceWithInitiatorAndTopology(nodeID, zone, region string, chunkClient ChunkClient, mounter Mounter, initiator NVMeInitiator) *NodeService {
	return &NodeService{
		nodeID:      nodeID,
		zone:        zone,
		region:      region,
		chunkClient: chunkClient,
		mounter:     mounter,
		initiator:   initiator,
		formatter:   RealDeviceFormatter{},
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

	targetAddressesJSON := volCtx["targetAddresses"]
	var devicePath string

	if targetAddressesJSON != "" && ns.initiator != nil {
		// Multi-path attachment: connect to all replica targets.
		var targets []TargetInfo
		if err := json.Unmarshal([]byte(targetAddressesJSON), &targets); err != nil {
			return nil, status.Errorf(codes.Internal, "parse target addresses: %v", err)
		}

		if spdkInit, ok := ns.initiator.(*SPDKInitiator); ok {
			var err error
			devicePath, err = spdkInit.ConnectMultipath(ctx, targets)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "connect NVMe-oF multipath: %v", err)
			}
		} else {
			// Fallback: connect to owner target only
			owner := targets[0]
			for _, t := range targets {
				if t.IsOwner {
					owner = t
					break
				}
			}
			var err error
			devicePath, err = ns.initiator.Connect(ctx, owner.Addr, owner.Port, owner.NQN)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "connect NVMe-oF target: %v", err)
			}
		}

		logging.L.Info("NodeStageVolume: connected NVMe-oF multipath",
			zap.String("devicePath", devicePath),
			zap.String("volumeID", req.GetVolumeId()),
			zap.Int("pathCount", len(targets)))

		// Write multipath marker with full targets JSON for disconnect on unstage.
		mpMarkerPath := filepath.Join(stagingPath, "nvme-multipath")
		if err := os.WriteFile(mpMarkerPath, []byte(targetAddressesJSON), 0600); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to write multipath marker: %v", err)
		}
	} else if ns.initiator != nil && targetAddr != "" && subsystemNQN != "" {
		// Single-target attachment (backward compat).
		if targetPort == "" {
			targetPort = "4430"
		}
		var err error
		devicePath, err = ns.initiator.Connect(ctx, targetAddr, targetPort, subsystemNQN)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to connect NVMe-oF target: %v", err)
		}
		logging.L.Info("NodeStageVolume: connected NVMe-oF device",
			zap.String("devicePath", devicePath),
			zap.String("volumeID", req.GetVolumeId()))
	}

	if devicePath != "" {
		// Format the device with ext4 if it has no filesystem yet.
		formatted, fmtErr := ns.formatter.IsFormatted(ctx, devicePath)
		if fmtErr != nil {
			return nil, status.Errorf(codes.Internal, "checking filesystem on %s: %v", devicePath, fmtErr)
		}
		if !formatted {
			logging.L.Info("NodeStageVolume: formatting device",
				zap.String("devicePath", devicePath),
				zap.String("volumeID", req.GetVolumeId()))
			if fmtErr := ns.formatter.Format(ctx, devicePath, "ext4"); fmtErr != nil {
				return nil, status.Errorf(codes.Internal, "formatting %s as ext4: %v", devicePath, fmtErr)
			}
		}

		// Mount the formatted device to the staging path.
		if mountErr := ns.formatter.Mount(ctx, devicePath, stagingPath, "ext4"); mountErr != nil {
			return nil, status.Errorf(codes.Internal, "mounting %s to %s: %v", devicePath, stagingPath, mountErr)
		}
		logging.L.Info("NodeStageVolume: mounted device",
			zap.String("devicePath", devicePath),
			zap.String("stagingPath", stagingPath),
			zap.String("volumeID", req.GetVolumeId()))

		// Write device path marker so NodeUnstageVolume knows what to disconnect.
		// For single path, store the NQN. For multipath, the nvme-multipath marker is already written.
		if targetAddressesJSON == "" {
			markerPath := filepath.Join(stagingPath, "nvme-device")
			if err := os.WriteFile(markerPath, []byte(subsystemNQN), 0600); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to write device marker: %v", err)
			}
		}
	} else {
		logging.L.Info("NodeStageVolume: staging volume (no NVMe-oF initiator)",
			zap.String("volumeID", req.GetVolumeId()),
			zap.String("stagingPath", stagingPath))
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

	// Check for multipath marker first.
	mpMarkerPath := filepath.Join(stagingPath, "nvme-multipath")
	markerPath := filepath.Join(stagingPath, "nvme-device")
	if mpData, err := os.ReadFile(mpMarkerPath); err == nil && ns.initiator != nil {
		// Unmount first.
		if umountErr := ns.formatter.Unmount(ctx, stagingPath); umountErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging path %s: %v", stagingPath, umountErr)
		}

		var targets []TargetInfo
		if err := json.Unmarshal(mpData, &targets); err == nil {
			if spdkInit, ok := ns.initiator.(*SPDKInitiator); ok {
				if disconnErr := spdkInit.DisconnectMultipath(ctx, targets); disconnErr != nil {
					logging.L.Warn("failed to disconnect multipath targets",
						zap.String("volumeID", req.GetVolumeId()),
						zap.Error(disconnErr))
				}
			} else {
				// Fallback: disconnect the owner target only
				for _, t := range targets {
					if t.IsOwner {
						_ = ns.initiator.Disconnect(ctx, t.NQN)
						break
					}
				}
			}
		}
		logging.L.Info("NodeUnstageVolume: disconnected NVMe-oF multipath",
			zap.String("volumeID", req.GetVolumeId()))
	} else if nqnBytes, readErr := os.ReadFile(markerPath); readErr == nil && ns.initiator != nil {
		nqn := string(nqnBytes)

		// Unmount the staging path before disconnecting the NVMe device.
		if umountErr := ns.formatter.Unmount(ctx, stagingPath); umountErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging path %s: %v", stagingPath, umountErr)
		}
		logging.L.Info("NodeUnstageVolume: unmounted staging path", zap.String("stagingPath", stagingPath), zap.String("volumeID", req.GetVolumeId()))

		if disconnErr := ns.initiator.Disconnect(ctx, nqn); disconnErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to disconnect NVMe-oF target %s: %v", nqn, disconnErr)
		}
		logging.L.Info("NodeUnstageVolume: disconnected NVMe-oF target", zap.String("nqn", nqn), zap.String("volumeID", req.GetVolumeId()))
	}

	// Clean up staging directory.
	if err := os.RemoveAll(stagingPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove staging path %s: %v", stagingPath, err)
	}

	logging.L.Info("NodeUnstageVolume: unstaged volume", zap.String("volumeID", req.GetVolumeId()), zap.String("stagingPath", stagingPath))
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staging path to the target path.
// If volume context indicates NFS-backed (RWX), it mounts via NFS instead.
func (ns *NodeService) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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
		logging.L.Info("NodePublishVolume: NFS mounting", zap.String("source", nfsSource), zap.String("targetPath", targetPath), zap.String("volumeID", req.GetVolumeId()))

		if err := ns.mounter.Mount(nfsSource, targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to NFS mount %s to %s: %v", nfsSource, targetPath, err)
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Ensure the target directory exists (required by CSI spec for NodePublishVolume).
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target path %s: %v", targetPath, err)
	}

	logging.L.Info("NodePublishVolume: bind-mounting", zap.String("stagingPath", stagingPath), zap.String("targetPath", targetPath), zap.String("volumeID", req.GetVolumeId()))

	if err := ns.mounter.Mount(stagingPath, targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind-mount %s to %s: %v", stagingPath, targetPath, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the target path.
func (ns *NodeService) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	targetPath := req.GetTargetPath()

	logging.L.Info("NodeUnpublishVolume: unmounting", zap.String("targetPath", targetPath), zap.String("volumeID", req.GetVolumeId()))

	if err := ns.mounter.Unmount(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount %s: %v", targetPath, err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities returns the capabilities supported by this node service.
func (ns *NodeService) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
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
func (ns *NodeService) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	segments := map[string]string{
		"novastor.io/node": ns.nodeID,
	}
	// Add standard Kubernetes topology labels if available.
	if ns.zone != "" {
		segments["topology.kubernetes.io/zone"] = ns.zone
	}
	if ns.region != "" {
		segments["topology.kubernetes.io/region"] = ns.region
	}

	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: segments,
		},
	}, nil
}

// NodeGetVolumeStats returns usage statistics for the volume at the given path.
func (ns *NodeService) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
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
func (ns *NodeService) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
	}, nil
}
