package agent

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/piwi3910/novastor/api/proto/nvme"
	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/nvmeof"
)

const (
	// nvmeTargetPort is the single shared TCP port for all NVMe-oF targets on this node.
	nvmeTargetPort = 4420

	// nqnPrefix must match the subsystemPrefix in internal/nvmeof/target.go so that
	// the NQN advertised to initiators exactly equals the nvmet configfs directory name.
	nqnPrefix = "novastor-"
)

// NVMeTargetServer implements the NVMeTargetService gRPC server. It assembles
// chunk-backed block devices and exposes them as NVMe-oF/TCP targets via the
// nvmet configfs interface.
type NVMeTargetServer struct {
	pb.UnimplementedNVMeTargetServiceServer

	hostIP        string
	targetManager *nvmeof.TargetManager
	chunkStore    chunk.Store
	metaClient    *metadata.GRPCClient
	// activeDevices maps volumeID to ChunkBlockDevice for tracking.
	activeDevices map[string]*ChunkBlockDevice
}

// NewNVMeTargetServer creates an NVMeTargetServer that listens on hostIP.
// It cleans up any stale nvmet configfs state left over from a previous agent
// pod instance, since the kernel configfs persists across pod restarts but
// the TCP listeners may be bound to dead network namespaces.
func NewNVMeTargetServer(hostIP string, chunkStore chunk.Store, metaClient *metadata.GRPCClient) *NVMeTargetServer {
	tm := nvmeof.NewTargetManager()
	if err := tm.CleanupAll(); err != nil {
		logging.L.Warn("nvme target: failed to clean stale configfs state", zap.Error(err))
	} else {
		logging.L.Info("nvme target: cleaned stale configfs state on startup")
	}
	return &NVMeTargetServer{
		hostIP:        hostIP,
		targetManager: tm,
		chunkStore:    chunkStore,
		metaClient:    metaClient,
		activeDevices: make(map[string]*ChunkBlockDevice),
	}
}

// Register adds the NVMeTargetService to the given gRPC server.
func (s *NVMeTargetServer) Register(srv *grpc.Server) {
	pb.RegisterNVMeTargetServiceServer(srv, s)
}

// CreateTarget assembles chunks into a block device, attaches it as a loop device,
// and exposes it as an NVMe-oF/TCP target via the nvmet configfs interface.
// This function is idempotent: if a target for the same volumeID already exists,
// it returns the existing target's connection parameters instead of failing.
func (s *NVMeTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	// Check if target already exists (idempotency).
	if dev, exists := s.activeDevices[volumeID]; exists {
		if loopDev := dev.GetLoopDevice(); loopDev != "" {
			// Device already assembled and attached.
			nqn := nqnPrefix + volumeID
			logging.L.Info("nvme target: target already exists, returning existing connection params",
				zap.String("volumeID", volumeID),
				zap.String("nqn", nqn),
			)
			return &pb.CreateTargetResponse{
				SubsystemNqn:  nqn,
				TargetAddress: s.hostIP,
				TargetPort:    fmt.Sprintf("%d", nvmeTargetPort),
			}, nil
		}
	}

	// Check for existing targets in nvmet.
	existingTargets, listErr := s.targetManager.ListTargets()
	if listErr == nil {
		for _, existingVolumeID := range existingTargets {
			if existingVolumeID == volumeID {
				// NVMe target exists in configfs but not in our active map.
				// Clean it up and recreate.
				logging.L.Info("nvme target: stale nvmet target found, cleaning up",
					zap.String("volumeID", volumeID),
				)
				_ = s.targetManager.DeleteTarget(volumeID)
				break
			}
		}
	}

	// Create chunk-backed block device.
	blockDev := NewChunkBlockDevice(volumeID, s.chunkStore, s.metaClient)

	// Ensure all chunks exist, creating empty blocks for new volumes.
	if err := blockDev.EnsureChunks(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "ensuring chunks for volume %s: %v", volumeID, err)
	}

	// Assemble chunks into block device and attach loop device.
	loopDev, err := blockDev.Assemble(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "assembling chunk block device for volume %s: %v", volumeID, err)
	}

	// Register the nvmet target backed by the loop device.
	// Use "0.0.0.0" as the kernel listen address so nvmet binds on all interfaces.
	if err := s.targetManager.CreateTargetWithDevice(volumeID, nvmeTargetPort, loopDev, "0.0.0.0"); err != nil {
		// Clean up block device on failure.
		_ = blockDev.Disassemble(ctx)
		return nil, status.Errorf(codes.Internal, "creating nvmet target for volume %s: %v", volumeID, err)
	}

	// Track the active device.
	s.activeDevices[volumeID] = blockDev

	nqn := nqnPrefix + volumeID
	logging.L.Info("nvme target: target created",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("address", s.hostIP),
		zap.Int("port", nvmeTargetPort),
		zap.String("loopDevice", loopDev),
	)

	return &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", nvmeTargetPort),
	}, nil
}

// DeleteTarget removes the nvmet target, disassembles the chunk-backed block device,
// and detaches the loop device.
func (s *NVMeTargetServer) DeleteTarget(ctx context.Context, req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	// Remove the nvmet target (best-effort; target may not exist on retry).
	if err := s.targetManager.DeleteTarget(volumeID); err != nil {
		logging.L.Warn("nvme target: failed to delete nvmet target", zap.String("volumeID", volumeID), zap.Error(err))
	}

	// Disassemble the chunk-backed block device.
	if blockDev, exists := s.activeDevices[volumeID]; exists {
		if err := blockDev.Disassemble(ctx); err != nil {
			logging.L.Warn("nvme target: failed to disassemble block device", zap.String("volumeID", volumeID), zap.Error(err))
		}
		delete(s.activeDevices, volumeID)
	}

	logging.L.Info("nvme target: target deleted", zap.String("volumeID", volumeID))
	return &pb.DeleteTargetResponse{}, nil
}

// attachLoopDevice runs `losetup -f --show <imgPath>` and returns the loop device path.
func attachLoopDevice(ctx context.Context, imgPath string) (string, error) {
	cmd := exec.CommandContext(ctx, "losetup", "-f", "--show", imgPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("losetup -f --show %s: %w: %s", imgPath, err, string(out))
	}
	return strings.TrimSpace(string(out)), nil
}

// detachLoopDevice runs `losetup -d <loopDev>`.
func detachLoopDevice(ctx context.Context, loopDev string) error {
	cmd := exec.CommandContext(ctx, "losetup", "-d", loopDev)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("losetup -d %s: %w: %s", loopDev, err, string(out))
	}
	return nil
}
