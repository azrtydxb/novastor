package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/piwi3910/novastor/api/proto/nvme"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/nvmeof"
)

const (
	// nvmeTargetPort is the single shared TCP port for all NVMe-oF targets on this node.
	nvmeTargetPort = 4420

	// volumesDir is where sparse backing files are stored.
	volumesDir = "/var/lib/novastor/volumes"

	// nqnPrefix is the NVMe Qualified Name prefix for NovaStor volumes.
	nqnPrefix = "nqn.2024-01.io.novastor:vol-"
)

// NVMeTargetServer implements the NVMeTargetService gRPC server. It creates
// sparse backing files, attaches loop devices, and registers nvmet configfs
// targets so the CSI initiator can connect via NVMe-oF/TCP.
type NVMeTargetServer struct {
	pb.UnimplementedNVMeTargetServiceServer

	hostIP        string
	targetManager *nvmeof.TargetManager
}

// NewNVMeTargetServer creates an NVMeTargetServer that listens on hostIP.
func NewNVMeTargetServer(hostIP string) *NVMeTargetServer {
	return &NVMeTargetServer{
		hostIP:        hostIP,
		targetManager: nvmeof.NewTargetManager(),
	}
}

// Register adds the NVMeTargetService to the given gRPC server.
func (s *NVMeTargetServer) Register(srv *grpc.Server) {
	pb.RegisterNVMeTargetServiceServer(srv, s)
}

// CreateTarget creates a sparse file, attaches a loop device, and exposes it
// as an NVMe-oF/TCP target via the nvmet configfs interface.
func (s *NVMeTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	// Ensure the volumes directory exists.
	if err := os.MkdirAll(volumesDir, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "creating volumes directory: %v", err)
	}

	imgPath := filepath.Join(volumesDir, volumeID+".img")

	// Create a sparse file of the requested size using truncate.
	truncCmd := exec.CommandContext(ctx, "truncate", "-s", fmt.Sprintf("%d", sizeBytes), imgPath)
	if out, err := truncCmd.CombinedOutput(); err != nil {
		return nil, status.Errorf(codes.Internal, "creating sparse file %s: %v: %s", imgPath, err, string(out))
	}
	logging.L.Info("nvme target: sparse file created", zap.String("volumeID", volumeID), zap.String("path", imgPath), zap.Int64("sizeBytes", sizeBytes))

	// Attach a loop device to the sparse file.
	loopDev, err := attachLoopDevice(ctx, imgPath)
	if err != nil {
		_ = os.Remove(imgPath)
		return nil, status.Errorf(codes.Internal, "attaching loop device for %s: %v", imgPath, err)
	}
	logging.L.Info("nvme target: loop device attached", zap.String("volumeID", volumeID), zap.String("loopDev", loopDev))

	// Register the nvmet target backed by the loop device.
	if err := s.targetManager.CreateTargetWithDevice(volumeID, nvmeTargetPort, loopDev, s.hostIP); err != nil {
		_ = detachLoopDevice(ctx, loopDev)
		_ = os.Remove(imgPath)
		return nil, status.Errorf(codes.Internal, "creating nvmet target for volume %s: %v", volumeID, err)
	}

	nqn := nqnPrefix + volumeID
	logging.L.Info("nvme target: target created",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("address", s.hostIP),
		zap.Int("port", nvmeTargetPort),
	)

	return &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", nvmeTargetPort),
	}, nil
}

// DeleteTarget removes the nvmet target, detaches the loop device, and deletes
// the backing sparse file.
func (s *NVMeTargetServer) DeleteTarget(ctx context.Context, req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	// Remove the nvmet target (best-effort; target may not exist on retry).
	if err := s.targetManager.DeleteTarget(volumeID); err != nil {
		logging.L.Warn("nvme target: failed to delete nvmet target", zap.String("volumeID", volumeID), zap.Error(err))
	}

	// Find and detach loop device(s) backing this volume's image.
	imgPath := filepath.Join(volumesDir, volumeID+".img")
	if loopDev, err := findLoopDevice(ctx, imgPath); err == nil && loopDev != "" {
		if err := detachLoopDevice(ctx, loopDev); err != nil {
			logging.L.Warn("nvme target: failed to detach loop device", zap.String("loopDev", loopDev), zap.Error(err))
		}
	}

	// Remove the sparse backing file.
	if err := os.Remove(imgPath); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "removing backing file %s: %v", imgPath, err)
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

// findLoopDevice returns the loop device associated with imgPath using `losetup -j`.
func findLoopDevice(ctx context.Context, imgPath string) (string, error) {
	cmd := exec.CommandContext(ctx, "losetup", "-j", imgPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("losetup -j %s: %w: %s", imgPath, err, string(out))
	}
	// Output format: "/dev/loop0: []: (/path/to/file)"
	line := strings.TrimSpace(string(out))
	if line == "" {
		return "", fmt.Errorf("no loop device found for %s", imgPath)
	}
	parts := strings.SplitN(line, ":", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("unexpected losetup output: %s", line)
	}
	return strings.TrimSpace(parts[0]), nil
}
