// Package agent provides the NovaStor node agent logic.
// This file implements the NVMeTargetService using the Linux kernel's nvmet
// configfs interface. It creates sparse backing files on the NVMe storage disk,
// attaches loop devices, and exposes them as NVMe-oF/TCP targets via nvmet.
package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/azrtydxb/novastor/api/proto/nvme"
	"github.com/azrtydxb/novastor/internal/logging"
)

const (
	// nvmetConfigFS is the root of the NVMe target configfs.
	nvmetConfigFS = "/sys/kernel/config/nvmet"

	// nvmetTargetPort is the TCP port used by kernel NVMe-oF targets.
	// We use 14420 instead of the standard 4420 to avoid conflict with
	// NovaNET's eBPF socket-LB which intercepts well-known ports.
	nvmetTargetPort = 14420

	// nvmetNQNPrefix is prepended to the volumeID to form the subsystem NQN.
	nvmetNQNPrefix = "novastor-"

	// volumeDir is the directory for sparse backing files on the NVMe storage.
	volumeDir = "/var/lib/novastor/volumes"
)

// KernelTargetServer implements the NVMeTargetService gRPC server using the
// Linux kernel's nvmet configfs interface. Each volume gets a sparse backing
// file on the local NVMe storage, attached via a loop device and exposed as
// an NVMe-oF/TCP target.
type KernelTargetServer struct {
	pb.UnimplementedNVMeTargetServiceServer

	hostIP string

	// mu protects loopDevices and portCreated.
	mu          sync.Mutex
	loopDevices map[string]string // volumeID -> loop device path
	portCreated bool              // whether the nvmet port has been created
}

// NewKernelTargetServer creates a KernelTargetServer that exposes NVMe-oF
// targets through the Linux kernel nvmet subsystem.
func NewKernelTargetServer(hostIP string) *KernelTargetServer {
	logging.L.Info("kernel nvmet target: initialized",
		zap.String("hostIP", hostIP),
		zap.String("volumeDir", volumeDir),
	)
	return &KernelTargetServer{
		hostIP:      hostIP,
		loopDevices: make(map[string]string),
	}
}

// Register adds the NVMeTargetService to the given gRPC server.
func (s *KernelTargetServer) Register(srv *grpc.Server) {
	pb.RegisterNVMeTargetServiceServer(srv, s)
}

// ensureVolumeDir creates the volume backing file directory if it doesn't exist.
func ensureVolumeDir() error {
	return os.MkdirAll(volumeDir, 0o755)
}

// backingFilePath returns the path to the sparse backing file for a volume.
func backingFilePath(volumeID string) string {
	return filepath.Join(volumeDir, volumeID+".img")
}

// ensurePort creates the nvmet port for TCP transport if it doesn't already exist.
// NVMe-oF targets share a single port (portID=1) that listens on 0.0.0.0:4420.
func (s *KernelTargetServer) ensurePort() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.portCreated {
		return nil
	}

	portDir := filepath.Join(nvmetConfigFS, "ports", "1")
	if _, err := os.Stat(portDir); err == nil {
		s.portCreated = true
		return nil
	}

	if err := os.MkdirAll(portDir, 0o755); err != nil {
		return fmt.Errorf("creating nvmet port dir: %w", err)
	}

	writes := map[string]string{
		"addr_trtype":  "tcp",
		"addr_trsvcid": strconv.Itoa(nvmetTargetPort),
		"addr_traddr":  "0.0.0.0",
		"addr_adrfam":  "ipv4",
	}
	for attr, val := range writes {
		if err := os.WriteFile(filepath.Join(portDir, attr), []byte(val), 0o644); err != nil {
			return fmt.Errorf("writing nvmet port attr %s=%s: %w", attr, val, err)
		}
	}

	s.portCreated = true
	logging.L.Info("kernel nvmet target: port created",
		zap.Int("port", nvmetTargetPort),
	)
	return nil
}

// createSparseFile creates a sparse file of the given size. The file takes
// no disk space until data is actually written (thin provisioning).
func createSparseFile(path string, sizeBytes int64) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating backing file: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(sizeBytes); err != nil {
		return fmt.Errorf("truncating backing file to %d bytes: %w", sizeBytes, err)
	}

	return nil
}

// attachLoop attaches a backing file to a loop device and returns the device path.
func attachLoop(ctx context.Context, backingFile string) (string, error) {
	out, err := exec.CommandContext(ctx, "losetup", "--find", "--show", backingFile).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("losetup: %s: %w", strings.TrimSpace(string(out)), err)
	}
	return strings.TrimSpace(string(out)), nil
}

// detachLoop detaches a loop device.
func detachLoop(ctx context.Context, loopDev string) error {
	out, err := exec.CommandContext(ctx, "losetup", "--detach", loopDev).CombinedOutput()
	if err != nil {
		return fmt.Errorf("losetup --detach %s: %s: %w", loopDev, strings.TrimSpace(string(out)), err)
	}
	return nil
}

// createNvmetSubsystem sets up the nvmet configfs entries for a subsystem:
// subsystem -> namespace -> backing device, then links it to the port.
func createNvmetSubsystem(nqn, loopDev string) error {
	subsysDir := filepath.Join(nvmetConfigFS, "subsystems", nqn)
	if err := os.MkdirAll(subsysDir, 0o755); err != nil {
		return fmt.Errorf("creating subsystem dir: %w", err)
	}

	// Allow any host to connect (required for CSI node initiators).
	if err := os.WriteFile(filepath.Join(subsysDir, "attr_allow_any_host"), []byte("1"), 0o644); err != nil {
		return fmt.Errorf("setting allow_any_host: %w", err)
	}

	// Create namespace 1 within the subsystem.
	nsDir := filepath.Join(subsysDir, "namespaces", "1")
	if err := os.MkdirAll(nsDir, 0o755); err != nil {
		return fmt.Errorf("creating namespace dir: %w", err)
	}

	// Point the namespace to the loop device.
	if err := os.WriteFile(filepath.Join(nsDir, "device_path"), []byte(loopDev), 0o644); err != nil {
		return fmt.Errorf("writing device_path: %w", err)
	}

	// Enable the namespace.
	if err := os.WriteFile(filepath.Join(nsDir, "enable"), []byte("1"), 0o644); err != nil {
		return fmt.Errorf("enabling namespace: %w", err)
	}

	// Link the subsystem to port 1.
	portSubsysLink := filepath.Join(nvmetConfigFS, "ports", "1", "subsystems", nqn)
	if err := os.Symlink(subsysDir, portSubsysLink); err != nil && !os.IsExist(err) {
		return fmt.Errorf("linking subsystem to port: %w", err)
	}

	return nil
}

// removeNvmetSubsystem tears down the nvmet configfs entries for a subsystem.
func removeNvmetSubsystem(nqn string) {
	// Remove port link first.
	portLink := filepath.Join(nvmetConfigFS, "ports", "1", "subsystems", nqn)
	_ = os.Remove(portLink)

	subsysDir := filepath.Join(nvmetConfigFS, "subsystems", nqn)

	// Disable and remove namespace.
	nsDir := filepath.Join(subsysDir, "namespaces", "1")
	_ = os.WriteFile(filepath.Join(nsDir, "enable"), []byte("0"), 0o644)
	_ = os.Remove(nsDir)

	// Remove subsystem.
	_ = os.Remove(subsysDir)
}

// CreateTarget creates a sparse backing file on the NVMe storage, attaches it
// to a loop device, and exposes it as an NVMe-oF/TCP target via kernel nvmet.
func (s *KernelTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	nqn := nvmetNQNPrefix + volumeID

	// Ensure the nvmet TCP port exists.
	if err := s.ensurePort(); err != nil {
		return nil, status.Errorf(codes.Internal, "ensuring nvmet port: %v", err)
	}

	// Create volume directory.
	if err := ensureVolumeDir(); err != nil {
		return nil, status.Errorf(codes.Internal, "creating volume directory: %v", err)
	}

	// Create sparse backing file (thin provisioned).
	backingFile := backingFilePath(volumeID)
	if err := createSparseFile(backingFile, sizeBytes); err != nil {
		return nil, status.Errorf(codes.Internal, "creating sparse file for volume %s: %v", volumeID, err)
	}

	logging.L.Info("kernel nvmet target: sparse backing file created",
		zap.String("volumeID", volumeID),
		zap.String("path", backingFile),
		zap.Int64("sizeBytes", sizeBytes),
	)

	// Attach to a loop device.
	loopDev, err := attachLoop(ctx, backingFile)
	if err != nil {
		os.Remove(backingFile)
		return nil, status.Errorf(codes.Internal, "attaching loop device for volume %s: %v", volumeID, err)
	}

	s.mu.Lock()
	s.loopDevices[volumeID] = loopDev
	s.mu.Unlock()

	logging.L.Info("kernel nvmet target: loop device attached",
		zap.String("volumeID", volumeID),
		zap.String("loopDev", loopDev),
	)

	// Create nvmet subsystem and link to port.
	if err := createNvmetSubsystem(nqn, loopDev); err != nil {
		detachLoop(ctx, loopDev)
		os.Remove(backingFile)
		s.mu.Lock()
		delete(s.loopDevices, volumeID)
		s.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "creating nvmet subsystem for volume %s: %v", volumeID, err)
	}

	logging.L.Info("kernel nvmet target: target created",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("address", s.hostIP),
		zap.Int("port", nvmetTargetPort),
		zap.String("loopDev", loopDev),
		zap.String("backingFile", backingFile),
	)

	// ANA state is not supported by the basic nvmet configfs interface.
	// Log if requested but don't fail.
	if req.GetAnaState() != "" {
		logging.L.Debug("kernel nvmet target: ANA state requested but not supported in kernel mode",
			zap.String("volumeID", volumeID),
			zap.String("anaState", req.GetAnaState()),
		)
	}

	return &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", nvmetTargetPort),
	}, nil
}

// DeleteTarget removes the NVMe-oF target, detaches the loop device, and
// deletes the sparse backing file.
func (s *KernelTargetServer) DeleteTarget(ctx context.Context, req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	nqn := nvmetNQNPrefix + volumeID

	// Remove nvmet subsystem (best-effort).
	removeNvmetSubsystem(nqn)

	// Detach loop device.
	s.mu.Lock()
	loopDev, ok := s.loopDevices[volumeID]
	if ok {
		delete(s.loopDevices, volumeID)
	}
	s.mu.Unlock()

	if ok {
		if err := detachLoop(ctx, loopDev); err != nil {
			logging.L.Warn("kernel nvmet target: failed to detach loop device",
				zap.String("volumeID", volumeID),
				zap.String("loopDev", loopDev),
				zap.Error(err),
			)
		}
	}

	// Remove backing file (best-effort).
	backingFile := backingFilePath(volumeID)
	if err := os.Remove(backingFile); err != nil && !os.IsNotExist(err) {
		logging.L.Warn("kernel nvmet target: failed to remove backing file",
			zap.String("volumeID", volumeID),
			zap.String("path", backingFile),
			zap.Error(err),
		)
	}

	logging.L.Info("kernel nvmet target: target deleted", zap.String("volumeID", volumeID))
	return &pb.DeleteTargetResponse{}, nil
}

// SetANAState is a no-op for kernel nvmet. Basic nvmet configfs does not
// support per-subsystem ANA groups without the nvmet-ana extension.
func (s *KernelTargetServer) SetANAState(ctx context.Context, req *pb.SetANAStateRequest) (*pb.SetANAStateResponse, error) {
	logging.L.Debug("kernel nvmet target: SetANAState is a no-op in kernel mode",
		zap.String("volumeID", req.GetVolumeId()),
		zap.String("anaState", req.GetAnaState()),
	)
	return &pb.SetANAStateResponse{}, nil
}
