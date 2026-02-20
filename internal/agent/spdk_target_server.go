package agent

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/piwi3910/novastor/api/proto/nvme"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/spdk"
)

const (
	// spdkTargetPort is the TCP port used by SPDK NVMe-oF targets on this node.
	spdkTargetPort = 4420

	// spdkNQNPrefix is prepended to the volumeID to form the subsystem NQN.
	spdkNQNPrefix = "novastor-"

	// spdkLvsName is the default logical volume store used for SPDK-backed volumes.
	spdkLvsName = "novastor_lvs"
)

// SPDKTargetServer implements the NVMeTargetService gRPC server using the SPDK
// data-plane process. Instead of chunk-backed block devices and the nvmet configfs
// interface, it delegates logical volume and NVMe-oF target management to the
// Rust data-plane via JSON-RPC.
type SPDKTargetServer struct {
	pb.UnimplementedNVMeTargetServiceServer

	hostIP     string
	spdkClient *spdk.Client
	metaClient *metadata.GRPCClient
}

// NewSPDKTargetServer creates an SPDKTargetServer that exposes NVMe-oF targets
// through the SPDK data-plane process reachable via the provided spdkClient.
func NewSPDKTargetServer(hostIP string, spdkClient *spdk.Client, metaClient *metadata.GRPCClient) *SPDKTargetServer {
	logging.L.Info("spdk target: initialized SPDK target server",
		zap.String("hostIP", hostIP),
	)
	return &SPDKTargetServer{
		hostIP:     hostIP,
		spdkClient: spdkClient,
		metaClient: metaClient,
	}
}

// Register adds the NVMeTargetService to the given gRPC server.
func (s *SPDKTargetServer) Register(srv *grpc.Server) {
	pb.RegisterNVMeTargetServiceServer(srv, s)
}

// CreateTarget creates an SPDK logical volume and exposes it as an NVMe-oF/TCP
// target via the data-plane process. The NQN is derived as "novastor-" + volumeID.
func (s *SPDKTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	nqn := spdkNQNPrefix + volumeID
	sizeMB := uint64(sizeBytes) / (1024 * 1024)
	if sizeMB == 0 {
		sizeMB = 1
	}

	// Create a logical volume in the SPDK data-plane.
	lvolName, err := s.spdkClient.CreateLvol(spdkLvsName, volumeID, sizeMB)
	if err != nil {
		logging.L.Error("spdk target: failed to create lvol",
			zap.String("volumeID", volumeID),
			zap.Uint64("sizeMB", sizeMB),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating SPDK lvol for volume %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: lvol created",
		zap.String("volumeID", volumeID),
		zap.String("lvolName", lvolName),
		zap.Uint64("sizeMB", sizeMB),
	)

	// Expose the lvol as an NVMe-oF/TCP target.
	if err := s.spdkClient.CreateNvmfTarget(nqn, "0.0.0.0", spdkTargetPort, lvolName); err != nil {
		// Best-effort cleanup of the lvol on failure.
		if delErr := s.spdkClient.DeleteBdev(lvolName); delErr != nil {
			logging.L.Warn("spdk target: failed to clean up lvol after target creation failure",
				zap.String("volumeID", volumeID),
				zap.String("lvolName", lvolName),
				zap.Error(delErr),
			)
		}
		logging.L.Error("spdk target: failed to create NVMe-oF target",
			zap.String("volumeID", volumeID),
			zap.String("nqn", nqn),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating SPDK NVMe-oF target for volume %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: target created",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("address", s.hostIP),
		zap.Int("port", spdkTargetPort),
		zap.String("lvolName", lvolName),
	)

	return &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", spdkTargetPort),
	}, nil
}

// DeleteTarget removes the SPDK NVMe-oF target subsystem and deletes the
// backing logical volume via the data-plane process.
func (s *SPDKTargetServer) DeleteTarget(ctx context.Context, req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	nqn := spdkNQNPrefix + volumeID

	// Remove the NVMe-oF target (best-effort; target may not exist on retry).
	if err := s.spdkClient.DeleteNvmfTarget(nqn); err != nil {
		logging.L.Warn("spdk target: failed to delete NVMe-oF target",
			zap.String("volumeID", volumeID),
			zap.String("nqn", nqn),
			zap.Error(err),
		)
	}

	// Delete the backing lvol bdev. The lvol name follows the SPDK convention
	// of "lvsName/lvolName", which matches what CreateLvol returns.
	lvolBdevName := spdkLvsName + "/" + volumeID
	if err := s.spdkClient.DeleteBdev(lvolBdevName); err != nil {
		logging.L.Warn("spdk target: failed to delete lvol bdev",
			zap.String("volumeID", volumeID),
			zap.String("bdevName", lvolBdevName),
			zap.Error(err),
		)
	}

	logging.L.Info("spdk target: target deleted", zap.String("volumeID", volumeID))
	return &pb.DeleteTargetResponse{}, nil
}
