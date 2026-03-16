package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/azrtydxb/novastor/api/proto/nvme"
	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
)

const (
	// spdkTargetPort is the TCP port used by SPDK NVMe-oF targets on this node.
	spdkTargetPort = 4430

	// mallocBdevSizeMB is the size of the auto-created Malloc bdev.
	// Must fit within the SPDK memory allocation minus SPDK/DPDK overhead (~200MB).
	// With memSize=1800 and DPDK overhead, ~1500MB is available for bdevs.
	mallocBdevSizeMB = 1024
)

// SPDKTargetServer implements the NVMeTargetService gRPC server using the SPDK
// data-plane process. Volumes are created as chunk-backed volumes in the Rust
// dataplane and exposed as NVMe-oF/TCP targets. All data-path I/O flows through
// the Rust SPDK data-plane's chunk engine; the Go agent never touches data.
type SPDKTargetServer struct {
	pb.UnimplementedNVMeTargetServiceServer

	hostIP     string
	baseBdev   string
	nodeUUID   string
	testMode   bool
	dpClient   *dataplane.Client
	metaClient *metadata.GRPCClient

	// chunkInit tracks whether the chunk store has been initialised.
	// Uses mutex+bool instead of sync.Once so transient failures can be retried.
	initMu   sync.Mutex
	initDone bool
	initErr  error
}

// NewSPDKTargetServer creates an SPDKTargetServer that exposes NVMe-oF targets
// through the SPDK data-plane process reachable via the provided gRPC client.
// baseBdev is the name of the SPDK bdev to use as the backing device for the
// storage backend (e.g. "NVMe0n1" for real NVMe drives, "Malloc0" for testing).
// testMode must be true to allow Malloc bdev auto-creation; in production this
// should always be false and only real NVMe/AIO bdevs are accepted.
func NewSPDKTargetServer(hostIP, baseBdev, nodeUUID string, testMode bool, dpClient *dataplane.Client, metaClient *metadata.GRPCClient) *SPDKTargetServer {
	logging.L.Info("spdk target: initialized SPDK target server",
		zap.String("hostIP", hostIP),
		zap.String("baseBdev", baseBdev),
		zap.String("nodeUUID", nodeUUID),
		zap.Bool("testMode", testMode),
	)
	return &SPDKTargetServer{
		hostIP:     hostIP,
		baseBdev:   baseBdev,
		nodeUUID:   nodeUUID,
		testMode:   testMode,
		dpClient:   dpClient,
		metaClient: metaClient,
	}
}

// Register adds the NVMeTargetService to the given gRPC server.
func (s *SPDKTargetServer) Register(srv *grpc.Server) {
	pb.RegisterNVMeTargetServiceServer(srv, s)
}

// ensureChunkStore initialises the chunk store on the SPDK data-plane.
// The underlying bdev is determined by baseBdev (e.g. "NVMe0n1" for real
// NVMe drives, "Malloc0" for testing). This is called lazily so the agent
// can start even if the storage bdev isn't immediately available.
//
// The chunk store sits atop the storage backend and stores content-addressed
// 4MB chunks. Architecture: Backend (Raw/LVM/File) → Chunk Engine → NVMe-oF.
//
// Uses mutex+bool instead of sync.Once so transient init failures (e.g.
// dataplane not ready yet) can be retried on subsequent CreateTarget calls.
func (s *SPDKTargetServer) ensureChunkStore() error {
	s.initMu.Lock()
	defer s.initMu.Unlock()

	if s.initDone {
		return s.initErr
	}

	// Auto-create a Malloc bdev only when --test-mode is explicitly enabled.
	// In production, only real NVMe/AIO bdevs are accepted.
	if strings.HasPrefix(s.baseBdev, "Malloc") {
		if !s.testMode {
			return fmt.Errorf("malloc bdev %q requested but --test-mode is not enabled; "+
				"use real NVMe/AIO bdevs in production or pass --test-mode for testing", s.baseBdev)
		}
		logging.L.Warn("spdk target: creating malloc bdev (TEST MODE ONLY — not for production)",
			zap.String("name", s.baseBdev),
			zap.Uint64("sizeMB", mallocBdevSizeMB),
		)
		if _, err := s.dpClient.CreateMallocBdev(s.baseBdev, mallocBdevSizeMB, 512); err != nil {
			// EEXIST — the bdev persists across agent restarts.
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("creating malloc bdev %s: %w", s.baseBdev, err)
			}
			logging.L.Info("spdk target: malloc bdev already exists, reusing",
				zap.String("name", s.baseBdev),
			)
		}
	}

	// Initialise the chunk store on the Rust data-plane via gRPC.
	// Use the nodeUUID which matches the topology node IDs pushed to the dataplane.
	if _, err := s.dpClient.InitChunkStore(s.baseBdev, s.nodeUUID); err != nil {
		// The chunk store may already be initialised if the agent restarted
		// while the dataplane kept running.
		if !strings.Contains(err.Error(), "already") {
			return fmt.Errorf("initialising chunk store on %s: %w", s.baseBdev, err)
		}
		logging.L.Info("spdk target: chunk store already initialised, reusing",
			zap.String("baseBdev", s.baseBdev),
		)
	}

	logging.L.Info("spdk target: chunk store initialised",
		zap.String("baseBdev", s.baseBdev),
	)

	s.initDone = true
	s.initErr = nil
	return nil
}

// CreateTarget creates a chunk-backed volume and exposes it as an NVMe-oF/TCP
// target via the data-plane process. The volume is stored as content-addressed
// 4MB chunks in the Rust dataplane's chunk engine.
func (s *SPDKTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	// Ensure chunk store is ready.
	if err := s.ensureChunkStore(); err != nil {
		return nil, status.Errorf(codes.Internal, "chunk store init: %v", err)
	}

	// Create volume via Rust data-plane gRPC. This registers an SPDK bdev
	// named "novastor_<volumeID>" that bridges block I/O to the chunk engine.
	// The volume is a virtual bdev backed by content-addressed 4MB chunks —
	// it does NOT map to a physical device. The backend type is irrelevant
	// at volume creation time; only the chunk engine matters.
	bdevName, _, err := s.dpClient.CreateVolume("", volumeID, uint64(sizeBytes))
	if err != nil {
		logging.L.Error("spdk target: failed to create volume",
			zap.String("volumeID", volumeID),
			zap.Int64("sizeBytes", sizeBytes),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating volume for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: chunk volume created",
		zap.String("volumeID", volumeID),
		zap.String("bdevName", bdevName),
		zap.Int64("sizeBytes", sizeBytes),
	)

	// Set per-volume protection policy on the dataplane's policy engine.
	if prot := req.GetProtection(); prot != nil {
		var replicas, dataShards, parityShards uint32
		if prot.GetDataShards() > 0 && prot.GetParityShards() > 0 {
			// Erasure coding mode
			dataShards = prot.GetDataShards()
			parityShards = prot.GetParityShards()
		} else if prot.GetReplicationFactor() > 0 {
			// Replication mode
			replicas = prot.GetReplicationFactor()
		}
		if replicas > 0 || dataShards > 0 {
			if accepted, err := s.dpClient.SetVolumePolicy(volumeID, replicas, dataShards, parityShards); err != nil {
				logging.L.Warn("spdk target: failed to set volume policy (non-fatal)",
					zap.String("volumeID", volumeID),
					zap.Error(err),
				)
			} else if accepted {
				logging.L.Info("spdk target: volume policy set",
					zap.String("volumeID", volumeID),
					zap.Uint32("replicas", replicas),
					zap.Uint32("dataShards", dataShards),
					zap.Uint32("parityShards", parityShards),
				)
			}
		}
	}

	// Expose the chunk bdev as an NVMe-oF/TCP target via gRPC.
	// Use the specific host IP (not 0.0.0.0) because SPDK's listener ACL
	// performs a literal address comparison: the subsystem listener address
	// must match the address the client connects to.
	anaState := req.GetAnaState()
	anaGroupID := req.GetAnaGroupId()
	nqn, err := s.dpClient.CreateNvmfTarget(volumeID, bdevName, s.hostIP, spdkTargetPort, anaState, anaGroupID)
	if err != nil {
		// Cleanup volume on failure.
		if delErr := s.dpClient.DeleteVolume("", volumeID); delErr != nil {
			logging.L.Warn("spdk target: failed to clean up volume",
				zap.String("volumeID", volumeID),
				zap.Error(delErr),
			)
		}
		logging.L.Error("spdk target: failed to create NVMe-oF target",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating NVMe-oF target for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: target created",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("address", s.hostIP),
		zap.Int("port", spdkTargetPort),
		zap.String("bdevName", bdevName),
	)

	return &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", spdkTargetPort),
	}, nil
}

// DeleteTarget removes the NVMe-oF target and the volume via the data-plane.
func (s *SPDKTargetServer) DeleteTarget(ctx context.Context, req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	// Remove the NVMe-oF target (best-effort; target may not exist on retry).
	if err := s.dpClient.DeleteNvmfTarget(volumeID); err != nil {
		logging.L.Warn("spdk target: failed to delete NVMe-oF target",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
	}

	// Remove the volume via Rust data-plane (best-effort).
	// This destroys the novastor_<volumeID> virtual bdev and its chunk map.
	if err := s.dpClient.DeleteVolume("", volumeID); err != nil {
		logging.L.Warn("spdk target: failed to delete volume",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
	}

	logging.L.Info("spdk target: target deleted", zap.String("volumeID", volumeID))
	return &pb.DeleteTargetResponse{}, nil
}

// SetANAState changes the ANA state for a volume's NVMe-oF target subsystem.
func (s *SPDKTargetServer) SetANAState(ctx context.Context, req *pb.SetANAStateRequest) (*pb.SetANAStateResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id required")
	}

	// The gRPC dataplane service converts volumeID to NQN internally.
	if err := s.dpClient.SetANAState(volumeID, req.GetAnaGroupId(), req.GetAnaState()); err != nil {
		return nil, status.Errorf(codes.Internal, "set ANA state for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: ANA state updated",
		zap.String("volumeID", volumeID),
		zap.String("anaState", req.GetAnaState()),
		zap.Uint32("anaGroupID", req.GetAnaGroupId()),
	)

	return &pb.SetANAStateResponse{}, nil
}
