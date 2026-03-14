package agent

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dppb "github.com/azrtydxb/novastor/api/proto/dataplane"
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
	testMode   bool
	dpClient   *dataplane.Client
	metaClient *metadata.GRPCClient

	// chunkInit tracks whether the chunk backend has been initialised.
	// Uses mutex+bool instead of sync.Once so transient failures can be retried.
	initMu   sync.Mutex
	initDone bool
	initErr  error
}

// NewSPDKTargetServer creates an SPDKTargetServer that exposes NVMe-oF targets
// through the SPDK data-plane process reachable via the provided gRPC client.
// baseBdev is the name of the SPDK bdev to use as the backing device for the
// chunk backend (e.g. "NVMe0n1" for real NVMe drives, "Malloc0" for testing).
// testMode must be true to allow Malloc bdev auto-creation; in production this
// should always be false and only real NVMe/AIO bdevs are accepted.
func NewSPDKTargetServer(hostIP, baseBdev string, testMode bool, dpClient *dataplane.Client, metaClient *metadata.GRPCClient) *SPDKTargetServer {
	logging.L.Info("spdk target: initialized SPDK target server",
		zap.String("hostIP", hostIP),
		zap.String("baseBdev", baseBdev),
		zap.Bool("testMode", testMode),
	)
	return &SPDKTargetServer{
		hostIP:     hostIP,
		baseBdev:   baseBdev,
		testMode:   testMode,
		dpClient:   dpClient,
		metaClient: metaClient,
	}
}

// Register adds the NVMeTargetService to the given gRPC server.
func (s *SPDKTargetServer) Register(srv *grpc.Server) {
	pb.RegisterNVMeTargetServiceServer(srv, s)
}

// ensureChunkBackend initialises the chunk storage backend on the SPDK
// data-plane. The underlying bdev is determined by baseBdev (e.g. "NVMe0n1"
// for real NVMe drives, "Malloc0" for testing). This is called lazily so the
// agent can start even if the storage bdev isn't immediately available.
//
// The chunk backend stores data as content-addressed 4MB chunks on the bdev,
// matching the NovaStor architecture: Backend → Chunk Engine → NVMe-oF.
//
// Uses mutex+bool instead of sync.Once so transient init failures (e.g.
// dataplane not ready yet) can be retried on subsequent CreateTarget calls.
func (s *SPDKTargetServer) ensureChunkBackend() error {
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
	if _, err := s.dpClient.InitChunkStore(s.baseBdev); err != nil {
		// The chunk store may already be initialised if the agent restarted
		// while the dataplane kept running.
		if !strings.Contains(err.Error(), "already") {
			return fmt.Errorf("initialising chunk store on %s: %w", s.baseBdev, err)
		}
		logging.L.Info("spdk target: chunk store already initialised, reusing",
			zap.String("baseBdev", s.baseBdev),
		)
	}

	logging.L.Info("spdk target: chunk backend initialised",
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

	// Ensure chunk backend is ready.
	if err := s.ensureChunkBackend(); err != nil {
		return nil, status.Errorf(codes.Internal, "chunk backend init: %v", err)
	}

	// Create chunk volume via Rust data-plane gRPC. This also registers an SPDK
	// bdev named "novastor_<volumeID>" that bridges block I/O to the chunk engine.
	bdevName, _, err := s.dpClient.CreateVolume("chunk", volumeID, uint64(sizeBytes))
	if err != nil {
		logging.L.Error("spdk target: failed to create chunk volume",
			zap.String("volumeID", volumeID),
			zap.Int64("sizeBytes", sizeBytes),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating chunk volume for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: chunk volume created",
		zap.String("volumeID", volumeID),
		zap.String("bdevName", bdevName),
		zap.Int64("sizeBytes", sizeBytes),
	)

	// Expose the chunk bdev as an NVMe-oF/TCP target via gRPC.
	// Use the specific host IP (not 0.0.0.0) because SPDK's listener ACL
	// performs a literal address comparison: the subsystem listener address
	// must match the address the client connects to.
	anaState := req.GetAnaState()
	anaGroupID := req.GetAnaGroupId()
	nqn, err := s.dpClient.CreateNvmfTarget(volumeID, bdevName, s.hostIP, spdkTargetPort, anaState, anaGroupID)
	if err != nil {
		// Cleanup chunk volume on failure.
		if delErr := s.dpClient.DeleteVolume("chunk", volumeID); delErr != nil {
			logging.L.Warn("spdk target: failed to clean up chunk volume",
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

// DeleteTarget removes the NVMe-oF target and the chunk volume via the data-plane.
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

	// Remove the chunk volume via Rust data-plane (best-effort).
	// This also destroys the novastor_<volumeID> SPDK bdev.
	if err := s.dpClient.DeleteVolume("chunk", volumeID); err != nil {
		logging.L.Warn("spdk target: failed to delete chunk volume",
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

// SetupReplication configures the owner node to replicate writes to remote
// replica targets. It:
// 1. Connects as NVMe-oF initiator to each remote target
// 2. Creates a replica bdev combining local chunk bdev + remote initiator bdevs
// 3. Tears down the simple NVMe-oF target
// 4. Re-exports using the replica bdev as the backing device
//
// After this, all writes to this volume's NVMe-oF target are fanned out
// to all replicas with majority quorum by the Rust dataplane's replica bdev.
func (s *SPDKTargetServer) SetupReplication(ctx context.Context, req *pb.SetupReplicationRequest) (*pb.SetupReplicationResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	localBdevName := req.GetLocalBdevName()
	if localBdevName == "" {
		return nil, status.Error(codes.InvalidArgument, "local_bdev_name is required")
	}
	remoteTargets := req.GetRemoteTargets()
	if len(remoteTargets) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one remote target is required")
	}

	logging.L.Info("spdk target: setting up replication",
		zap.String("volumeID", volumeID),
		zap.String("localBdev", localBdevName),
		zap.Int("remoteCount", len(remoteTargets)),
	)

	// Step 1: Connect as NVMe-oF initiator to each remote replica target.
	var connectedBdevs []string
	var connectedNQNs []string
	for _, rt := range remoteTargets {
		_, err := strconv.ParseUint(rt.GetPort(), 10, 16)
		if err != nil {
			// Clean up already connected initiators.
			for _, nqn := range connectedNQNs {
				_ = s.dpClient.DisconnectInitiator(nqn)
			}
			return nil, status.Errorf(codes.InvalidArgument, "invalid port %q for remote target %s: %v", rt.GetPort(), rt.GetAddress(), err)
		}

		remoteBdev, err := s.dpClient.ConnectInitiator(rt.GetAddress(), rt.GetPort(), rt.GetNqn())
		if err != nil {
			logging.L.Error("spdk target: failed to connect initiator to remote replica",
				zap.String("volumeID", volumeID),
				zap.String("remoteAddr", rt.GetAddress()),
				zap.Error(err),
			)
			// Clean up already connected initiators.
			for _, nqn := range connectedNQNs {
				_ = s.dpClient.DisconnectInitiator(nqn)
			}
			return nil, status.Errorf(codes.Internal, "connecting initiator to %s for volume %s: %v", rt.GetAddress(), volumeID, err)
		}
		connectedBdevs = append(connectedBdevs, remoteBdev)
		connectedNQNs = append(connectedNQNs, rt.GetNqn())
		logging.L.Info("spdk target: connected initiator to remote replica",
			zap.String("volumeID", volumeID),
			zap.String("remoteAddr", rt.GetAddress()),
			zap.String("remoteBdev", remoteBdev),
		)
	}

	// Step 2: Build replica target list (local + remote bdevs).
	replicaTargets := make([]*dppb.ReplicaTarget, 0, 1+len(connectedBdevs))

	// Local chunk bdev — marked as local for read preference.
	replicaTargets = append(replicaTargets, &dppb.ReplicaTarget{
		BdevName: localBdevName,
		IsLocal:  true,
	})

	// Remote initiator bdevs.
	for _, remoteBdev := range connectedBdevs {
		replicaTargets = append(replicaTargets, &dppb.ReplicaTarget{
			BdevName: remoteBdev,
			IsLocal:  false,
		})
	}

	// Step 3: Create the composite replica bdev.
	sizeBytes := req.GetSizeBytes()
	replicaBdevName, err := s.dpClient.CreateReplicaBdev(volumeID, replicaTargets, uint64(sizeBytes), "local_first")
	if err != nil {
		logging.L.Error("spdk target: failed to create replica bdev",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
		for _, nqn := range connectedNQNs {
			_ = s.dpClient.DisconnectInitiator(nqn)
		}
		return nil, status.Errorf(codes.Internal, "creating replica bdev for %s: %v", volumeID, err)
	}
	logging.L.Info("spdk target: replica bdev created",
		zap.String("volumeID", volumeID),
		zap.String("replicaBdev", replicaBdevName),
		zap.Int("totalReplicas", len(replicaTargets)),
	)

	// Step 4: Tear down the old NVMe-oF target backed by the simple chunk bdev.
	if err := s.dpClient.DeleteNvmfTarget(volumeID); err != nil {
		logging.L.Warn("spdk target: failed to delete old NVMe-oF target (may not exist)",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
	}

	// Step 5: Re-export with the replica bdev as the backing device.
	nqn, err := s.dpClient.CreateNvmfTarget(volumeID, replicaBdevName, s.hostIP, spdkTargetPort, "", 0)
	if err != nil {
		logging.L.Error("spdk target: failed to re-export with replica bdev, restoring original target",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
		// Rollback: recreate the original target with the chunk bdev so the volume remains accessible.
		if _, restoreErr := s.dpClient.CreateNvmfTarget(volumeID, localBdevName, s.hostIP, spdkTargetPort, "", 0); restoreErr != nil {
			logging.L.Error("spdk target: failed to restore original NVMe-oF target",
				zap.String("volumeID", volumeID),
				zap.Error(restoreErr),
			)
		} else {
			logging.L.Info("spdk target: restored original NVMe-oF target after replication failure",
				zap.String("volumeID", volumeID),
				zap.String("bdev", localBdevName),
			)
		}
		return nil, status.Errorf(codes.Internal, "re-exporting replica bdev for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: replication setup complete",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("replicaBdev", replicaBdevName),
		zap.Int("totalReplicas", len(replicaTargets)),
	)

	return &pb.SetupReplicationResponse{
		ReplicaBdevName: replicaBdevName,
		SubsystemNqn:    nqn,
	}, nil
}
