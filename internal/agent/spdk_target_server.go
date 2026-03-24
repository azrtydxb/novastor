package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
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

	// skipNVMeF disables NVMe-oF target creation in the agent when a frontend
	// controller handles NVMe-oF presentation via kernel nvmet + NBD.
	skipNVMeF bool

	// chunkInit tracks whether the chunk store has been initialised.
	// Uses mutex+bool instead of sync.Once so transient failures can be retried.
	initMu   sync.Mutex
	initDone bool
	initErr  error

	// volumes tracks active NVMe-oF targets by volume ID for reconciliation.
	volumesMu sync.RWMutex
	volumes   map[string]*pb.CreateTargetResponse // volumeID → response
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
		skipNVMeF:  false, // Use SPDK reactor for NVMe-oF (burns 1 core, max performance).
		volumes:    make(map[string]*pb.CreateTargetResponse),
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
	// Retry up to 5 times with 2s delay — the BackendAssignment reconciler
	// may still be initializing the backend bdev on this node.
	var initErr error
	for attempt := 0; attempt < 5; attempt++ {
		if _, err := s.dpClient.InitChunkStore(s.baseBdev, s.nodeUUID); err != nil {
			if strings.Contains(err.Error(), "already") {
				logging.L.Info("spdk target: chunk store already initialised, reusing",
					zap.String("baseBdev", s.baseBdev),
				)
				initErr = nil
				break
			}
			initErr = err
			if attempt < 4 {
				logging.L.Warn("spdk target: chunk store init failed, retrying",
					zap.String("baseBdev", s.baseBdev),
					zap.Int("attempt", attempt+1),
					zap.Error(err),
				)
				s.initMu.Unlock()
				time.Sleep(2 * time.Second)
				s.initMu.Lock()
				continue
			}
		} else {
			initErr = nil
			break
		}
	}
	if initErr != nil {
		return fmt.Errorf("initialising chunk store on %s after retries: %w", s.baseBdev, initErr)
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
	_, span := otel.Tracer("novastor-agent").Start(ctx, "SPDKTargetServer.CreateTarget")
	defer span.End()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	// Determine whether this node operates in frontend-only mode (no local
	// chunk store). A node is frontend-only when no base bdev is configured.
	frontendOnly := s.baseBdev == ""

	// Ensure chunk store is ready — skip on frontend-only nodes that have
	// no local backend bdev.
	if !frontendOnly {
		if err := s.ensureChunkStore(); err != nil {
			return nil, status.Errorf(codes.Internal, "chunk store init: %v", err)
		}
	} else {
		logging.L.Info("spdk target: frontend-only mode — skipping chunk store init",
			zap.String("volumeID", volumeID),
		)
	}

	// Create volume via Rust data-plane gRPC. This registers an SPDK bdev
	// named "novastor_<volumeID>" that bridges block I/O to the chunk engine.
	// The volume is a virtual bdev backed by content-addressed 4MB chunks —
	// it does NOT map to a physical device. The backend type is irrelevant
	// at volume creation time; only the chunk engine matters.
	// On frontend-only nodes, the dataplane creates a remote-only ChunkEngine
	// that routes all I/O to backends on other nodes via NDP.
	bdevName, _, err := s.dpClient.CreateVolume("", volumeID, uint64(sizeBytes), frontendOnly)
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

	// When skipNVMeF is set, the frontend controller handles NVMe-oF target
	// creation via kernel nvmet + NBD. The agent only creates the volume.
	if s.skipNVMeF {
		nqn := fmt.Sprintf("nqn.2024-01.io.novastor:volume-%s", volumeID)
		logging.L.Info("spdk target: volume created (NVMe-oF handled by frontend)",
			zap.String("volumeID", volumeID),
			zap.String("bdevName", bdevName),
		)
		resp := &pb.CreateTargetResponse{
			SubsystemNqn:  nqn,
			TargetAddress: s.hostIP,
			TargetPort:    fmt.Sprintf("%d", spdkTargetPort),
		}
		s.volumesMu.Lock()
		s.volumes[volumeID] = resp
		s.volumesMu.Unlock()
		return resp, nil
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

	resp := &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", spdkTargetPort),
	}
	s.volumesMu.Lock()
	s.volumes[volumeID] = resp
	s.volumesMu.Unlock()
	return resp, nil
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

	s.volumesMu.Lock()
	delete(s.volumes, volumeID)
	s.volumesMu.Unlock()

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

// ListTargets returns all currently active NVMe-oF targets on this node.
// Used by the reconciliation loop to compare desired vs actual state.
func (s *SPDKTargetServer) ListTargets(ctx context.Context, req *pb.ListTargetsRequest) (*pb.ListTargetsResponse, error) {
	s.volumesMu.RLock()
	defer s.volumesMu.RUnlock()

	entries := make([]*pb.TargetEntry, 0, len(s.volumes))
	for volID, resp := range s.volumes {
		entries = append(entries, &pb.TargetEntry{
			VolumeId:      volID,
			SubsystemNqn:  resp.GetSubsystemNqn(),
			TargetAddress: resp.GetTargetAddress(),
			TargetPort:    resp.GetTargetPort(),
		})
	}
	return &pb.ListTargetsResponse{Targets: entries}, nil
}

// ResetInit clears the init state so ensureChunkStore will retry.
// Called after dataplane restart when all SPDK state is lost.
func (s *SPDKTargetServer) ResetInit() {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	s.initDone = false
	s.initErr = nil
}

// EnsureChunkStore is the public wrapper for ensureChunkStore.
func (s *SPDKTargetServer) EnsureChunkStore() error {
	return s.ensureChunkStore()
}
