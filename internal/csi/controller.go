package csi

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/metrics"
)

const (
	// chunkSize is the fixed chunk size in bytes (4 MiB).
	chunkSize uint64 = 4 * 1024 * 1024

	// defaultVolumeSize is the fallback volume size when none is requested.
	defaultVolumeSize uint64 = 1 * 1024 * 1024 * 1024 // 1 GiB
)

// MetadataStore is the subset of the metadata service used by the controller.
type MetadataStore interface {
	PutVolumeMeta(ctx context.Context, meta *metadata.VolumeMeta) error
	GetVolumeMeta(ctx context.Context, volumeID string) (*metadata.VolumeMeta, error)
	DeleteVolumeMeta(ctx context.Context, volumeID string) error
	ListVolumesMeta(ctx context.Context) ([]*metadata.VolumeMeta, error)
	// Placement map methods for recovery management.
	PutPlacementMap(ctx context.Context, pm *metadata.PlacementMap) error
	DeletePlacementMap(ctx context.Context, chunkID string) error
}

// NodeMetaStore provides access to node metadata for capacity calculations.
type NodeMetaStore interface {
	// ListLiveNodeMetas returns nodes that have sent a heartbeat within the TTL.
	ListLiveNodeMetas(ctx context.Context, ttl time.Duration) ([]*metadata.NodeMeta, error)
}

// PlacementEngine selects storage nodes for new chunks.
type PlacementEngine interface {
	Place(count int) []string
}

// AgentTargetClient abstracts NVMe-oF target creation/deletion on agent nodes.
// When nil, the controller skips NVMe-oF target management (backward compatible).
type AgentTargetClient interface {
	// CreateTarget creates an NVMe-oF target on the agent and returns connection params.
	CreateTarget(ctx context.Context, agentAddr string, volumeID string, sizeBytes int64) (subsystemNQN, targetAddress, targetPort string, err error)
	// DeleteTarget tears down the NVMe-oF target on the agent.
	DeleteTarget(ctx context.Context, agentAddr string, volumeID string) error
}

// ControllerServer implements the CSI Controller service.
type ControllerServer struct {
	csi.UnimplementedControllerServer
	meta        MetadataStore
	nodeMeta    NodeMetaStore
	placer      PlacementEngine
	agentTarget AgentTargetClient
}

// NewControllerServer creates a ControllerServer backed by the given stores.
// agentTarget may be nil to disable NVMe-oF target management.
func NewControllerServer(meta MetadataStore, placer PlacementEngine, agentTarget AgentTargetClient) *ControllerServer {
	return &ControllerServer{
		meta:        meta,
		placer:      placer,
		agentTarget: agentTarget,
	}
}

// NewControllerServerWithNodeMeta creates a ControllerServer with node metadata support.
func NewControllerServerWithNodeMeta(meta MetadataStore, nodeMeta NodeMetaStore, placer PlacementEngine, agentTarget AgentTargetClient) *ControllerServer {
	return &ControllerServer{
		meta:        meta,
		nodeMeta:    nodeMeta,
		placer:      placer,
		agentTarget: agentTarget,
	}
}

// hasRWXCapability checks whether any of the volume capabilities request
// MULTI_NODE_MULTI_WRITER (ReadWriteMany) access.
func hasRWXCapability(caps []*csi.VolumeCapability) bool {
	for _, cap := range caps {
		if am := cap.GetAccessMode(); am != nil {
			if am.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
				return true
			}
		}
	}
	return false
}

// CreateVolume provisions a new volume by computing chunks and persisting metadata.
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	start := time.Now()
	defer func() {
		metrics.VolumeProvisionDuration.Observe(time.Since(start).Seconds())
	}()

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	// Determine requested capacity.
	requiredBytes := defaultVolumeSize
	if capRange := req.GetCapacityRange(); capRange != nil {
		if capRange.GetRequiredBytes() > 0 {
			requiredBytes = uint64(capRange.GetRequiredBytes())
		}
	}

	// Calculate chunk count (round up).
	chunkCount := int((requiredBytes + chunkSize - 1) / chunkSize)

	// Place chunks across storage nodes.
	nodeIDs := cs.placer.Place(chunkCount)
	if len(nodeIDs) == 0 {
		return nil, status.Error(codes.ResourceExhausted, "no storage nodes available for placement")
	}

	volumeID := uuid.New().String()

	chunkIDs := make([]string, chunkCount)
	for i := range chunkCount {
		chunkIDs[i] = fmt.Sprintf("%s-chunk-%04d", volumeID, i)
	}

	vm := &metadata.VolumeMeta{
		VolumeID:  volumeID,
		SizeBytes: requiredBytes,
		ChunkIDs:  chunkIDs,
	}

	// Write placement maps for each chunk. This is critical for recovery:
	// if a node fails, the recovery manager uses these maps to know which
	// chunks were on that node and need re-replication.
	// Each chunk is placed on the node at the same index in nodeIDs.
	for i, chunkID := range chunkIDs {
		if i < len(nodeIDs) {
			pm := &metadata.PlacementMap{
				ChunkID: chunkID,
				Nodes:   []string{nodeIDs[i]},
			}
			if err := cs.meta.PutPlacementMap(ctx, pm); err != nil {
				return nil, status.Errorf(codes.Internal, "writing placement map for chunk %s: %v", chunkID, err)
			}
		}
	}

	// Set volume context for RWX (NFS-backed) volumes.
	volContext := map[string]string{}
	if hasRWXCapability(req.GetVolumeCapabilities()) {
		volContext["nfsServer"] = nodeIDs[0]
		volContext["nfsShare"] = fmt.Sprintf("/exports/%s", volumeID)
		volContext["accessMode"] = "RWX"
	} else if cs.agentTarget != nil {
		// For RWO block volumes, create an NVMe-oF target on the first placed node.
		// NOTE: nodeIDs[0] is used as the agent address. The PlacementEngine must
		//       return network addresses (host:port) that can be used for gRPC calls
		//       to the agent's NVMeTargetService. If the placement engine returns
		//       logical node IDs that differ from network addresses, a separate
		//       nodeID→address mapping mechanism is required.
		agentAddr := nodeIDs[0]
		nqn, targetAddr, targetPort, targetErr := cs.agentTarget.CreateTarget(ctx, agentAddr, volumeID, int64(requiredBytes))
		if targetErr != nil {
			return nil, status.Errorf(codes.Internal, "creating NVMe-oF target for volume %s: %v", volumeID, targetErr)
		}
		volContext["targetAddress"] = targetAddr
		volContext["targetPort"] = targetPort
		volContext["subsystemNQN"] = nqn

		// Persist target fields in volume metadata.
		vm.TargetNodeID = nodeIDs[0]
		vm.TargetAddress = targetAddr
		vm.TargetPort = targetPort
		vm.SubsystemNQN = nqn
	}

	if err := cs.meta.PutVolumeMeta(ctx, vm); err != nil {
		// Clean up target if metadata storage fails.
		if cs.agentTarget != nil && vm.SubsystemNQN != "" {
			_ = cs.agentTarget.DeleteTarget(ctx, nodeIDs[0], volumeID)
		}
		return nil, status.Errorf(codes.Internal, "storing volume metadata: %v", err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: int64(requiredBytes),
			VolumeContext: volContext,
		},
	}, nil
}

// DeleteVolume removes a volume's metadata.
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	start := time.Now()
	defer func() {
		metrics.VolumeDeleteDuration.Observe(time.Since(start).Seconds())
	}()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Verify the volume exists. If not found, succeed idempotently per CSI spec.
	vm, err := cs.meta.GetVolumeMeta(ctx, volumeID)
	if err != nil {
		return &csi.DeleteVolumeResponse{}, nil
	}

	// Tear down NVMe-oF target before removing metadata (best-effort).
	// If the target deletion fails (e.g., agent unreachable), we still proceed
	// with metadata cleanup to avoid blocking volume deletion on retries.
	if cs.agentTarget != nil && vm.TargetNodeID != "" {
		if deleteErr := cs.agentTarget.DeleteTarget(ctx, vm.TargetNodeID, volumeID); deleteErr != nil {
			logging.L.Warn("failed to delete NVMe-oF target (proceeding with metadata cleanup)",
				zap.String("volumeID", volumeID),
				zap.String("targetNodeID", vm.TargetNodeID),
				zap.Error(deleteErr))
		}
	}

	// Clean up placement maps for all chunks in this volume (best-effort).
	for _, chunkID := range vm.ChunkIDs {
		if deleteErr := cs.meta.DeletePlacementMap(ctx, chunkID); deleteErr != nil {
			// Log but don't fail the delete operation if placement map cleanup fails.
			// The volume metadata takes precedence; orphaned placement maps will be
			// cleaned up by the garbage collector.
			logging.L.Warn("failed to delete placement map (proceeding with metadata cleanup)",
				zap.String("volumeID", volumeID),
				zap.String("chunkID", chunkID),
				zap.Error(deleteErr))
		}
	}

	if err := cs.meta.DeleteVolumeMeta(ctx, volumeID); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting volume metadata: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities checks that the requested access modes are supported.
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Verify the volume exists.
	if _, err := cs.meta.GetVolumeMeta(ctx, req.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
	}

	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	for _, cap := range req.GetVolumeCapabilities() {
		accessMode := cap.GetAccessMode()
		if accessMode == nil {
			continue
		}
		switch accessMode.GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:
			// Supported -- ReadWriteOnce.
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
			// Supported -- ReadWriteMany via NFS.
		default:
			return &csi.ValidateVolumeCapabilitiesResponse{
				Message: fmt.Sprintf("unsupported access mode: %v", accessMode.GetMode()),
			}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// ListVolumes returns all volumes known to the metadata store.
// It supports the optional max_entries and starting_token pagination fields
// from the CSI spec, but the token is a simple volume-ID cursor.
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	vols, err := cs.meta.ListVolumesMeta(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing volumes: %v", err)
	}

	// Apply starting_token offset (token is a volumeID; start after it).
	startIdx := 0
	if token := req.GetStartingToken(); token != "" {
		found := false
		for i, v := range vols {
			if v.VolumeID == token {
				startIdx = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, status.Errorf(codes.Aborted, "invalid starting_token: volume %s not found", token)
		}
	}
	vols = vols[startIdx:]

	// Apply max_entries limit.
	var nextToken string
	maxEntries := int(req.GetMaxEntries())
	if maxEntries > 0 && len(vols) > maxEntries {
		nextToken = vols[maxEntries].VolumeID
		vols = vols[:maxEntries]
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(vols))
	for _, vm := range vols {
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      vm.VolumeID,
				CapacityBytes: int64(vm.SizeBytes),
			},
		})
	}

	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

// ControllerPublishVolume publishes a volume to a node.
// For NovaStor, this is a lightweight validation that the volume exists.
// The actual volume attachment (NVMe-oF connection or NFS mount) happens
// at the node level via NodeStageVolume/NodePublishVolume.
// This implementation is idempotent: republishing an already published
// volume succeeds with the same publish context.
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	start := time.Now()
	defer func() {
		metrics.VolumePublishDuration.Observe(time.Since(start).Seconds())
	}()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	nodeID := req.GetNodeId()
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}

	// Verify the volume exists.
	vm, err := cs.meta.GetVolumeMeta(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	// Build publish context with information the node will need.
	publishContext := map[string]string{
		"volumeId": volumeID,
	}

	// Determine access type from volume metadata.
	// For RWX (NFS) volumes, the CreateVolume sets volume context with accessMode="RWX".
	// For block volumes with NVMe-oF targets, SubsystemNQN will be populated.
	if vm.SubsystemNQN != "" {
		// RWO block volume with NVMe-oF target.
		publishContext["accessType"] = "nvmeof"
		publishContext["subsystemNQN"] = vm.SubsystemNQN
		publishContext["targetAddress"] = vm.TargetAddress
		publishContext["targetPort"] = vm.TargetPort
		publishContext["targetNodeID"] = vm.TargetNodeID
	} else {
		// Basic block volume (without NVMe-oF) or NFS volume.
		// The node will determine the actual access method based on
		// volume capabilities passed to NodeStageVolume/NodePublishVolume.
		publishContext["accessType"] = "block"
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: publishContext,
	}, nil
}

// ControllerUnpublishVolume unpublishes a volume from a node.
// For NovaStor, this is a no-op at the controller level since the
// actual detachment happens at the node level. This implementation
// is idempotent: unpublishing a non-existent or already unpublished
// volume succeeds.
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	start := time.Now()
	defer func() {
		metrics.VolumeUnpublishDuration.Observe(time.Since(start).Seconds())
	}()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// NodeID is optional per CSI spec for unpublish (can be empty for forced cleanup).
	// We don't enforce node-specific tracking at the controller level.

	// Verify the volume exists. If not, succeed idempotently per CSI spec.
	if _, err := cs.meta.GetVolumeMeta(ctx, volumeID); err != nil {
		// Volume doesn't exist or was already deleted - succeed idempotently.
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// No controller-side state to clean up. The node handles the actual detachment.
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// GetCapacity returns the available storage capacity.
// If topology information is provided in the request, returns capacity
// for that segment only. Otherwise, returns total cluster capacity.
// The capacity is calculated from live nodes' available storage,
// divided by the replica factor for replicated volumes.
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	start := time.Now()
	defer func() {
		metrics.CapacityQueryDuration.Observe(time.Since(start).Seconds())
	}()

	// If node metadata store is not available, return zero capacity.
	if cs.nodeMeta == nil {
		return &csi.GetCapacityResponse{
			AvailableCapacity: 0,
		}, nil
	}

	// List live nodes (nodes that have sent a heartbeat recently).
	// Use a generous TTL to account for network delays and missed heartbeats.
	const nodeHeartbeatTTL = 2 * time.Minute
	nodes, err := cs.nodeMeta.ListLiveNodeMetas(ctx, nodeHeartbeatTTL)
	if err != nil {
		return &csi.GetCapacityResponse{
			AvailableCapacity: 0,
		}, nil
	}

	// Sum available capacity across all live nodes.
	var totalCapacity uint64
	for _, n := range nodes {
		if n.AvailableCapacity > 0 {
			totalCapacity += uint64(n.AvailableCapacity)
		}
	}

	// Divide by replica factor (assume 3-way replication for safety).
	// This ensures we don't over-provision when replicas need to be maintained.
	const replicaFactor = 3
	availableCapacity := totalCapacity / uint64(replicaFactor)

	return &csi.GetCapacityResponse{
		AvailableCapacity: int64(availableCapacity),
	}, nil
}

// ControllerGetCapabilities returns the controller capabilities.
func (cs *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES,
	}

	var capabilities []*csi.ControllerServiceCapability
	for _, c := range caps {
		capabilities = append(capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: c,
				},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}
