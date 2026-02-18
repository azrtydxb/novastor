package csi

import (
	"context"
	"fmt"
	"strconv"
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

	// StorageClass parameter keys.
	paramReplicas     = "replicas"
	paramDataShards   = "dataShards"
	paramParityShards = "parityShards"

	// Default protection settings.
	defaultReplicas = 1

	// TopologyDomain is the topology key used for segment topology.
	// It enables WaitForFirstConsumer binding mode for data locality.
	TopologyDomain = "novastor.io/node"
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

// protectionConfig holds parsed protection parameters from StorageClass.
type protectionConfig struct {
	replicas     int
	dataShards   int
	parityShards int
}

// parseProtectionParams extracts protection parameters from StorageClass parameters.
// Returns default configuration (1 replica, no erasure coding) if no parameters are specified.
func parseProtectionParams(params map[string]string) protectionConfig {
	cfg := protectionConfig{
		replicas: defaultReplicas,
	}

	if params == nil {
		return cfg
	}

	if v, ok := params[paramReplicas]; ok {
		if r, err := strconv.Atoi(v); err == nil && r > 0 {
			cfg.replicas = r
		}
	}

	if v, ok := params[paramDataShards]; ok {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			cfg.dataShards = d
		}
	}

	if v, ok := params[paramParityShards]; ok {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			cfg.parityShards = p
		}
	}

	return cfg
}

// toProtectionProfile converts protectionConfig to metadata.ProtectionProfile.
// Returns nil if using default (1 replica, no EC).
func (cfg protectionConfig) toProtectionProfile() *metadata.ProtectionProfile {
	// Default case: single replica, no protection profile needed.
	if cfg.replicas <= 1 && cfg.dataShards == 0 {
		return nil
	}

	profile := &metadata.ProtectionProfile{}

	if cfg.dataShards > 0 && cfg.parityShards > 0 {
		// Erasure coding mode.
		profile.Mode = metadata.ProtectionModeErasureCoding
		profile.ErasureCoding = &metadata.ErasureCodingProfile{
			DataShards:   cfg.dataShards,
			ParityShards: cfg.parityShards,
		}
	} else if cfg.replicas > 1 {
		// Replication mode.
		profile.Mode = metadata.ProtectionModeReplication
		profile.Replication = &metadata.ReplicationProfile{
			Factor:      cfg.replicas,
			WriteQuorum: cfg.replicas/2 + 1, // Majority quorum.
		}
	}

	return profile
}

// requiredNodes returns the number of nodes needed for the protection scheme.
func (cfg protectionConfig) requiredNodes() int {
	if cfg.dataShards > 0 && cfg.parityShards > 0 {
		// Erasure coding: need data + parity shards.
		return cfg.dataShards + cfg.parityShards
	}
	if cfg.replicas > 0 {
		return cfg.replicas
	}
	return 1
}

// extractTopologyRequirement extracts the topology requirement from the request.
// It returns the list of topology segments that the volume must be accessible from,
// or nil if no topology requirement is specified (e.g., immediate binding mode).
func extractTopologyRequirement(req *csi.CreateVolumeRequest) []*csi.Topology {
	if req.GetAccessibilityRequirements() == nil {
		return nil
	}
	return req.GetAccessibilityRequirements().GetPreferred()
}

// filterNodesByTopology filters the given node IDs to those that match at least
// one of the required topology segments. If no topology requirement is given,
// all nodes are returned.
//
// For NovaStor, we match based on the TopologyDomain key (novastor.io/node).
// This enables the WaitForFirstConsumer binding mode: the scheduler provides
// the topology of the consumer node, and we place volume data on that node
// for data locality.
func filterNodesByTopology(nodeIDs []string, required []*csi.Topology) []string {
	if len(required) == 0 {
		// No topology requirement: all nodes are eligible.
		return nodeIDs
	}

	// Build a set of required node IDs from the topology segments.
	requiredNodeIDs := make(map[string]struct{})
	for _, topo := range required {
		if nodeID, ok := topo.Segments[TopologyDomain]; ok {
			requiredNodeIDs[nodeID] = struct{}{}
		}
	}

	// Filter nodes that match at least one of the required segments.
	var filtered []string
	for _, nodeID := range nodeIDs {
		if _, ok := requiredNodeIDs[nodeID]; ok {
			filtered = append(filtered, nodeID)
		}
	}

	// If no nodes match the topology requirement, we still return all nodes.
	// This allows the volume to be provisioned even when topology constraints
	// cannot be satisfied (fallback behavior).
	if len(filtered) == 0 {
		logging.L.Warn("no nodes match topology requirement, using all nodes",
			zap.Int("requiredSegments", len(required)),
			zap.Int("availableNodes", len(nodeIDs)))
		return nodeIDs
	}

	return filtered
}

// buildVolumeTopology constructs the topology information for a volume based on
// the nodes where its chunks are placed. This is returned in CreateVolumeResponse
// as AccessibleTopology, which informs the scheduler where the volume is reachable.
func buildVolumeTopology(nodeIDs []string) []*csi.Topology {
	if len(nodeIDs) == 0 {
		return nil
	}

	// Deduplicate node IDs while preserving order.
	seen := make(map[string]struct{})
	topologies := make([]*csi.Topology, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if _, ok := seen[nodeID]; !ok {
			seen[nodeID] = struct{}{}
			topologies = append(topologies, &csi.Topology{
				Segments: map[string]string{
					TopologyDomain: nodeID,
				},
			})
		}
	}

	return topologies
}

// CreateVolume provisions a new volume by computing chunks and persisting metadata.
// It respects topology requirements from the accessibility requirements for
// topology-aware provisioning (WaitForFirstConsumer binding mode).
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	start := time.Now()
	defer func() {
		metrics.VolumeProvisionDuration.Observe(time.Since(start).Seconds())
	}()

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	// Parse protection parameters from StorageClass.
	protCfg := parseProtectionParams(req.GetParameters())

	// Determine requested capacity.
	requiredBytes := defaultVolumeSize
	if capRange := req.GetCapacityRange(); capRange != nil {
		if capRange.GetRequiredBytes() > 0 {
			requiredBytes = uint64(capRange.GetRequiredBytes())
		}
	}

	// Calculate chunk count (round up).
	chunkCount := int((requiredBytes + chunkSize - 1) / chunkSize)

	// Calculate how many nodes we need for placement.
	// For replication: each chunk needs 'replicas' nodes.
	// For erasure coding: each chunk needs dataShards + parityShards nodes.
	nodesPerChunk := protCfg.requiredNodes()

	// Extract topology requirement for topology-aware provisioning.
	topologyReq := extractTopologyRequirement(req)

	// Place chunks across storage nodes.
	// We need chunkCount * nodesPerChunk placement slots.
	totalSlots := chunkCount * nodesPerChunk
	allNodeIDs := cs.placer.Place(totalSlots)
	if len(allNodeIDs) < totalSlots {
		return nil, status.Errorf(codes.ResourceExhausted,
			"not enough storage nodes: need %d for %d chunks with protection factor %d, got %d",
			totalSlots, chunkCount, nodesPerChunk, len(allNodeIDs))
	}

	// Filter nodes based on topology requirement (if any).
	// This enables WaitForFirstConsumer: pods are scheduled on nodes with data locality.
	// When topology is specified, we try to use only nodes matching the topology.
	nodeIDs := filterNodesByTopology(allNodeIDs, topologyReq)

	// If topology filtering reduced our available nodes below what we need for protection,
	// we still proceed with the filtered nodes (best-effort). The placement will use
	// fewer nodes than requested for protection, which is acceptable for topology-aware
	// scenarios where data locality is prioritized over full protection.
	if len(topologyReq) > 0 && len(nodeIDs) > 0 {
		logging.L.Info("CreateVolume: topology-aware placement",
			zap.String("volumeName", req.GetName()),
			zap.Int("requiredSegments", len(topologyReq)),
			zap.Int("requestedNodes", len(allNodeIDs)),
			zap.Int("selectedNodes", len(nodeIDs)),
			zap.Strings("nodeIDs", nodeIDs))
	}

	volumeID := uuid.New().String()

	chunkIDs := make([]string, chunkCount)
	for i := range chunkCount {
		chunkIDs[i] = fmt.Sprintf("%s-chunk-%04d", volumeID, i)
	}

	vm := &metadata.VolumeMeta{
		VolumeID:          volumeID,
		SizeBytes:         requiredBytes,
		ChunkIDs:          chunkIDs,
		ProtectionProfile: protCfg.toProtectionProfile(),
	}

	// Write placement maps for each chunk with multiple nodes for protection.
	// Each chunk is placed on 'nodesPerChunk' consecutive nodes from nodeIDs.
	for i, chunkID := range chunkIDs {
		// Calculate the slice of nodes for this chunk.
		startIdx := i * nodesPerChunk
		endIdx := startIdx + nodesPerChunk
		if endIdx > len(nodeIDs) {
			endIdx = len(nodeIDs)
		}
		chunkNodes := nodeIDs[startIdx:endIdx]

		pm := &metadata.PlacementMap{
			ChunkID: chunkID,
			Nodes:   chunkNodes,
		}
		if err := cs.meta.PutPlacementMap(ctx, pm); err != nil {
			return nil, status.Errorf(codes.Internal, "writing placement map for chunk %s: %v", chunkID, err)
		}

		logging.L.Debug("placed chunk with protection",
			zap.String("volumeID", volumeID),
			zap.String("chunkID", chunkID),
			zap.Strings("nodes", chunkNodes),
			zap.Int("replicas", nodesPerChunk))
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

	// Build accessible topology from the nodes where chunks are placed.
	// This informs the Kubernetes scheduler where the volume is reachable.
	accessibleTopology := buildVolumeTopology(nodeIDs)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volumeID,
			CapacityBytes:      int64(requiredBytes),
			VolumeContext:      volContext,
			AccessibleTopology: accessibleTopology,
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
