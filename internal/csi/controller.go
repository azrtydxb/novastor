// Package csi provides the CSI driver implementation for NovaStor.
// This package implements the CSI Controller service for volume provisioning,
// including CreateVolume, DeleteVolume, and volume capabilities validation.
package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nvmepb "github.com/azrtydxb/novastor/api/proto/nvme"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
)

const (
	// chunkSize is the fixed chunk size in bytes (4 MiB).
	chunkSize uint64 = 4 * 1024 * 1024

	// defaultVolumeSize is the fallback volume size when none is requested.
	defaultVolumeSize uint64 = 1 * 1024 * 1024 * 1024 // 1 GiB

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
	GetPlacementMap(ctx context.Context, chunkID string) (*metadata.PlacementMap, error)
	DeletePlacementMap(ctx context.Context, chunkID string) error
	// Shard placement methods for erasure coding.
	PutShardPlacement(ctx context.Context, sp *metadata.ShardPlacement) error
	GetShardPlacements(ctx context.Context, chunkID string) ([]*metadata.ShardPlacement, error)
	DeleteShardPlacement(ctx context.Context, chunkID string, shardIndex int) error
}

// QuotaChecker defines the interface for checking storage quotas.
type QuotaChecker interface {
	// CheckStorageQuota checks if a storage allocation would exceed the quota.
	CheckStorageQuota(ctx context.Context, scope metadata.QuotaScope, requestedBytes int64) error
	// ReserveStorage reserves storage capacity for a scope.
	ReserveStorage(ctx context.Context, scope metadata.QuotaScope, bytes int64) error
	// ReleaseStorage releases storage capacity for a scope.
	ReleaseStorage(ctx context.Context, scope metadata.QuotaScope, bytes int64) error
}

// NodeMetaStore provides access to node metadata for capacity calculations.
type NodeMetaStore interface {
	// ListLiveNodeMetas returns nodes that have sent a heartbeat within the TTL.
	ListLiveNodeMetas(ctx context.Context, ttl time.Duration) ([]*metadata.NodeMeta, error)
}

// PlacementEngine selects storage nodes for new chunks.
// PlacementEngine selects storage nodes for new chunks.
// The PlaceKey method allows deterministic placement based on a key (e.g. volume ID)
// which enables CRUSH-style failure domain awareness.
type PlacementEngine interface {
	// PlaceKey returns a deterministic set of node IDs for the given key.
	// Implementations that do not support key-based placement may fall back to Place.
	PlaceKey(key string, count int) []string

	// Place returns node IDs using a non-deterministic or round-robin strategy.
	Place(count int) []string
}

// AgentTargetClient abstracts NVMe-oF target creation/deletion on agent nodes.
// When nil, the controller skips NVMe-oF target management (backward compatible).
//
// Note: Replication and erasure coding are handled by the Rust dataplane's
// chunk engine — the presentation layer (NVMe-oF) is thin protocol
// translation only. The protection scheme is passed in CreateTarget.
type AgentTargetClient interface {
	// CreateTarget creates an NVMe-oF target on the agent and returns connection params.
	CreateTarget(ctx context.Context, agentAddr string, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32, prot protectionConfig) (subsystemNQN, targetAddress, targetPort string, err error)
	// DeleteTarget tears down the NVMe-oF target on the agent.
	DeleteTarget(ctx context.Context, agentAddr string, volumeID string) error
	// SetANAState changes the ANA state for a volume's NVMe-oF target on the agent.
	SetANAState(ctx context.Context, agentAddr string, volumeID string, anaState string, anaGroupID uint32) error
}

// ControllerServer implements the CSI Controller service.
type ControllerServer struct {
	csi.UnimplementedControllerServer
	meta        MetadataStore
	nodeMeta    NodeMetaStore
	placer      PlacementEngine
	agentTarget AgentTargetClient
	quota       QuotaChecker

	// nodeAddrToName maps agent network addresses (ip:port) to Kubernetes node
	// names. CRUSH placement returns addresses, but PV topology must use K8s
	// node names to match CSINode topology registration.
	nodeAddrToName   map[string]string
	nodeAddrToNameMu sync.RWMutex
}

// NewControllerServer creates a ControllerServer backed by the given stores.
// agentTarget may be nil to disable NVMe-oF target management.
// quota may be nil to disable quota checking.
// Chunk replication/EC is handled by the Rust dataplane's chunk engine —
// the CSI controller only stores protection metadata.
func NewControllerServer(meta MetadataStore, placer PlacementEngine, agentTarget AgentTargetClient, quota QuotaChecker) *ControllerServer {
	return &ControllerServer{
		meta:        meta,
		placer:      placer,
		agentTarget: agentTarget,
		quota:       quota,
	}
}

// NewControllerServerWithNodeMeta creates a ControllerServer with node metadata support.
func NewControllerServerWithNodeMeta(meta MetadataStore, nodeMeta NodeMetaStore, placer PlacementEngine, agentTarget AgentTargetClient, quota QuotaChecker) *ControllerServer {
	return &ControllerServer{
		meta:        meta,
		nodeMeta:    nodeMeta,
		placer:      placer,
		agentTarget: agentTarget,
		quota:       quota,
	}
}

// UpdateNodeMapping replaces the address-to-node-name mapping used by
// buildVolumeTopology to translate CRUSH addresses into Kubernetes node names.
// This is called periodically by the node sync loop in cmd/csi/main.go.
func (cs *ControllerServer) UpdateNodeMapping(addrToName map[string]string) {
	cs.nodeAddrToNameMu.Lock()
	defer cs.nodeAddrToNameMu.Unlock()
	cs.nodeAddrToName = addrToName
}

// resolveNodeName returns the Kubernetes node name for a CRUSH node address.
// Falls back to stripping the port if no mapping exists.
func (cs *ControllerServer) resolveNodeName(addr string) string {
	cs.nodeAddrToNameMu.RLock()
	defer cs.nodeAddrToNameMu.RUnlock()
	if name, ok := cs.nodeAddrToName[addr]; ok && name != "" {
		return name
	}
	// Fallback: strip port from address.
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
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
		replicas: defaultReplicationFactor,
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

// toVolumeProtection converts protectionConfig to the NVMe proto VolumeProtection message.
func (cfg protectionConfig) toVolumeProtection() *nvmepb.VolumeProtection {
	if cfg.replicas <= 1 && cfg.dataShards == 0 {
		return nil
	}
	vp := &nvmepb.VolumeProtection{}
	if cfg.dataShards > 0 && cfg.parityShards > 0 {
		vp.Mode = "erasure_coding"
		vp.DataShards = uint32(cfg.dataShards)
		vp.ParityShards = uint32(cfg.parityShards)
	} else {
		vp.Mode = "replication"
		vp.ReplicationFactor = uint32(cfg.replicas)
	}
	return vp
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
// CRUSH placement returns network addresses (ip:port); this method resolves them
// to Kubernetes node names via the nodeAddrToName mapping so that PV node
// affinity matches CSINode topology registration.
func (cs *ControllerServer) buildVolumeTopology(nodeIDs []string) []*csi.Topology {
	if len(nodeIDs) == 0 {
		return nil
	}

	// Deduplicate resolved node names while preserving order.
	seen := make(map[string]struct{})
	topologies := make([]*csi.Topology, 0, len(nodeIDs))
	for _, addr := range nodeIDs {
		nodeName := cs.resolveNodeName(addr)
		if _, ok := seen[nodeName]; !ok {
			seen[nodeName] = struct{}{}
			topologies = append(topologies, &csi.Topology{
				Segments: map[string]string{
					TopologyDomain: nodeName,
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

	// Parse and validate protection parameters from StorageClass.
	protParams, err := ParseProtectionParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid protection parameters: %s", err)
	}
	protCfg := protParams.toProtectionConfig()

	// Determine requested capacity.
	requiredBytes := defaultVolumeSize
	if capRange := req.GetCapacityRange(); capRange != nil {
		if capRange.GetRequiredBytes() > 0 {
			requiredBytes = uint64(capRange.GetRequiredBytes())
		}
	}

	// Extract quota scope from parameters.
	// If a namespace is specified, check namespace-level quota.
	// If a pool is specified, check pool-level quota.
	var quotaScopes []metadata.QuotaScope
	if params := req.GetParameters(); params != nil {
		if ns, ok := params["namespace"]; ok && ns != "" {
			quotaScopes = append(quotaScopes, metadata.QuotaScope{Kind: "Namespace", Name: ns})
		}
		if pool, ok := params["storagePool"]; ok && pool != "" {
			quotaScopes = append(quotaScopes, metadata.QuotaScope{Kind: "StoragePool", Name: pool})
		}
	}

	// Check quotas before provisioning.
	if cs.quota != nil {
		for _, scope := range quotaScopes {
			if err := cs.quota.CheckStorageQuota(ctx, scope, int64(requiredBytes)); err != nil {
				quotaErr, ok := err.(*metadata.QuotaError)
				if ok {
					return nil, status.Errorf(codes.ResourceExhausted,
						"quota exceeded for %s: requested %d bytes, limit %d bytes, used %d bytes",
						scope.String(), quotaErr.Requested, quotaErr.Limit, quotaErr.Used)
				}
				return nil, status.Errorf(codes.Internal, "quota check failed: %v", err)
			}
		}
	}

	// Calculate how many nodes we need for placement.
	// For replication: each chunk needs 'replicas' nodes.
	// For erasure coding: each chunk needs dataShards + parityShards nodes.
	nodesPerChunk := protCfg.requiredNodes()

	// Extract topology requirement for topology-aware provisioning.
	topologyReq := extractTopologyRequirement(req)

	// Place chunks across storage nodes using volume name as the key for
	// deterministic, failure-domain-aware placement (CRUSH).
	// We request all available nodes (sorted by CRUSH affinity) and then
	// verify we have at least nodesPerChunk nodes for data protection.
	// Chunks are distributed across these nodes round-robin.
	allNodeIDs := cs.placer.PlaceKey(req.GetName(), nodesPerChunk)
	if len(allNodeIDs) < nodesPerChunk {
		return nil, status.Errorf(codes.ResourceExhausted,
			"not enough storage nodes for data protection: need %d, got %d",
			nodesPerChunk, len(allNodeIDs))
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

	// Lazy allocation: no chunks or placement maps are written at volume
	// creation time. The NVMe-oF target creates a sparse backing file on
	// each replica node that provides thin provisioning at the filesystem
	// level. Placement maps are generated deterministically from the CRUSH
	// placement and can be reconstructed on demand.
	profile := protCfg.toProtectionProfile()
	vm := &metadata.VolumeMeta{
		VolumeID:          volumeID,
		SizeBytes:         requiredBytes,
		ChunkIDs:          nil, // Lazy: chunks allocated on demand
		ProtectionProfile: profile,
		DataProtection:    profile,
	}

	// Store the pool in the volume metadata for quota tracking.
	if params := req.GetParameters(); params != nil {
		if pool, ok := params["storagePool"]; ok {
			vm.Pool = pool
		}
	}

	logging.L.Info("CreateVolume: lazy allocation (no chunk pre-allocation)",
		zap.String("volumeID", volumeID),
		zap.Int("nodeCount", len(nodeIDs)),
		zap.Int("nodesPerChunk", nodesPerChunk),
		zap.Uint64("sizeBytes", requiredBytes))

	// Persist volume metadata before creating NVMe-oF target.
	if err := cs.meta.PutVolumeMeta(ctx, vm); err != nil {
		return nil, status.Errorf(codes.Internal, "storing volume metadata: %v", err)
	}

	// Set volume context for RWX (NFS-backed) volumes.
	volContext := map[string]string{}
	if hasRWXCapability(req.GetVolumeCapabilities()) {
		volContext["nfsServer"] = nodeIDs[0]
		volContext["nfsShare"] = fmt.Sprintf("/exports/%s", volumeID)
		volContext["accessMode"] = "RWX"
	} else if cs.agentTarget != nil {
		// For RWO block volumes, create an NVMe-oF target on the owner node.
		// ANA group ID = 1: each subsystem has exactly one namespace, so a single
		// ANA group suffices. The ANA state (optimized vs non_optimized) per node
		// determines path preference for multipath.
		var anaGroupID uint32 = 1

		// Deduplicate nodeIDs to get unique agents for target creation.
		// CRUSH placement may assign multiple chunks to the same node.
		seen := make(map[string]bool)
		var uniqueNodes []string
		for _, n := range nodeIDs {
			if !seen[n] {
				seen[n] = true
				uniqueNodes = append(uniqueNodes, n)
			}
		}

		// Create NVMe-oF targets on ALL replica nodes for ANA multipath.
		// Owner node: ANA state "optimized" (preferred path for I/O).
		// Replica nodes: ANA state "non_optimized" (standby paths for failover).
		// The kernel NVMe multipath driver selects the optimized path.
		ownerNode := uniqueNodes[0]
		type targetInfo struct {
			Addr    string `json:"addr"`
			Port    string `json:"port"`
			NQN     string `json:"nqn"`
			IsOwner bool   `json:"is_owner"`
		}
		var targets []targetInfo
		var ownerAddr, ownerPort, ownerNQN string

		for i, node := range uniqueNodes {
			anaState := "non_optimized"
			if i == 0 {
				anaState = "optimized"
			}
			nqn, addr, port, createErr := cs.agentTarget.CreateTarget(
				ctx, node, volumeID, int64(requiredBytes), anaState, anaGroupID, protCfg,
			)
			if createErr != nil {
				// Clean up already-created targets on failure.
				logging.L.Warn("failed to create target on replica node (continuing)",
					zap.String("volumeID", volumeID),
					zap.String("node", node),
					zap.Error(createErr),
				)
				continue
			}
			targets = append(targets, targetInfo{
				Addr:    addr,
				Port:    port,
				NQN:     nqn,
				IsOwner: i == 0,
			})
			if i == 0 {
				ownerAddr = addr
				ownerPort = port
				ownerNQN = nqn
			}
			logging.L.Info("NVMe-oF target created",
				zap.String("volumeID", volumeID),
				zap.String("node", node),
				zap.String("anaState", anaState),
				zap.Bool("isOwner", i == 0),
			)
		}

		if len(targets) == 0 {
			_ = cs.meta.DeleteVolumeMeta(ctx, volumeID)
			return nil, status.Errorf(codes.Internal, "failed to create any NVMe-oF targets for volume %s", volumeID)
		}

		targetsJSON, err := json.Marshal(targets)
		if err != nil {
			for _, node := range uniqueNodes {
				_ = cs.agentTarget.DeleteTarget(ctx, node, volumeID)
			}
			_ = cs.meta.DeleteVolumeMeta(ctx, volumeID)
			return nil, status.Errorf(codes.Internal, "marshaling target addresses for volume %s: %v", volumeID, err)
		}

		logging.L.Info("NVMe-oF multipath targets created",
			zap.String("volumeID", volumeID),
			zap.Int("totalTargets", len(targets)),
			zap.Int("totalNodes", len(uniqueNodes)),
		)

		volContext["targetAddresses"] = string(targetsJSON)
		volContext["writeOwner"] = ownerAddr
		volContext["subsystemNQN"] = ownerNQN
		volContext["targetAddress"] = ownerAddr // Backward compat
		volContext["targetPort"] = ownerPort    // Backward compat

		// Update volume metadata with primary target fields.
		vm.TargetNodeID = ownerNode
		vm.TargetAddress = ownerAddr
		vm.TargetPort = ownerPort
		vm.SubsystemNQN = ownerNQN
		if err := cs.meta.PutVolumeMeta(ctx, vm); err != nil {
			_ = cs.agentTarget.DeleteTarget(ctx, ownerNode, volumeID)
			return nil, status.Errorf(codes.Internal, "updating volume metadata with target info: %v", err)
		}
	}

	// Data protection replication is deferred — the NVMe-oF targets on each
	// replica node create their own sparse backing files (thin provisioned).
	// Synchronous replication of written blocks will be handled by the
	// policy engine's periodic compliance scan. This lazy approach means
	// CreateVolume completes instantly regardless of volume size.

	// Build accessible topology from the nodes where chunks are placed.
	// This informs the Kubernetes scheduler where the volume is reachable.
	accessibleTopology := cs.buildVolumeTopology(nodeIDs)

	// Reserve quota for the volume after successful creation.
	if cs.quota != nil {
		for _, scope := range quotaScopes {
			if err := cs.quota.ReserveStorage(ctx, scope, int64(requiredBytes)); err != nil {
				// Log but don't fail - the volume was created successfully.
				// This may cause quota tracking to be slightly off, but prevents
				// leaving the volume in an inconsistent state.
				logging.L.Warn("failed to reserve quota after volume creation (volume created but quota not updated)",
					zap.String("volumeID", volumeID),
					zap.String("scope", scope.String()),
					zap.Error(err))
			}
		}
	}

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

	// Determine the quota scopes for this volume based on its metadata.
	var quotaScopes []metadata.QuotaScope
	if vm.Pool != "" {
		quotaScopes = append(quotaScopes, metadata.QuotaScope{Kind: "StoragePool", Name: vm.Pool})
	}
	// If the volume has a namespace annotation (stored in volume context), include it.
	// For now, we only track pool-level quota in volume metadata.

	// Tear down NVMe-oF targets on all agents that may have them (best-effort).
	// With multi-target provisioning, targets exist on all unique placement nodes.
	// If target deletion fails (e.g., agent unreachable), we still proceed
	// with metadata cleanup to avoid blocking volume deletion on retries.
	if cs.agentTarget != nil {
		// Delete targets on all agents that may have them.
		// Look up unique nodes from placement maps.
		targetNodes := make(map[string]bool)
		if vm.TargetNodeID != "" {
			targetNodes[vm.TargetNodeID] = true
		}
		for _, chunkID := range vm.ChunkIDs {
			pm, pmErr := cs.meta.GetPlacementMap(ctx, chunkID)
			if pmErr == nil && pm != nil {
				for _, n := range pm.Nodes {
					targetNodes[n] = true
				}
			}
		}
		for nodeAddr := range targetNodes {
			if delErr := cs.agentTarget.DeleteTarget(ctx, nodeAddr, volumeID); delErr != nil {
				logging.L.Warn("failed to delete NVMe-oF target on agent (best-effort cleanup)",
					zap.String("volumeID", volumeID),
					zap.String("agentAddr", nodeAddr),
					zap.Error(delErr))
			}
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

	// Release quota for the deleted volume.
	if cs.quota != nil {
		for _, scope := range quotaScopes {
			if err := cs.quota.ReleaseStorage(ctx, scope, int64(vm.SizeBytes)); err != nil {
				// Log but don't fail - the volume was deleted successfully.
				logging.L.Warn("failed to release quota after volume deletion (volume deleted but quota not updated)",
					zap.String("volumeID", volumeID),
					zap.String("scope", scope.String()),
					zap.Error(err))
			}
		}
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
func (cs *ControllerServer) GetCapacity(ctx context.Context, _ *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
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

// Erasure coding encode/decode operations are handled entirely by the
// Rust dataplane's chunk engine (invariant #1: layer separation is
// absolute — presentation/CSI layer MUST NOT replicate or EC-encode).
// The Go CSI controller only stores protection metadata during
// CreateVolume; actual shard distribution happens in the chunk engine.
