// Package failover implements a controller that watches metadata for volume
// ownership changes and manages NVMe-oF ANA (Asymmetric Namespace Access)
// state transitions. When ownership moves to this node the controller promotes
// the local NVMe-oF target to "optimized"; when ownership moves away it demotes
// the target to "non_optimized".
package failover

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/metadata"
)

const (
	// watchInterval is the polling interval for ownership checks.
	watchInterval = 500 * time.Millisecond

	// nqnPrefix is prepended to the volumeID to form the subsystem NQN.
	// Must match spdkNQNPrefix in internal/agent/spdk_target_server.go.
	nqnPrefix = "novastor-"

	// ANAOptimized is the ANA state for the active/preferred path.
	ANAOptimized = "optimized"

	// ANANonOptimized is the ANA state for the standby path.
	ANANonOptimized = "non_optimized"
)

// MetadataClient is the interface for metadata operations needed by the failover controller.
type MetadataClient interface {
	GetVolumeOwner(ctx context.Context, volumeID string) (*metadata.VolumeOwnership, error)
	RequestOwnership(ctx context.Context, volumeID, requesterAddr string) (bool, uint64, error)
}

// SPDKClient is the interface for SPDK data-plane operations needed by the failover controller.
type SPDKClient interface {
	SetANAState(nqn string, anaGroupID uint32, state string) error
}

// VolumeState tracks the failover state of a single volume on this agent.
type VolumeState struct {
	VolumeID     string
	IsOwner      bool
	OwnerAddr    string
	Generation   uint64
	ANAState     string
	ReplicaNodes []string
	ReplicaBdev  string
}

// Controller watches metadata for volume ownership changes and drives
// ANA state transitions on the local SPDK data-plane.
type Controller struct {
	nodeID   string
	nodeAddr string
	hostIP   string

	metaClient MetadataClient
	spdkClient SPDKClient
	logger     *zap.Logger

	mu      sync.RWMutex
	volumes map[string]*VolumeState // keyed by volume ID

	stopCh chan struct{}
	done   chan struct{}
}

// New creates a new failover Controller.
func New(nodeID, nodeAddr, hostIP string, metaClient MetadataClient, spdkClient SPDKClient, logger *zap.Logger) *Controller {
	return &Controller{
		nodeID:     nodeID,
		nodeAddr:   nodeAddr,
		hostIP:     hostIP,
		metaClient: metaClient,
		spdkClient: spdkClient,
		logger:     logger.Named("failover"),
		volumes:    make(map[string]*VolumeState),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Start initialises ANA states for currently managed volumes and begins the
// ownership watch loop.
func (c *Controller) Start(ctx context.Context) error {
	c.logger.Info("starting failover controller",
		zap.String("nodeID", c.nodeID),
		zap.String("nodeAddr", c.nodeAddr),
	)

	// Set initial ANA states for any volumes already registered.
	c.mu.RLock()
	vols := make([]*VolumeState, 0, len(c.volumes))
	for _, vs := range c.volumes {
		vols = append(vols, vs)
	}
	c.mu.RUnlock()

	for _, vs := range vols {
		if err := c.syncInitialState(ctx, vs); err != nil {
			c.logger.Warn("failed to sync initial ANA state",
				zap.String("volumeID", vs.VolumeID),
				zap.Error(err),
			)
		}
	}

	go c.watchLoop(ctx)
	return nil
}

// Stop signals the watch loop to exit and waits for it to finish.
func (c *Controller) Stop() {
	close(c.stopCh)
	<-c.done
	c.logger.Info("failover controller stopped")
}

// RegisterVolume adds a volume to the managed set. It should be called when
// the agent discovers or creates a replica for a volume.
func (c *Controller) RegisterVolume(volumeID string, replicaNodes []string, isOwner bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := ANANonOptimized
	if isOwner {
		state = ANAOptimized
	}

	c.volumes[volumeID] = &VolumeState{
		VolumeID:     volumeID,
		IsOwner:      isOwner,
		ANAState:     state,
		ReplicaNodes: replicaNodes,
	}

	c.logger.Info("volume registered with failover controller",
		zap.String("volumeID", volumeID),
		zap.Bool("isOwner", isOwner),
		zap.Strings("replicaNodes", replicaNodes),
	)
}

// UnregisterVolume removes a volume from the managed set.
func (c *Controller) UnregisterVolume(volumeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.volumes, volumeID)
	c.logger.Info("volume unregistered from failover controller",
		zap.String("volumeID", volumeID),
	)
}

// watchLoop polls metadata at a fixed interval and checks for ownership changes.
func (c *Controller) watchLoop(ctx context.Context) {
	defer close(c.done)

	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkOwnership(ctx)
		}
	}
}

// checkOwnership queries metadata for each managed volume and triggers
// promote/demote transitions when ownership changes.
func (c *Controller) checkOwnership(ctx context.Context) {
	c.mu.RLock()
	vols := make([]*VolumeState, 0, len(c.volumes))
	for _, vs := range c.volumes {
		vols = append(vols, vs)
	}
	c.mu.RUnlock()

	for _, vs := range vols {
		ownership, err := c.metaClient.GetVolumeOwner(ctx, vs.VolumeID)
		if err != nil {
			c.logger.Warn("failed to get volume owner",
				zap.String("volumeID", vs.VolumeID),
				zap.Error(err),
			)
			continue
		}
		if ownership == nil {
			continue
		}

		ownerIsUs := ownership.OwnerAddr == c.nodeAddr

		if ownerIsUs && !vs.IsOwner {
			// Ownership moved to us: promote.
			c.promote(ctx, vs, ownership.Generation)
		} else if !ownerIsUs && vs.IsOwner {
			// Ownership moved away from us: demote.
			c.demote(ctx, vs)
		}

		// Update tracked owner address regardless.
		c.mu.Lock()
		if v, ok := c.volumes[vs.VolumeID]; ok {
			v.OwnerAddr = ownership.OwnerAddr
			v.Generation = ownership.Generation
		}
		c.mu.Unlock()
	}
}

// promote transitions a volume's local NVMe-oF target to the optimized ANA state.
func (c *Controller) promote(ctx context.Context, vs *VolumeState, generation uint64) {
	nqn := nqnPrefix + vs.VolumeID
	anaGroupID := anaGroupForVolume(vs.VolumeID)

	c.logger.Info("promoting volume to optimized",
		zap.String("volumeID", vs.VolumeID),
		zap.Uint64("generation", generation),
		zap.Uint32("anaGroupID", anaGroupID),
	)

	if err := c.spdkClient.SetANAState(nqn, anaGroupID, ANAOptimized); err != nil {
		c.logger.Error("failed to set ANA state to optimized",
			zap.String("volumeID", vs.VolumeID),
			zap.Error(err),
		)
		return
	}

	c.mu.Lock()
	if v, ok := c.volumes[vs.VolumeID]; ok {
		v.IsOwner = true
		v.Generation = generation
		v.ANAState = ANAOptimized
	}
	c.mu.Unlock()
}

// demote transitions a volume's local NVMe-oF target to the non-optimized ANA state.
func (c *Controller) demote(ctx context.Context, vs *VolumeState) {
	nqn := nqnPrefix + vs.VolumeID
	anaGroupID := anaGroupForVolume(vs.VolumeID)

	c.logger.Info("demoting volume to non_optimized",
		zap.String("volumeID", vs.VolumeID),
		zap.Uint32("anaGroupID", anaGroupID),
	)

	if err := c.spdkClient.SetANAState(nqn, anaGroupID, ANANonOptimized); err != nil {
		c.logger.Error("failed to set ANA state to non_optimized",
			zap.String("volumeID", vs.VolumeID),
			zap.Error(err),
		)
		return
	}

	c.mu.Lock()
	if v, ok := c.volumes[vs.VolumeID]; ok {
		v.IsOwner = false
		v.ANAState = ANANonOptimized
	}
	c.mu.Unlock()
}

// syncInitialState queries metadata for ownership and sets the ANA state
// accordingly. Called once at Start for volumes that were registered before
// the controller started.
func (c *Controller) syncInitialState(ctx context.Context, vs *VolumeState) error {
	ownership, err := c.metaClient.GetVolumeOwner(ctx, vs.VolumeID)
	if err != nil {
		return fmt.Errorf("get volume owner: %w", err)
	}
	if ownership == nil {
		return nil
	}

	nqn := nqnPrefix + vs.VolumeID
	anaGroupID := anaGroupForVolume(vs.VolumeID)
	ownerIsUs := ownership.OwnerAddr == c.nodeAddr

	var targetState string
	if ownerIsUs {
		targetState = ANAOptimized
	} else {
		targetState = ANANonOptimized
	}

	if err := c.spdkClient.SetANAState(nqn, anaGroupID, targetState); err != nil {
		return fmt.Errorf("set ANA state to %s: %w", targetState, err)
	}

	c.mu.Lock()
	if v, ok := c.volumes[vs.VolumeID]; ok {
		v.IsOwner = ownerIsUs
		v.OwnerAddr = ownership.OwnerAddr
		v.Generation = ownership.Generation
		v.ANAState = targetState
	}
	c.mu.Unlock()

	c.logger.Info("initial ANA state set",
		zap.String("volumeID", vs.VolumeID),
		zap.String("state", targetState),
		zap.String("owner", ownership.OwnerAddr),
	)
	return nil
}

// anaGroupForVolume computes a deterministic ANA group ID for a volume.
func anaGroupForVolume(volumeID string) uint32 {
	return crc32.ChecksumIEEE([]byte(volumeID)) & 0xFF
}
