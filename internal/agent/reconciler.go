package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	pb "github.com/azrtydxb/novastor/api/proto/nvme"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
)

const (
	// reconcileInterval is the default period between reconciliation runs.
	reconcileInterval = 60 * time.Second

	// reconcileTimeout is the maximum duration for a single reconcile pass.
	reconcileTimeout = 30 * time.Second
)

// MetadataQuerier abstracts the metadata operations needed by the reconciler.
// This interface is intentionally narrow for testability — it only includes
// the method(s) required for volume reconciliation.
type MetadataQuerier interface {
	// ListVolumesMeta returns all volume metadata entries from the metadata service.
	ListVolumesMeta(ctx context.Context) ([]*metadata.VolumeMeta, error)
}

// VolumeReconciler ensures that NVMe-oF targets on this node match the
// desired state stored in the metadata service. After a dataplane restart,
// targets are lost; the reconciler detects missing targets and re-creates
// them, and removes orphan targets that no longer have metadata entries.
type VolumeReconciler struct {
	nodeID     string
	targetSrv  *SPDKTargetServer
	metaClient MetadataQuerier
	triggerCh  chan struct{}

	// reconciling guards against concurrent reconcile runs.
	reconciling sync.Mutex
}

// NewVolumeReconciler creates a new VolumeReconciler that monitors and repairs
// NVMe-oF targets on the specified node. The metaClient is used to query the
// desired volume state, and targetSrv is the local SPDK target server that
// manages actual NVMe-oF targets.
func NewVolumeReconciler(nodeID string, targetSrv *SPDKTargetServer, metaClient MetadataQuerier) *VolumeReconciler {
	return &VolumeReconciler{
		nodeID:     nodeID,
		targetSrv:  targetSrv,
		metaClient: metaClient,
		triggerCh:  make(chan struct{}, 1),
	}
}

// Trigger requests an immediate reconciliation pass. Non-blocking: if a
// trigger is already pending it is a no-op. Useful when the dataplane
// reconnects or an external event indicates state may be inconsistent.
func (r *VolumeReconciler) Trigger() {
	select {
	case r.triggerCh <- struct{}{}:
		logging.L.Debug("reconciler: triggered")
	default:
		// A trigger is already pending; skip.
	}
}

// Run starts the reconciliation loop. It runs an initial reconcile on startup,
// then periodically every reconcileInterval and on every trigger. Run blocks
// until ctx is cancelled.
func (r *VolumeReconciler) Run(ctx context.Context) {
	logging.L.Info("reconciler: starting volume reconciliation loop",
		zap.String("nodeID", r.nodeID),
		zap.Duration("interval", reconcileInterval),
	)

	// Initial reconcile on startup.
	r.runReconcile(ctx)

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logging.L.Info("reconciler: stopped")
			return
		case <-ticker.C:
			r.runReconcile(ctx)
		case <-r.triggerCh:
			r.runReconcile(ctx)
		}
	}
}

// runReconcile executes a single reconcile pass with a timeout. It acquires
// the reconciling mutex to prevent overlapping reconcile runs.
func (r *VolumeReconciler) runReconcile(ctx context.Context) {
	if !r.reconciling.TryLock() {
		logging.L.Debug("reconciler: skipping — another reconcile is in progress")
		return
	}
	defer r.reconciling.Unlock()

	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()

	if err := r.reconcile(rctx); err != nil {
		logging.L.Error("reconciler: reconcile failed", zap.Error(err))
	}
}

// reconcile is the core reconciliation logic. It compares desired state
// (metadata) against actual state (active targets) and creates or deletes
// targets to converge.
func (r *VolumeReconciler) reconcile(ctx context.Context) error {
	// Step 1: Query metadata for all volumes placed on this node.
	allVolumes, err := r.metaClient.ListVolumesMeta(ctx)
	if err != nil {
		return fmt.Errorf("listing volume metadata: %w", err)
	}

	// Filter volumes assigned to this node.
	desired := make(map[string]*metadata.VolumeMeta)
	for _, vol := range allVolumes {
		if vol.TargetNodeID == r.nodeID {
			desired[vol.VolumeID] = vol
		}
	}

	// Step 2: Query active targets on this node.
	listResp, err := r.targetSrv.ListTargets(ctx, &pb.ListTargetsRequest{})
	if err != nil {
		return fmt.Errorf("listing active targets: %w", err)
	}

	active := make(map[string]*pb.TargetEntry)
	for _, entry := range listResp.GetTargets() {
		active[entry.GetVolumeId()] = entry
	}

	logging.L.Info("reconciler: state comparison",
		zap.Int("desired", len(desired)),
		zap.Int("active", len(active)),
	)

	// Step 3: Create missing targets (in metadata but not active).
	var createErrors int
	for volID, vol := range desired {
		if _, exists := active[volID]; exists {
			continue
		}

		logging.L.Info("reconciler: creating missing target",
			zap.String("volumeID", volID),
			zap.Uint64("sizeBytes", vol.SizeBytes),
		)

		req := r.buildCreateTargetRequest(vol)
		if _, err := r.targetSrv.CreateTarget(ctx, req); err != nil {
			logging.L.Error("reconciler: failed to create target",
				zap.String("volumeID", volID),
				zap.Error(err),
			)
			createErrors++
			continue
		}

		logging.L.Info("reconciler: target re-created successfully",
			zap.String("volumeID", volID),
		)
	}

	// Step 4: Delete orphan targets (active but not in metadata).
	// Only delete orphans if we got a non-empty desired list from metadata.
	// If desired is empty, metadata might be unavailable or the query might
	// not match (e.g., IP vs hostname mismatch) — don't nuke valid targets.
	var deleteErrors int
	if len(desired) == 0 {
		if len(active) > 0 {
			logging.L.Warn("reconciler: skipping orphan cleanup — metadata returned 0 desired volumes but agent has active targets",
				zap.Int("active", len(active)))
		}
	}
	for volID := range active {
		if _, exists := desired[volID]; exists {
			continue
		}
		if len(desired) == 0 {
			continue // Don't delete when we can't verify against metadata.
		}

		logging.L.Info("reconciler: deleting orphan target",
			zap.String("volumeID", volID),
		)

		delReq := &pb.DeleteTargetRequest{VolumeId: volID}
		if _, err := r.targetSrv.DeleteTarget(ctx, delReq); err != nil {
			logging.L.Error("reconciler: failed to delete orphan target",
				zap.String("volumeID", volID),
				zap.Error(err),
			)
			deleteErrors++
			continue
		}

		logging.L.Info("reconciler: orphan target deleted",
			zap.String("volumeID", volID),
		)
	}

	if createErrors > 0 || deleteErrors > 0 {
		return fmt.Errorf("reconcile completed with errors: %d create failures, %d delete failures",
			createErrors, deleteErrors)
	}

	return nil
}

// buildCreateTargetRequest constructs a CreateTargetRequest from volume metadata.
func (r *VolumeReconciler) buildCreateTargetRequest(vol *metadata.VolumeMeta) *pb.CreateTargetRequest {
	req := &pb.CreateTargetRequest{
		VolumeId:  vol.VolumeID,
		SizeBytes: int64(vol.SizeBytes),
	}

	// Map volume protection profile to the proto VolumeProtection message.
	if pp := vol.ProtectionProfile; pp != nil {
		prot := &pb.VolumeProtection{
			Mode: string(pp.Mode),
		}
		if pp.Replication != nil {
			prot.ReplicationFactor = uint32(pp.Replication.Factor)
		}
		if pp.ErasureCoding != nil {
			prot.DataShards = uint32(pp.ErasureCoding.DataShards)
			prot.ParityShards = uint32(pp.ErasureCoding.ParityShards)
		}
		req.Protection = prot
	} else if vol.DataProtection != nil {
		// Fall back to legacy DataProtection field if ProtectionProfile is nil.
		dp := vol.DataProtection
		prot := &pb.VolumeProtection{
			Mode: string(dp.Mode),
		}
		if dp.Replication != nil {
			prot.ReplicationFactor = uint32(dp.Replication.Factor)
		}
		if dp.ErasureCoding != nil {
			prot.DataShards = uint32(dp.ErasureCoding.DataShards)
			prot.ParityShards = uint32(dp.ErasureCoding.ParityShards)
		}
		req.Protection = prot
	}

	return req
}
