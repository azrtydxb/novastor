package policy

import (
	"context"
	"fmt"
	"time"

	"github.com/piwi3910/novastor/api/v1alpha1"
	"github.com/piwi3910/novastor/internal/logging"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
)

// PolicyEngineRunnable wraps the policy engine and reconciler as a controller-runtime Runnable.
type PolicyEngineRunnable struct {
	engine        *PolicyEngine
	reconciler    *Reconciler
	metrics       *MetricsReporter
	scanInterval  time.Duration
	repairEnabled bool
	eventRecorder events.EventRecorder
}

// NewPolicyEngineRunnable creates a new PolicyEngineRunnable.
func NewPolicyEngineRunnable(
	metaClient MetadataClient,
	poolLookup PoolLookup,
	nodeChecker NodeAvailabilityChecker,
	chunkReplicator ChunkReplicator,
	shardReplicator ShardReplicator,
	eventRecorder events.EventRecorder,
	scanInterval time.Duration,
	repairEnabled bool,
) *PolicyEngineRunnable {
	engine := NewPolicyEngine(metaClient, nodeChecker)
	reconciler := NewReconciler(metaClient, nodeChecker, poolLookup)
	reconciler.SetChunkReplicator(chunkReplicator)
	reconciler.SetShardReplicator(shardReplicator)
	reconciler.SetEventRecorder(eventRecorder)

	return &PolicyEngineRunnable{
		engine:        engine,
		reconciler:    reconciler,
		metrics:       NewMetricsReporter(),
		scanInterval:  scanInterval,
		repairEnabled: repairEnabled,
		eventRecorder: eventRecorder,
	}
}

// Start implements the manager.Runnable interface.
func (r *PolicyEngineRunnable) Start(ctx context.Context) error {
	logger := logging.L.Named("policy-engine")

	ticker := time.NewTicker(r.scanInterval)
	defer ticker.Stop()

	logger.Info("policy engine started",
		zap.Duration("scanInterval", r.scanInterval),
		zap.Bool("repairEnabled", r.repairEnabled),
	)

	// Run initial scan immediately
	r.runCycle(ctx, logger)

	for {
		select {
		case <-ctx.Done():
			logger.Info("policy engine stopping")
			return nil
		case <-ticker.C:
			r.runCycle(ctx, logger)
		}
	}
}

// runCycle performs one full cycle: scan for compliance issues and optionally repair.
func (r *PolicyEngineRunnable) runCycle(ctx context.Context, logger *zap.Logger) {
	// Phase 1: Compliance scan
	scanStart := r.metrics.ReportScanStart("full")

	reports, err := r.engine.CheckAllPoolsCompliance(ctx, r.reconciler.getPoolLookup())
	if err != nil {
		logger.Error("compliance scan failed", zap.Error(err))
		return
	}

	r.metrics.ReportScanComplete("full", scanStart)

	// Phase 2: Report metrics
	for poolName, report := range reports {
		r.metrics.ReportPoolCompliance(poolName, report.ProtectionMode, report.IsCompliant)
		r.metrics.ReportChunkViolations(
			poolName,
			report.CompliantChunks,
			report.UnderReplicatedChunks,
			report.UnavailableChunks,
			report.CorruptedChunks,
		)

		// Record events for non-compliant pools
		if !report.IsCompliant {
			logger.Warn("pool not compliant",
				zap.String("pool", poolName),
				zap.Int("underReplicated", report.UnderReplicatedChunks),
				zap.Int("unavailable", report.UnavailableChunks),
				zap.Int("corrupted", report.CorruptedChunks),
			)
			r.emitComplianceEvent(poolName, report)
		}
	}

	// Phase 3: Enqueue repair tasks
	summary, err := r.reconciler.ScanAndEnqueue(ctx)
	if err != nil {
		logger.Error("failed to enqueue repair tasks", zap.Error(err))
		return
	}

	r.metrics.ReportRepairQueued(r.reconciler.QueueSize())

	logger.Info("compliance scan completed",
		zap.Int("pools", len(reports)),
		zap.Duration("duration", summary.Duration),
		zap.Int("tasksQueued", summary.TasksQueued),
		zap.Int("queueSize", r.reconciler.QueueSize()),
	)

	// Phase 4: Process repair queue
	if r.repairEnabled && r.reconciler.QueueSize() > 0 {
		repairStart := time.Now()

		if err := r.reconciler.ProcessQueue(ctx); err != nil {
			logger.Error("repair queue processing failed", zap.Error(err))
		}

		repairDuration := time.Since(repairStart)
		logger.Info("repair cycle completed",
			zap.Duration("duration", repairDuration),
			zap.Int64("completed", r.reconciler.CompletedRepairs()),
			zap.Int64("failed", r.reconciler.FailedRepairs()),
		)
	}
}

// Engine returns the underlying PolicyEngine for external queries.
func (r *PolicyEngineRunnable) Engine() *PolicyEngine {
	return r.engine
}

// Reconciler returns the underlying Reconciler for status queries.
func (r *PolicyEngineRunnable) Reconciler() *Reconciler {
	return r.reconciler
}

// TriggerScan forces an immediate compliance scan and repair cycle.
// This is useful for on-demand checks triggered by external events.
func (r *PolicyEngineRunnable) TriggerScan(ctx context.Context) error {
	logger := logging.L.Named("policy-engine")
	logger.Info("triggering manual compliance scan")
	r.runCycle(ctx, logger)
	return nil
}

// PoolComplianceStatus returns the current compliance status for a specific pool.
func (r *PolicyEngineRunnable) PoolComplianceStatus(ctx context.Context, poolName string) (*PoolComplianceReport, error) {
	pool, err := r.reconciler.getPoolLookup().GetPool(ctx, poolName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("pool %s not found", poolName)
		}
		return nil, err
	}

	return r.engine.CheckPoolCompliance(ctx, pool)
}

// VolumeComplianceStatus returns the current compliance status for a specific volume.
func (r *PolicyEngineRunnable) VolumeComplianceStatus(ctx context.Context, volumeID string, pool *v1alpha1.StoragePool) (*VolumeComplianceReport, error) {
	volume, err := r.engine.metaClient.GetVolumeMeta(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("getting volume metadata: %w", err)
	}

	return r.engine.CheckVolumeCompliance(ctx, volume, pool)
}

// RepairStatus returns the current status of the repair queue.
func (r *PolicyEngineRunnable) RepairStatus() (queueSize int, completed int64, failed int64, lastScan time.Time) {
	return r.reconciler.QueueSize(),
		r.reconciler.CompletedRepairs(),
		r.reconciler.FailedRepairs(),
		r.reconciler.lastScanTime
}

// emitComplianceEvent emits a Kubernetes event for a non-compliant pool.
func (r *PolicyEngineRunnable) emitComplianceEvent(poolName string, report *PoolComplianceReport) {
	if r.eventRecorder == nil {
		return
	}

	pool := &v1alpha1.StoragePool{}
	pool.Name = poolName
	pool.Namespace = "novastor-system" // Default namespace, adjust as needed

	message := fmt.Sprintf("Pool compliance check failed: %d under-replicated, %d unavailable, %d corrupted chunks",
		report.UnderReplicatedChunks, report.UnavailableChunks, report.CorruptedChunks)

	r.eventRecorder.Eventf(pool, nil, corev1.EventTypeWarning, "ComplianceCheckFailed", "CheckCompliance", message)
}
