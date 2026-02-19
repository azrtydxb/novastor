package policy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/piwi3910/novastor/api/v1alpha1"
	"github.com/piwi3910/novastor/internal/logging"
)

// Reconciler repairs non-compliant chunks to restore data protection.
type Reconciler struct {
	metaClient      MetadataClient
	nodeChecker     NodeAvailabilityChecker
	poolLookup      PoolLookup
	chunkReplicator ChunkReplicator
	shardReplicator ShardReplicator

	mu               sync.Mutex
	maxConcurrent    int
	repairQueue      []RepairTask
	completedRepairs int64
	failedRepairs    int64
	lastScanTime     time.Time
	lastScanDuration time.Duration
}

// RepairTask represents a chunk that needs repair.
type RepairTask struct {
	ChunkID        string           `json:"chunkID"`
	VolumeID       string           `json:"volumeID"`
	Pool           string           `json:"pool"`
	ProtectionMode string           `json:"protectionMode"`
	Status         ComplianceStatus `json:"status"`
	AvailableNodes []string         `json:"availableNodes"`
	FailedNodes    []string         `json:"failedNodes"`
	Priority       int              `json:"priority"` // Lower is more urgent
	CreatedAt      time.Time        `json:"createdAt"`
}

// RepairSummary contains statistics about a repair operation.
type RepairSummary struct {
	TasksQueued    int           `json:"tasksQueued"`
	TasksCompleted int           `json:"tasksCompleted"`
	TasksFailed    int           `json:"tasksFailed"`
	Duration       time.Duration `json:"duration"`
}

// NewReconciler creates a new Reconciler.
func NewReconciler(metaClient MetadataClient, nodeChecker NodeAvailabilityChecker, poolLookup PoolLookup) *Reconciler {
	return &Reconciler{
		metaClient:    metaClient,
		nodeChecker:   nodeChecker,
		poolLookup:    poolLookup,
		maxConcurrent: 4,
		repairQueue:   make([]RepairTask, 0),
	}
}

// SetChunkReplicator sets the chunk replicator for repairing replicated chunks.
func (r *Reconciler) SetChunkReplicator(replicator ChunkReplicator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunkReplicator = replicator
}

// SetShardReplicator sets the shard replicator for repairing erasure-coded chunks.
func (r *Reconciler) SetShardReplicator(replicator ShardReplicator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shardReplicator = replicator
}

// SetMaxConcurrent sets the maximum number of concurrent repair operations.
func (r *Reconciler) SetMaxConcurrent(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > 0 {
		r.maxConcurrent = n
	}
}

// ScanAndEnqueue scans all pools for non-compliant chunks and enqueues repair tasks.
func (r *Reconciler) ScanAndEnqueue(ctx context.Context) (*RepairSummary, error) {
	startTime := time.Now()

	engine := NewPolicyEngine(r.metaClient, r.nodeChecker)
	reports, err := engine.CheckAllPoolsCompliance(ctx, r.poolLookup)
	if err != nil {
		return nil, fmt.Errorf("checking compliance: %w", err)
	}

	queuedCount := 0
	for poolName, report := range reports {
		if report.IsCompliant {
			continue
		}

		for _, volumeReport := range report.VolumeReports {
			for _, chunkResult := range volumeReport.ChunkResults {
				if chunkResult.Status == StatusCompliant {
					continue
				}

				// Calculate priority: fewer available replicas = higher priority
				priority := len(chunkResult.AvailableNodes)
				if chunkResult.Status == StatusCorrupted {
					priority = -1 // Highest priority for corrupted chunks
				}

				task := RepairTask{
					ChunkID:        chunkResult.ChunkID,
					VolumeID:       chunkResult.VolumeID,
					Pool:           poolName,
					ProtectionMode: chunkResult.ProtectionMode,
					Status:         chunkResult.Status,
					AvailableNodes: chunkResult.AvailableNodes,
					FailedNodes:    chunkResult.FailedNodes,
					Priority:       priority,
					CreatedAt:      time.Now(),
				}

				r.mu.Lock()
				r.repairQueue = append(r.repairQueue, task)
				r.mu.Unlock()

				queuedCount++
			}
		}
	}

	// Sort queue by priority (lower first)
	r.sortQueue()

	scanDuration := time.Since(startTime)
	r.mu.Lock()
	r.lastScanTime = startTime
	r.lastScanDuration = scanDuration
	r.mu.Unlock()

	logging.L.Info("compliance scan completed",
		zap.Int("tasksQueued", queuedCount),
		zap.Duration("scanDuration", scanDuration),
	)

	return &RepairSummary{
		TasksQueued: queuedCount,
		Duration:    scanDuration,
	}, nil
}

// ProcessQueue processes pending repair tasks with a concurrency limit.
func (r *Reconciler) ProcessQueue(ctx context.Context) error {
	r.mu.Lock()
	tasks := make([]RepairTask, len(r.repairQueue))
	copy(tasks, r.repairQueue)
	r.repairQueue = make([]RepairTask, 0)
	r.mu.Unlock()

	if len(tasks) == 0 {
		return nil
	}

	logging.L.Info("processing repair queue",
		zap.Int("tasks", len(tasks)),
		zap.Int("concurrency", r.maxConcurrent),
	)

	sem := make(chan struct{}, r.maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var completed, failed int

	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(t RepairTask) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := r.repairTask(ctx, t); err != nil {
				logging.L.Error("failed to repair chunk",
					zap.String("chunkID", t.ChunkID),
					zap.String("pool", t.Pool),
					zap.Error(err),
				)
				mu.Lock()
				failed++
				r.failedRepairs++
				mu.Unlock()
				return
			}

			mu.Lock()
			completed++
			r.completedRepairs++
			mu.Unlock()

			logging.L.Debug("repaired chunk",
				zap.String("chunkID", t.ChunkID),
				zap.String("pool", t.Pool),
				zap.String("mode", t.ProtectionMode),
			)
		}(task)
	}

	wg.Wait()

	logging.L.Info("repair queue processing completed",
		zap.Int("completed", completed),
		zap.Int("failed", failed),
	)

	return nil
}

// repairTask performs a single repair operation.
func (r *Reconciler) repairTask(ctx context.Context, task RepairTask) error {
	pool, err := r.poolLookup.GetPool(ctx, task.Pool)
	if err != nil {
		return fmt.Errorf("getting pool %s: %w", task.Pool, err)
	}

	// Get healthy nodes for potential repair targets
	nodes, err := r.metaClient.ListNodeMetas(ctx)
	if err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	var healthyNodes []string
	for _, node := range nodes {
		if r.nodeChecker.IsNodeAvailable(ctx, node.NodeID) {
			// Skip nodes that already have this chunk
			alreadyHas := false
			for _, n := range task.AvailableNodes {
				if n == node.NodeID {
					alreadyHas = true
					break
				}
			}
			if !alreadyHas {
				healthyNodes = append(healthyNodes, node.NodeID)
			}
		}
	}

	if len(healthyNodes) == 0 {
		return fmt.Errorf("no healthy nodes available for repair")
	}

	switch task.ProtectionMode {
	case "replication":
		return r.repairReplicatedChunk(ctx, task, pool, healthyNodes)
	case "erasureCoding":
		return r.repairErasureChunk(ctx, task, pool, healthyNodes)
	default:
		return fmt.Errorf("unknown protection mode: %s", task.ProtectionMode)
	}
}

// repairReplicatedChunk replicates a chunk to a new node.
func (r *Reconciler) repairReplicatedChunk(ctx context.Context, task RepairTask, pool *v1alpha1.StoragePool, healthyNodes []string) error {
	if len(task.AvailableNodes) == 0 {
		return fmt.Errorf("no available replicas for chunk %s", task.ChunkID)
	}

	if r.chunkReplicator == nil {
		return fmt.Errorf("chunk replicator not configured")
	}

	// Pick first available node as source
	sourceNode := task.AvailableNodes[0]
	destNode := healthyNodes[0]

	if err := r.chunkReplicator.ReplicateChunk(ctx, task.ChunkID, sourceNode, destNode); err != nil {
		return fmt.Errorf("replicating chunk: %w", err)
	}

	// Update placement map
	placement, err := r.metaClient.GetPlacementMap(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("getting placement map: %w", err)
	}

	// Replace a failed node with the new node
	updated := false
	for i, node := range placement.Nodes {
		isFailed := false
		for _, failed := range task.FailedNodes {
			if node == failed {
				isFailed = true
				break
			}
		}
		if isFailed && !updated {
			placement.Nodes[i] = destNode
			updated = true
			break
		}
	}

	if !updated {
		// Add the new node if we didn't replace a failed one
		placement.Nodes = append(placement.Nodes, destNode)
	}

	if err := r.metaClient.PutPlacementMap(ctx, placement); err != nil {
		return fmt.Errorf("updating placement map: %w", err)
	}

	return nil
}

// repairErasureChunk regenerates a lost shard.
func (r *Reconciler) repairErasureChunk(ctx context.Context, task RepairTask, pool *v1alpha1.StoragePool, healthyNodes []string) error {
	if len(task.AvailableNodes) == 0 {
		return fmt.Errorf("no available shards for chunk %s", task.ChunkID)
	}

	if r.shardReplicator == nil {
		return fmt.Errorf("shard replicator not configured")
	}

	ecSpec := pool.Spec.DataProtection.ErasureCoding
	if ecSpec == nil {
		return fmt.Errorf("pool has nil erasure coding spec")
	}

	dataShards := ecSpec.DataShards
	if dataShards == 0 {
		dataShards = 4
	}
	parityShards := ecSpec.ParityShards
	if parityShards == 0 {
		parityShards = 2
	}

	destNode := healthyNodes[0]

	if err := r.shardReplicator.RegenerateShard(ctx, task.ChunkID, task.AvailableNodes, destNode, dataShards, parityShards); err != nil {
		return fmt.Errorf("regenerating shard: %w", err)
	}

	// Update placement map
	placement, err := r.metaClient.GetPlacementMap(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("getting placement map: %w", err)
	}

	// Replace a failed node with the new node
	updated := false
	for i, node := range placement.Nodes {
		isFailed := false
		for _, failed := range task.FailedNodes {
			if node == failed {
				isFailed = true
				break
			}
		}
		if isFailed && !updated {
			placement.Nodes[i] = destNode
			updated = true
			break
		}
	}

	if !updated {
		// Add the new node if we didn't replace a failed one
		placement.Nodes = append(placement.Nodes, destNode)
	}

	if err := r.metaClient.PutPlacementMap(ctx, placement); err != nil {
		return fmt.Errorf("updating placement map: %w", err)
	}

	return nil
}

// sortQueue sorts the repair queue by priority (lower first).
func (r *Reconciler) sortQueue() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Sort by priority (lower = more urgent), then by creation time (older first)
	for i := 0; i < len(r.repairQueue); i++ {
		for j := i + 1; j < len(r.repairQueue); j++ {
			if r.repairQueue[i].Priority > r.repairQueue[j].Priority ||
				(r.repairQueue[i].Priority == r.repairQueue[j].Priority &&
					r.repairQueue[i].CreatedAt.After(r.repairQueue[j].CreatedAt)) {
				r.repairQueue[i], r.repairQueue[j] = r.repairQueue[j], r.repairQueue[i]
			}
		}
	}
}

// QueueSize returns the number of pending repair tasks.
func (r *Reconciler) QueueSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.repairQueue)
}

// CompletedRepairs returns the total number of completed repairs.
func (r *Reconciler) CompletedRepairs() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completedRepairs
}

// FailedRepairs returns the total number of failed repairs.
func (r *Reconciler) FailedRepairs() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failedRepairs
}

// LastScanInfo returns information about the last compliance scan.
func (r *Reconciler) LastScanInfo() (lastScan time.Time, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastScanTime, r.lastScanDuration
}

// getPoolLookup returns the pool lookup for use by the policy engine runnable.
func (r *Reconciler) getPoolLookup() PoolLookup {
	return r.poolLookup
}
