package operator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/logging"
)

// NodeStatus represents the health state of a storage node.
type NodeStatus int

const (
	// NodeHealthy indicates the node is healthy and responding.
	NodeHealthy NodeStatus = iota
	// NodeSuspect indicates the node hasn't been heard from recently.
	NodeSuspect
	// NodeDown indicates the node is considered down.
	NodeDown
)

// NodeInfo tracks a node's health state.
type NodeInfo struct {
	ID         string
	LastSeen   time.Time
	Status     NodeStatus
	ChunkCount int64
}

// PlacementLookup finds chunks affected by a failed node.
type PlacementLookup interface {
	ChunksOnNode(ctx context.Context, nodeID string) ([]string, error)
	ReplicaNodes(ctx context.Context, chunkID string) ([]string, error)
	UpdatePlacement(ctx context.Context, chunkID string, oldNode, newNode string) error
}

// ChunkReplicator re-replicates a chunk from source to destination.
type ChunkReplicator interface {
	ReplicateChunk(ctx context.Context, chunkID, sourceNode, destNode string) error
}

// NodeHealthChecker checks if a node is reachable.
type NodeHealthChecker interface {
	IsNodeHealthy(ctx context.Context, nodeID string) bool
}

// RecoveryManager monitors node health and recovers under-replicated chunks.
type RecoveryManager struct {
	mu              sync.Mutex
	nodes           map[string]*NodeInfo
	placement       PlacementLookup
	replicator      ChunkReplicator
	healthChecker   NodeHealthChecker
	suspectTimeout  time.Duration
	downTimeout     time.Duration
	maxConcurrent   int
	pendingRecovery []recoveryTask
	completedCount  int64
}

type recoveryTask struct {
	ChunkID    string
	SourceNode string
	DestNode   string
	Priority   int // lower = higher priority (fewer surviving copies)
}

// NewRecoveryManager creates a RecoveryManager with sensible defaults.
func NewRecoveryManager(placement PlacementLookup, replicator ChunkReplicator, healthChecker NodeHealthChecker) *RecoveryManager {
	return &RecoveryManager{
		nodes:          make(map[string]*NodeInfo),
		placement:      placement,
		replicator:     replicator,
		healthChecker:  healthChecker,
		suspectTimeout: 30 * time.Second,
		downTimeout:    60 * time.Second,
		maxConcurrent:  4,
	}
}

// Heartbeat updates a node's LastSeen timestamp and sets it to Healthy.
func (rm *RecoveryManager) Heartbeat(nodeID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	info, ok := rm.nodes[nodeID]
	if !ok {
		info = &NodeInfo{ID: nodeID}
		rm.nodes[nodeID] = info
	}
	info.LastSeen = time.Now()
	info.Status = NodeHealthy
}

// CheckNodes iterates all nodes and updates status based on timeouts.
func (rm *RecoveryManager) CheckNodes(_ context.Context) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	now := time.Now()
	for _, info := range rm.nodes {
		elapsed := now.Sub(info.LastSeen)
		switch {
		case elapsed > rm.downTimeout:
			if info.Status != NodeDown {
				logging.L.Warn("node marked DOWN", zap.String("nodeID", info.ID), zap.Duration("elapsed", elapsed))
			}
			info.Status = NodeDown
		case elapsed > rm.suspectTimeout:
			if info.Status == NodeHealthy {
				logging.L.Warn("node marked SUSPECT", zap.String("nodeID", info.ID), zap.Duration("elapsed", elapsed))
			}
			info.Status = NodeSuspect
		default:
			info.Status = NodeHealthy
		}
	}
}

// RecoverNode finds all chunks on the down node and schedules replication tasks.
func (rm *RecoveryManager) RecoverNode(ctx context.Context, nodeID string) error {
	chunks, err := rm.placement.ChunksOnNode(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("listing chunks on node %s: %w", nodeID, err)
	}

	rm.mu.Lock()
	// Collect healthy node IDs for destination selection.
	var healthyNodes []string
	for id, info := range rm.nodes {
		if id != nodeID && info.Status == NodeHealthy {
			healthyNodes = append(healthyNodes, id)
		}
	}
	rm.mu.Unlock()

	if len(healthyNodes) == 0 {
		return fmt.Errorf("no healthy nodes available for recovery")
	}

	destIdx := 0
	for _, chunkID := range chunks {
		replicas, err := rm.placement.ReplicaNodes(ctx, chunkID)
		if err != nil {
			logging.L.Error("error finding replicas for chunk", zap.String("chunkID", chunkID), zap.Error(err))
			continue
		}

		// Find a surviving replica node (not the failed node).
		var sourceNode string
		for _, r := range replicas {
			if r != nodeID {
				sourceNode = r
				break
			}
		}
		if sourceNode == "" {
			logging.L.Warn("no surviving replica found for chunk", zap.String("chunkID", chunkID))
			continue
		}

		// Pick a healthy destination that doesn't already hold this chunk.
		destNode := ""
		for i := 0; i < len(healthyNodes); i++ {
			candidate := healthyNodes[(destIdx+i)%len(healthyNodes)]
			alreadyHas := false
			for _, r := range replicas {
				if r == candidate {
					alreadyHas = true
					break
				}
			}
			if !alreadyHas {
				destNode = candidate
				destIdx = (destIdx + i + 1) % len(healthyNodes)
				break
			}
		}
		if destNode == "" {
			logging.L.Warn("no suitable destination for chunk", zap.String("chunkID", chunkID))
			continue
		}

		// Priority = number of surviving replicas (fewer = more urgent).
		survivingCount := 0
		for _, r := range replicas {
			if r != nodeID {
				survivingCount++
			}
		}

		rm.mu.Lock()
		rm.pendingRecovery = append(rm.pendingRecovery, recoveryTask{
			ChunkID:    chunkID,
			SourceNode: sourceNode,
			DestNode:   destNode,
			Priority:   survivingCount,
		})
		rm.mu.Unlock()
	}

	// Sort pending tasks by priority (fewer surviving replicas first).
	rm.mu.Lock()
	sort.Slice(rm.pendingRecovery, func(i, j int) bool {
		return rm.pendingRecovery[i].Priority < rm.pendingRecovery[j].Priority
	})
	rm.mu.Unlock()

	return nil
}

// ProcessRecoveryQueue processes pending recovery tasks with a concurrency limit.
func (rm *RecoveryManager) ProcessRecoveryQueue(ctx context.Context) error {
	rm.mu.Lock()
	tasks := make([]recoveryTask, len(rm.pendingRecovery))
	copy(tasks, rm.pendingRecovery)
	rm.pendingRecovery = nil
	rm.mu.Unlock()

	sem := make(chan struct{}, rm.maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(t recoveryTask) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := rm.replicator.ReplicateChunk(ctx, t.ChunkID, t.SourceNode, t.DestNode); err != nil {
				logging.L.Error("failed to replicate chunk",
					zap.String("chunkID", t.ChunkID),
					zap.String("source", t.SourceNode),
					zap.String("dest", t.DestNode),
					zap.Error(err),
				)
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			if err := rm.placement.UpdatePlacement(ctx, t.ChunkID, t.SourceNode, t.DestNode); err != nil {
				logging.L.Error("failed to update placement for chunk", zap.String("chunkID", t.ChunkID), zap.Error(err))
			}

			rm.mu.Lock()
			rm.completedCount++
			rm.mu.Unlock()
		}(task)
	}

	wg.Wait()
	return firstErr
}

// SetMaxConcurrent overrides the maximum number of concurrent recovery
// operations. It must be called before ProcessRecoveryQueue is invoked.
func (rm *RecoveryManager) SetMaxConcurrent(n int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if n > 0 {
		rm.maxConcurrent = n
	}
}

// SetDownTimeout overrides the duration after which a node without a
// heartbeat is marked Down. The suspect timeout is set to half this value.
func (rm *RecoveryManager) SetDownTimeout(d time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.downTimeout = d
	rm.suspectTimeout = d / 2
}

// NodeCount returns the total number of tracked nodes.
func (rm *RecoveryManager) NodeCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return len(rm.nodes)
}

// PendingRecoveries returns the number of pending recovery tasks.
func (rm *RecoveryManager) PendingRecoveries() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return len(rm.pendingRecovery)
}

// CompletedRecoveries returns the count of completed recovery tasks.
func (rm *RecoveryManager) CompletedRecoveries() int64 {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.completedCount
}
