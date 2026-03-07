package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metrics"
)

// GCResult holds the results of a garbage collection run.
type GCResult struct {
	OrphanPlacementsDeleted int
	StaleNodesDeleted       int
	Duration                time.Duration
}

// GarbageCollector handles periodic garbage collection of orphan metadata.
// It runs only on the Raft leader to avoid duplicate work.
type GarbageCollector struct {
	store    *RaftStore
	interval time.Duration
	ttl      time.Duration // TTL for considering node metadata as stale

	mu         sync.Mutex
	running    bool
	cancel     context.CancelFunc
	lastResult *GCResult
}

// NewGarbageCollector creates a new GarbageCollector.
func NewGarbageCollector(store *RaftStore, interval, ttl time.Duration) *GarbageCollector {
	return &GarbageCollector{
		store:    store,
		interval: interval,
		ttl:      ttl,
	}
}

// Start begins the periodic garbage collection loop.
// GC only runs when this node is the Raft leader.
func (gc *GarbageCollector) Start(ctx context.Context) {
	gc.mu.Lock()
	if gc.running {
		gc.mu.Unlock()
		return
	}
	gc.running = true
	ctx, gc.cancel = context.WithCancel(ctx)
	gc.mu.Unlock()

	go gc.loop(ctx)
}

// Stop halts the garbage collection loop.
func (gc *GarbageCollector) Stop() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	if !gc.running {
		return
	}
	gc.running = false
	if gc.cancel != nil {
		gc.cancel()
		gc.cancel = nil
	}
}

// loop runs periodic GC while this node is the leader.
func (gc *GarbageCollector) loop(ctx context.Context) {
	ticker := time.NewTicker(gc.interval)
	defer ticker.Stop()

	// Run an initial GC immediately if we're the leader.
	gc.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gc.runOnce(ctx)
		}
	}
}

// runOnce executes a single GC pass if this node is the leader.
func (gc *GarbageCollector) runOnce(ctx context.Context) {
	if !gc.store.IsLeader() {
		return
	}

	start := time.Now()
	result, err := gc.RunOnce(ctx)
	duration := time.Since(start)

	if err != nil {
		logging.L.Error("metadata gc failed", zap.Error(err))
		return
	}

	if result.OrphanPlacementsDeleted > 0 || result.StaleNodesDeleted > 0 {
		logging.L.Info("metadata gc completed",
			zap.Int("orphan_placements_deleted", result.OrphanPlacementsDeleted),
			zap.Int("stale_nodes_deleted", result.StaleNodesDeleted),
			zap.Duration("duration", duration))
	}

	metrics.GCDurationSeconds.Observe(duration.Seconds())

	gc.mu.Lock()
	gc.lastResult = result
	gc.mu.Unlock()
}

// RunOnce executes a single garbage collection pass.
// It scans all metadata to find orphan chunks and stale node entries.
func (gc *GarbageCollector) RunOnce(ctx context.Context) (*GCResult, error) {
	start := time.Now()
	result := &GCResult{}

	// Build the set of all referenced chunks from all metadata types.
	referencedChunks, err := gc.buildReferencedChunkSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("building referenced chunk set: %w", err)
	}

	// Find and delete orphan placement maps.
	orphanPlacements, err := gc.deleteOrphanPlacements(ctx, referencedChunks)
	if err != nil {
		return nil, fmt.Errorf("deleting orphan placements: %w", err)
	}
	result.OrphanPlacementsDeleted = orphanPlacements

	// Delete stale node metadata.
	staleNodes, err := gc.deleteStaleNodeMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("deleting stale node metadata: %w", err)
	}
	result.StaleNodesDeleted = staleNodes

	result.Duration = time.Since(start)

	// Store the result for querying via LastResult.
	gc.mu.Lock()
	gc.lastResult = result
	gc.mu.Unlock()

	return result, nil
}

// buildReferencedChunkSet scans all metadata and returns the set of chunk IDs that are referenced.
func (gc *GarbageCollector) buildReferencedChunkSet(ctx context.Context) (map[string]struct{}, error) {
	referenced := make(map[string]struct{})

	// Scan volumes.
	volumes, err := gc.store.ListVolumesMeta(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}
	for _, v := range volumes {
		for _, chunkID := range v.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	// Scan snapshots.
	snapshots, err := gc.store.ListSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing snapshots: %w", err)
	}
	for _, s := range snapshots {
		for _, chunkID := range s.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	// Scan objects.
	objects, err := gc.store.fsm.GetAll(bucketObjects)
	if err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}
	for _, data := range objects {
		var obj ObjectMeta
		if err := unmarshalJSON(data, &obj); err != nil {
			logging.L.Warn("failed to unmarshal object metadata during GC", zap.Error(err))
			continue
		}
		for _, chunkID := range obj.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	// Scan inodes.
	inodes, err := gc.store.fsm.GetAll(bucketInodes)
	if err != nil {
		return nil, fmt.Errorf("listing inodes: %w", err)
	}
	for _, data := range inodes {
		var inode InodeMeta
		if err := unmarshalJSON(data, &inode); err != nil {
			logging.L.Warn("failed to unmarshal inode metadata during GC", zap.Error(err))
			continue
		}
		for _, chunkID := range inode.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	// Scan multipart uploads (parts may reference chunks).
	multipart, err := gc.store.fsm.GetAll(bucketMultipart)
	if err != nil {
		return nil, fmt.Errorf("listing multipart uploads: %w", err)
	}
	for _, data := range multipart {
		var mu MultipartUpload
		if err := unmarshalJSON(data, &mu); err != nil {
			logging.L.Warn("failed to unmarshal multipart upload during GC", zap.Error(err))
			continue
		}
		for _, part := range mu.Parts {
			for _, chunkID := range part.ChunkIDs {
				referenced[chunkID] = struct{}{}
			}
		}
	}

	return referenced, nil
}

// deleteOrphanPlacements removes placement maps for chunks not in the referenced set.
func (gc *GarbageCollector) deleteOrphanPlacements(ctx context.Context, referenced map[string]struct{}) (int, error) {
	allPlacements, err := gc.store.ListPlacementMaps(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing placement maps: %w", err)
	}

	deleted := 0
	for _, pm := range allPlacements {
		if _, isReferenced := referenced[pm.ChunkID]; !isReferenced {
			if err := gc.store.DeletePlacementMap(ctx, pm.ChunkID); err != nil {
				logging.L.Warn("failed to delete orphan placement map",
					zap.String("chunkID", pm.ChunkID),
					zap.Error(err))
				continue
			}
			deleted++
			metrics.GCOrphanPlacementsDeleted.Inc()
		}
	}

	return deleted, nil
}

// deleteStaleNodeMetadata removes node metadata entries that haven't sent a heartbeat recently.
func (gc *GarbageCollector) deleteStaleNodeMetadata(ctx context.Context) (int, error) {
	nodes, err := gc.store.ListNodeMetas(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing node metadata: %w", err)
	}

	cutoff := time.Now().Add(-gc.ttl).Unix()
	deleted := 0

	for _, node := range nodes {
		// Skip nodes that are offline (they may have just been gracefully shut down).
		if node.Status == "offline" {
			// Only delete offline nodes if the heartbeat is very old (> 7 days).
			offlineCutoff := time.Now().Add(-7 * 24 * time.Hour).Unix()
			if node.LastHeartbeat > offlineCutoff {
				continue
			}
		}

		// Delete nodes that haven't sent a heartbeat within the TTL.
		if node.LastHeartbeat < cutoff {
			if err := gc.store.DeleteNodeMeta(ctx, node.NodeID); err != nil {
				logging.L.Warn("failed to delete stale node metadata",
					zap.String("nodeID", node.NodeID),
					zap.Error(err))
				continue
			}
			deleted++
			metrics.GCStaleNodesDeleted.Inc()
			logging.L.Info("deleted stale node metadata",
				zap.String("nodeID", node.NodeID),
				zap.Int64("lastHeartbeat", node.LastHeartbeat))
		}
	}

	return deleted, nil
}

// GetAllReferencedChunks is a helper that returns all chunk IDs referenced by metadata.
// This is exposed for agents to query and determine which local chunks are orphaned.
func (gc *GarbageCollector) GetAllReferencedChunks(ctx context.Context) (map[string]struct{}, error) {
	return gc.buildReferencedChunkSet(ctx)
}

// LastResult returns the result of the most recent GC run.
func (gc *GarbageCollector) LastResult() *GCResult {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.lastResult
}

// unmarshalJSON is a helper to unmarshal JSON with logging on failure.
func unmarshalJSON(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
