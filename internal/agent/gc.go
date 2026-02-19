package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/metrics"
)

// GCResult holds the results of an agent garbage collection run.
type GCResult struct {
	OrphanChunksDeleted int
	Duration            time.Duration
}

// GarbageCollector handles periodic garbage collection of orphan chunks
// on the local agent. It queries the metadata service to determine which
// local chunks are no longer referenced and deletes them.
type GarbageCollector struct {
	store      chunk.Store
	metaClient MetadataClient
	interval   time.Duration
	batchSize  int

	mu         sync.Mutex
	running    bool
	cancel     context.CancelFunc
	lastResult *GCResult
}

// MetadataClient defines the interface for querying metadata to determine
// which chunks are still referenced.
type MetadataClient interface {
	// GetAllReferencedChunks returns the set of chunk IDs referenced by metadata.
	GetAllReferencedChunks(ctx context.Context) (map[string]struct{}, error)
}

// metadataAdapter adapts the metadata.GRPCClient to implement MetadataClient.
// It queries the metadata service for all referenced chunks.
type metadataAdapter struct {
	client *metadata.GRPCClient
}

func (m *metadataAdapter) GetAllReferencedChunks(ctx context.Context) (map[string]struct{}, error) {
	// Query volumes for referenced chunks.
	volumes, err := m.client.ListVolumesMeta(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}

	referenced := make(map[string]struct{})
	for _, v := range volumes {
		for _, chunkID := range v.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	// Query snapshots.
	snapshots, err := m.client.ListSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing snapshots: %w", err)
	}
	for _, s := range snapshots {
		for _, chunkID := range s.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	// Query objects.
	objects, err := m.client.ListObjectMetas(ctx, "", "")
	if err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}
	for _, obj := range objects {
		for _, chunkID := range obj.ChunkIDs {
			referenced[chunkID] = struct{}{}
		}
	}

	return referenced, nil
}

// NewGarbageCollector creates a new GarbageCollector.
func NewGarbageCollector(store chunk.Store, metaClient *metadata.GRPCClient, interval time.Duration) *GarbageCollector {
	return &GarbageCollector{
		store:      store,
		metaClient: &metadataAdapter{client: metaClient},
		interval:   interval,
		batchSize:  100, // Process chunks in batches to avoid long-running operations
	}
}

// Start begins the periodic garbage collection loop.
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

// loop runs periodic GC.
func (gc *GarbageCollector) loop(ctx context.Context) {
	ticker := time.NewTicker(gc.interval)
	defer ticker.Stop()

	// Run an initial GC after a short delay to allow the system to start up.
	select {
	case <-time.After(30 * time.Second):
		gc.runOnce(ctx)
	case <-ctx.Done():
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gc.runOnce(ctx)
		}
	}
}

// runOnce executes a single GC pass.
func (gc *GarbageCollector) runOnce(ctx context.Context) {
	start := time.Now()
	result, err := gc.RunOnce(ctx)
	duration := time.Since(start)

	if err != nil {
		logging.L.Error("agent gc failed", zap.Error(err))
		return
	}

	if result.OrphanChunksDeleted > 0 {
		logging.L.Info("agent gc completed",
			zap.Int("orphan_chunks_deleted", result.OrphanChunksDeleted),
			zap.Duration("duration", duration))
	}

	metrics.GCLastRunTimestamp.Set(float64(time.Now().Unix()))

	gc.mu.Lock()
	gc.lastResult = result
	gc.mu.Unlock()
}

// RunOnce executes a single garbage collection pass.
// It lists local chunks, queries metadata for references, and deletes orphans.
func (gc *GarbageCollector) RunOnce(ctx context.Context) (*GCResult, error) {
	start := time.Now()
	result := &GCResult{}

	// List all local chunks.
	localChunks, err := gc.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing local chunks: %w", err)
	}

	// Build set of local chunk IDs for fast lookup.
	localSet := make(map[string]struct{}, len(localChunks))
	for _, id := range localChunks {
		localSet[string(id)] = struct{}{}
	}

	// Query metadata for all referenced chunks.
	referenced, err := gc.metaClient.GetAllReferencedChunks(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting referenced chunks: %w", err)
	}

	// Find and delete orphan chunks in batches.
	deleted := 0
	for _, chunkID := range localChunks {
		select {
		case <-ctx.Done():
			result.OrphanChunksDeleted = deleted
			return result, ctx.Err()
		default:
		}

		chunkIDStr := string(chunkID)
		if _, isReferenced := referenced[chunkIDStr]; !isReferenced {
			if err := gc.store.Delete(ctx, chunkID); err != nil {
				logging.L.Warn("failed to delete orphan chunk",
					zap.String("chunkID", chunkIDStr),
					zap.Error(err))
				continue
			}
			deleted++
			metrics.GCOrphanChunksDeleted.Inc()

			// Log progress periodically.
			if deleted%100 == 0 {
				logging.L.Info("agent gc progress",
					zap.Int("deleted_so_far", deleted),
					zap.Int("remaining", len(localChunks)-deleted))
			}
		}
	}

	result.OrphanChunksDeleted = deleted
	result.Duration = time.Since(start)

	// Store the result for querying via LastResult.
	gc.mu.Lock()
	gc.lastResult = result
	gc.mu.Unlock()

	return result, nil
}

// LastResult returns the result of the most recent GC run.
func (gc *GarbageCollector) LastResult() *GCResult {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.lastResult
}
