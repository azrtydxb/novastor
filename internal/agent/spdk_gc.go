package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
)

// SPDKGarbageCollector handles periodic garbage collection of orphan chunks
// via the SPDK data-plane gRPC interface. It queries the metadata service
// to determine which chunks are still referenced, then calls the dataplane's
// GarbageCollect RPC to delete unreferenced chunks.
type SPDKGarbageCollector struct {
	dpClient   *dataplane.Client
	metaClient *metadata.GRPCClient
	bdevName   string
	interval   time.Duration

	mu         sync.Mutex
	running    bool
	cancel     context.CancelFunc
	lastResult *GCResult
}

// NewSPDKGarbageCollector creates a garbage collector that operates via
// the SPDK data-plane gRPC interface.
func NewSPDKGarbageCollector(dpClient *dataplane.Client, metaClient *metadata.GRPCClient, bdevName string, interval time.Duration) *SPDKGarbageCollector {
	return &SPDKGarbageCollector{
		dpClient:   dpClient,
		metaClient: metaClient,
		bdevName:   bdevName,
		interval:   interval,
	}
}

// Start begins the periodic garbage collection loop.
func (gc *SPDKGarbageCollector) Start(ctx context.Context) {
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
func (gc *SPDKGarbageCollector) Stop() {
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
func (gc *SPDKGarbageCollector) loop(ctx context.Context) {
	ticker := time.NewTicker(gc.interval)
	defer ticker.Stop()

	// Run an initial GC after a short delay to allow the system to stabilize.
	select {
	case <-time.After(60 * time.Second):
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
func (gc *SPDKGarbageCollector) runOnce(ctx context.Context) {
	start := time.Now()
	result, err := gc.RunOnce(ctx)
	duration := time.Since(start)

	if err != nil {
		logging.L.Error("agent spdk gc failed", zap.Error(err))
		return
	}

	if result.OrphanChunksDeleted > 0 {
		logging.L.Info("agent spdk gc completed",
			zap.Int("orphan_chunks_deleted", result.OrphanChunksDeleted),
			zap.Duration("duration", duration))
	} else {
		logging.L.Debug("agent spdk gc completed: no orphans found")
	}

	metrics.GCLastRunTimestamp.Set(float64(time.Now().Unix()))

	gc.mu.Lock()
	gc.lastResult = result
	gc.mu.Unlock()
}

// RunOnce executes a single garbage collection pass via the dataplane gRPC.
// It queries metadata for all referenced chunks, then calls the dataplane's
// GarbageCollect RPC which deletes anything not in the valid set.
func (gc *SPDKGarbageCollector) RunOnce(ctx context.Context) (*GCResult, error) {
	start := time.Now()
	result := &GCResult{}

	// Gather all referenced chunks from metadata.
	adapter := &metadataAdapter{client: gc.metaClient}
	referenced, err := adapter.GetAllReferencedChunks(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting referenced chunks: %w", err)
	}

	// Convert to a list for the gRPC RPC.
	validIDs := make([]string, 0, len(referenced))
	for id := range referenced {
		validIDs = append(validIDs, id)
	}

	// Call the dataplane's GarbageCollect RPC which handles the actual deletion.
	deleted, errors, err := gc.dpClient.GarbageCollect(gc.bdevName, validIDs)
	if err != nil {
		return nil, fmt.Errorf("dataplane GarbageCollect RPC: %w", err)
	}

	result.OrphanChunksDeleted = int(deleted)
	result.Duration = time.Since(start)

	if errors > 0 {
		logging.L.Warn("agent spdk gc had errors",
			zap.Uint32("errors", errors),
			zap.Uint32("deleted", deleted))
	}

	// Update Prometheus metrics.
	for i := uint32(0); i < deleted; i++ {
		metrics.GCOrphanChunksDeleted.Inc()
	}

	gc.mu.Lock()
	gc.lastResult = result
	gc.mu.Unlock()

	return result, nil
}

// LastResult returns the result of the most recent GC run.
func (gc *SPDKGarbageCollector) LastResult() *GCResult {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.lastResult
}
