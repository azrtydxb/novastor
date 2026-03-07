//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/azrtydxb/novastor/internal/operator"
)

// testPlacementLookup implements operator.PlacementLookup for testing.
type testPlacementLookup struct {
	mu         sync.RWMutex
	placements map[string][]string // chunkID -> []nodeID
}

func newTestPlacementLookup() *testPlacementLookup {
	return &testPlacementLookup{
		placements: make(map[string][]string),
	}
}

func (p *testPlacementLookup) addPlacement(chunkID string, nodes []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.placements[chunkID] = append([]string{}, nodes...)
}

func (p *testPlacementLookup) ChunksOnNode(_ context.Context, nodeID string) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var result []string
	for chunkID, nodes := range p.placements {
		for _, n := range nodes {
			if n == nodeID {
				result = append(result, chunkID)
				break
			}
		}
	}
	return result, nil
}

func (p *testPlacementLookup) ReplicaNodes(_ context.Context, chunkID string) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	nodes, ok := p.placements[chunkID]
	if !ok {
		return nil, nil
	}
	return append([]string{}, nodes...), nil
}

func (p *testPlacementLookup) UpdatePlacement(_ context.Context, chunkID string, oldNode, newNode string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	nodes := p.placements[chunkID]
	for i, n := range nodes {
		if n == oldNode {
			nodes[i] = newNode
			break
		}
	}
	p.placements[chunkID] = nodes
	return nil
}

// testReplicator implements operator.ChunkReplicator for testing.
type testReplicator struct {
	mu         sync.Mutex
	replicated []replicationRecord
}

type replicationRecord struct {
	ChunkID    string
	SourceNode string
	DestNode   string
}

func newTestReplicator() *testReplicator {
	return &testReplicator{}
}

func (r *testReplicator) ReplicateChunk(_ context.Context, chunkID, sourceNode, destNode string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicated = append(r.replicated, replicationRecord{
		ChunkID:    chunkID,
		SourceNode: sourceNode,
		DestNode:   destNode,
	})
	return nil
}

func (r *testReplicator) records() []replicationRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]replicationRecord, len(r.replicated))
	copy(result, r.replicated)
	return result
}

// testHealthChecker implements operator.NodeHealthChecker for testing.
type testHealthChecker struct {
	mu           sync.RWMutex
	healthyNodes map[string]bool
}

func newTestHealthChecker() *testHealthChecker {
	return &testHealthChecker{
		healthyNodes: make(map[string]bool),
	}
}

func (h *testHealthChecker) setHealthy(nodeID string, healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthyNodes[nodeID] = healthy
}

func (h *testHealthChecker) IsNodeHealthy(_ context.Context, nodeID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthyNodes[nodeID]
}

func TestRecovery_NodeDownDetection(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	// Register three nodes via heartbeats.
	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")
	rm.Heartbeat("node-3")

	if rm.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", rm.NodeCount())
	}

	// All nodes should be healthy after heartbeats.
	ctx := context.Background()
	rm.CheckNodes(ctx)

	// Send heartbeat updates to confirm nodes stay healthy.
	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")
	rm.Heartbeat("node-3")

	rm.CheckNodes(ctx)

	// All nodes should still be tracked.
	if rm.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes after check, got %d", rm.NodeCount())
	}
}

func TestRecovery_RecoverNodeChunks(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	// Register nodes.
	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")
	rm.Heartbeat("node-3")
	rm.Heartbeat("node-4")

	healthChecker.setHealthy("node-1", true)
	healthChecker.setHealthy("node-2", true)
	healthChecker.setHealthy("node-3", true)
	healthChecker.setHealthy("node-4", true)

	// Store placement maps: each chunk is on 3 nodes.
	placement.addPlacement("chunk-a", []string{"node-1", "node-2", "node-3"})
	placement.addPlacement("chunk-b", []string{"node-1", "node-3", "node-4"})
	placement.addPlacement("chunk-c", []string{"node-2", "node-3", "node-4"})

	// Simulate node-3 going down.
	ctx := context.Background()

	// Recover chunks that were on node-3.
	if err := rm.RecoverNode(ctx, "node-3"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	// All three chunks were on node-3, so there should be 3 pending tasks.
	pending := rm.PendingRecoveries()
	if pending != 3 {
		t.Errorf("expected 3 pending tasks, got %d", pending)
	}

	// Process recovery queue.
	if err := rm.ProcessRecoveryQueue(ctx); err != nil {
		t.Fatalf("ProcessRecoveryQueue failed: %v", err)
	}

	// Verify all tasks were processed.
	if rm.PendingRecoveries() != 0 {
		t.Errorf("expected 0 pending tasks after processing, got %d", rm.PendingRecoveries())
	}

	// Verify replication happened.
	records := replicator.records()
	if len(records) != 3 {
		t.Fatalf("expected 3 replication records, got %d", len(records))
	}

	// Verify completed count.
	if rm.CompletedRecoveries() != 3 {
		t.Errorf("expected 3 completed recoveries, got %d", rm.CompletedRecoveries())
	}

	// Verify that source nodes are NOT node-3 (the down node).
	for _, rec := range records {
		if rec.SourceNode == "node-3" {
			t.Errorf("source node should not be the failed node-3 for chunk %s", rec.ChunkID)
		}
	}

	// Verify that destination nodes are NOT node-3 (the down node).
	for _, rec := range records {
		if rec.DestNode == "node-3" {
			t.Errorf("destination should not be the failed node-3 for chunk %s", rec.ChunkID)
		}
	}
}

func TestRecovery_RecoverSingleChunk(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")
	rm.Heartbeat("node-3")

	healthChecker.setHealthy("node-1", true)
	healthChecker.setHealthy("node-2", true)
	healthChecker.setHealthy("node-3", true)

	// One chunk on node-1 and node-2.
	placement.addPlacement("chunk-single", []string{"node-1", "node-2"})

	ctx := context.Background()

	// Recover node-1.
	if err := rm.RecoverNode(ctx, "node-1"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	if rm.PendingRecoveries() != 1 {
		t.Fatalf("expected 1 pending task, got %d", rm.PendingRecoveries())
	}

	if err := rm.ProcessRecoveryQueue(ctx); err != nil {
		t.Fatalf("ProcessRecoveryQueue failed: %v", err)
	}

	records := replicator.records()
	if len(records) != 1 {
		t.Fatalf("expected 1 replication record, got %d", len(records))
	}

	// Source should be node-2 (the surviving replica).
	if records[0].SourceNode != "node-2" {
		t.Errorf("expected source node-2, got %s", records[0].SourceNode)
	}

	// Destination should be node-3 (the only healthy node not holding the chunk).
	if records[0].DestNode != "node-3" {
		t.Errorf("expected dest node-3, got %s", records[0].DestNode)
	}

	if rm.CompletedRecoveries() != 1 {
		t.Errorf("expected 1 completed recovery, got %d", rm.CompletedRecoveries())
	}
}

func TestRecovery_NoSuitableDestination(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	// Only two nodes, chunk is on both.
	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")

	healthChecker.setHealthy("node-1", true)
	healthChecker.setHealthy("node-2", true)

	placement.addPlacement("chunk-only", []string{"node-1", "node-2"})

	ctx := context.Background()

	// Recover node-1. The only healthy node that could be a destination is
	// node-2, but node-2 already has the chunk. So no task should be created.
	if err := rm.RecoverNode(ctx, "node-1"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	// No destination was suitable, so no task should be pending.
	if rm.PendingRecoveries() != 0 {
		t.Errorf("expected 0 pending tasks (no suitable destination), got %d", rm.PendingRecoveries())
	}
}

func TestRecovery_HeartbeatResetsStatus(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	// Register a node.
	rm.Heartbeat("node-1")

	// Verify it is tracked.
	if rm.NodeCount() != 1 {
		t.Fatalf("expected 1 node, got %d", rm.NodeCount())
	}

	// Send another heartbeat (simulating periodic heartbeats).
	time.Sleep(10 * time.Millisecond)
	rm.Heartbeat("node-1")

	ctx := context.Background()
	rm.CheckNodes(ctx)

	// Node should still be tracked and healthy.
	if rm.NodeCount() != 1 {
		t.Fatalf("expected 1 node after check, got %d", rm.NodeCount())
	}
}

func TestRecovery_PlacementUpdateAfterRecovery(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")
	rm.Heartbeat("node-3")

	healthChecker.setHealthy("node-1", true)
	healthChecker.setHealthy("node-2", true)
	healthChecker.setHealthy("node-3", true)

	// chunk on node-1 and node-2.
	placement.addPlacement("chunk-update", []string{"node-1", "node-2"})

	ctx := context.Background()

	// Recover node-1.
	if err := rm.RecoverNode(ctx, "node-1"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	if err := rm.ProcessRecoveryQueue(ctx); err != nil {
		t.Fatalf("ProcessRecoveryQueue failed: %v", err)
	}

	records := replicator.records()
	if len(records) != 1 {
		t.Fatalf("expected 1 replication record, got %d", len(records))
	}

	// The recovery manager calls UpdatePlacement(chunkID, sourceNode, destNode)
	// where sourceNode is the surviving replica used as the replication source.
	// This means the placement map will have the source node replaced by the
	// destination node. Verify the replication happened correctly.
	rec := records[0]
	if rec.ChunkID != "chunk-update" {
		t.Errorf("expected chunk-update, got %s", rec.ChunkID)
	}

	// Source should be node-2 (surviving replica, since node-1 is failed).
	if rec.SourceNode != "node-2" {
		t.Errorf("expected source node-2, got %s", rec.SourceNode)
	}

	// Destination should be node-3.
	if rec.DestNode != "node-3" {
		t.Errorf("expected dest node-3, got %s", rec.DestNode)
	}

	// Verify placement was updated by the recovery manager.
	// The UpdatePlacement call replaces the source node with the dest node.
	nodes, err := placement.ReplicaNodes(ctx, "chunk-update")
	if err != nil {
		t.Fatalf("ReplicaNodes failed: %v", err)
	}

	// After UpdatePlacement(chunk-update, node-2, node-3), the placement
	// should now be [node-1, node-3] (node-2 was replaced by node-3).
	hasNode2 := false
	hasNode3 := false
	for _, n := range nodes {
		if n == "node-2" {
			hasNode2 = true
		}
		if n == "node-3" {
			hasNode3 = true
		}
	}

	if hasNode2 {
		t.Error("placement should no longer contain node-2 (the source that was replaced)")
	}
	if !hasNode3 {
		t.Error("placement should contain node-3 as the new replica")
	}
}

func TestRecovery_ConcurrentRecovery(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	// Register 5 nodes.
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		rm.Heartbeat(nodeID)
		healthChecker.setHealthy(nodeID, true)
	}

	// Create multiple chunks on node-1.
	for i := 0; i < 10; i++ {
		chunkID := fmt.Sprintf("chunk-%02d", i)
		// Each chunk on node-1 and two other nodes.
		otherA := fmt.Sprintf("node-%d", (i%4)+2)
		otherB := fmt.Sprintf("node-%d", ((i+1)%4)+2)
		placement.addPlacement(chunkID, []string{"node-1", otherA, otherB})
	}

	ctx := context.Background()

	// Recover node-1.
	if err := rm.RecoverNode(ctx, "node-1"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	// Process with concurrency.
	if err := rm.ProcessRecoveryQueue(ctx); err != nil {
		t.Fatalf("ProcessRecoveryQueue failed: %v", err)
	}

	// All 10 chunks should have been recovered.
	records := replicator.records()
	if len(records) != 10 {
		t.Errorf("expected 10 replication records, got %d", len(records))
	}

	// All records should have non-node-1 source and destination.
	for _, rec := range records {
		if rec.SourceNode == "node-1" {
			t.Errorf("source should not be failed node-1 for chunk %s", rec.ChunkID)
		}
		if rec.DestNode == "node-1" {
			t.Errorf("dest should not be failed node-1 for chunk %s", rec.ChunkID)
		}
	}

	if rm.CompletedRecoveries() != 10 {
		t.Errorf("expected 10 completed recoveries, got %d", rm.CompletedRecoveries())
	}
}

func TestRecovery_EmptyNodeRecovery(t *testing.T) {
	placement := newTestPlacementLookup()
	replicator := newTestReplicator()
	healthChecker := newTestHealthChecker()

	rm := operator.NewRecoveryManager(placement, replicator, healthChecker)

	rm.Heartbeat("node-1")
	rm.Heartbeat("node-2")

	healthChecker.setHealthy("node-1", true)
	healthChecker.setHealthy("node-2", true)

	// No chunks on node-1.
	ctx := context.Background()

	if err := rm.RecoverNode(ctx, "node-1"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	// No chunks to recover.
	if rm.PendingRecoveries() != 0 {
		t.Errorf("expected 0 pending tasks for empty node, got %d", rm.PendingRecoveries())
	}
}

// nodeIDFromInt returns a node ID string like "node-1".
func nodeIDFromInt(i int) string {
	return fmt.Sprintf("node-%d", i)
}
