package metadata

import (
	"context"
	"testing"
	"time"
)

func TestNodeMetaGC(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Add a fresh node (should NOT be deleted)
	freshNode := &NodeMeta{
		NodeID:        "node-fresh",
		Address:       "10.0.0.1:9100",
		Status:        "ready",
		LastHeartbeat: time.Now().Unix(),
	}
	if err := store.PutNodeMeta(ctx, freshNode); err != nil {
		t.Fatalf("Failed to put fresh node: %v", err)
	}

	// Add a stale node (should be deleted)
	staleNode := &NodeMeta{
		NodeID:        "node-stale",
		Address:       "10.0.0.2:9100",
		Status:        "ready",
		LastHeartbeat: time.Now().Add(-15 * time.Minute).Unix(), // 15 minutes ago
	}
	if err := store.PutNodeMeta(ctx, staleNode); err != nil {
		t.Fatalf("Failed to put stale node: %v", err)
	}

	// Add a borderline node (just under GC threshold, should NOT be deleted)
	borderlineNode := &NodeMeta{
		NodeID:        "node-borderline",
		Address:       "10.0.0.3:9100",
		Status:        "ready",
		LastHeartbeat: time.Now().Add(-9 * time.Minute).Unix(), // 9 minutes ago
	}
	if err := store.PutNodeMeta(ctx, borderlineNode); err != nil {
		t.Fatalf("Failed to put borderline node: %v", err)
	}

	// Simulate GC: delete nodes older than 10 minutes
	gcThreshold := 10 * time.Minute
	gcCutoff := time.Now().Add(-gcThreshold).Unix()

	nodes, err := store.ListNodeMetas(ctx)
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	deletedCount := 0
	for _, n := range nodes {
		if n.LastHeartbeat < gcCutoff {
			if err := store.DeleteNodeMeta(ctx, n.NodeID); err != nil {
				t.Errorf("Failed to delete stale node %s: %v", n.NodeID, err)
			}
			deletedCount++
		}
	}

	// Verify only one node was deleted
	if deletedCount != 1 {
		t.Errorf("Expected 1 node to be deleted, got %d", deletedCount)
	}

	// Verify fresh node still exists
	_, err = store.GetNodeMeta(ctx, "node-fresh")
	if err != nil {
		t.Errorf("Fresh node should still exist: %v", err)
	}

	// Verify borderline node still exists
	_, err = store.GetNodeMeta(ctx, "node-borderline")
	if err != nil {
		t.Errorf("Borderline node should still exist: %v", err)
	}

	// Verify stale node was deleted
	_, err = store.GetNodeMeta(ctx, "node-stale")
	if err == nil {
		t.Error("Stale node should have been deleted")
	}
}

func TestNodeMetaGCMultipleStale(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Add multiple stale nodes
	for i := 0; i < 5; i++ {
		staleNode := &NodeMeta{
			NodeID:        string(rune('a' + i)),
			Address:       "10.0.0.0:9100",
			Status:        "ready",
			LastHeartbeat: time.Now().Add(-20 * time.Minute).Unix(),
		}
		if err := store.PutNodeMeta(ctx, staleNode); err != nil {
			t.Fatalf("Failed to put stale node: %v", err)
		}
	}

	// Add one fresh node
	freshNode := &NodeMeta{
		NodeID:        "fresh",
		Address:       "10.0.0.1:9100",
		Status:        "ready",
		LastHeartbeat: time.Now().Unix(),
	}
	if err := store.PutNodeMeta(ctx, freshNode); err != nil {
		t.Fatalf("Failed to put fresh node: %v", err)
	}

	// Run GC
	gcThreshold := 10 * time.Minute
	gcCutoff := time.Now().Add(-gcThreshold).Unix()

	nodes, err := store.ListNodeMetas(ctx)
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	deletedCount := 0
	for _, n := range nodes {
		if n.LastHeartbeat < gcCutoff {
			if err := store.DeleteNodeMeta(ctx, n.NodeID); err != nil {
				t.Errorf("Failed to delete stale node %s: %v", n.NodeID, err)
			}
			deletedCount++
		}
	}

	// Verify all5 stale nodes were deleted
	if deletedCount != 5 {
		t.Errorf("Expected 5 nodes to be deleted, got %d", deletedCount)
	}

	// Verify only the fresh node remains
	nodes, err = store.ListNodeMetas(ctx)
	if err != nil {
		t.Fatalf("Failed to list nodes after GC: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node to remain, got %d", len(nodes))
	}
	if len(nodes) > 0 && nodes[0].NodeID != "fresh" {
		t.Errorf("Expected fresh node to remain, got %s", nodes[0].NodeID)
	}
}
