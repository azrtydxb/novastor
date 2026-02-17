package operator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- Mock implementations ---

type mockPlacement struct {
	mu         sync.Mutex
	chunks     map[string][]string // nodeID -> chunkIDs
	replicas   map[string][]string // chunkID -> nodeIDs
	placements []placementUpdate
}

type placementUpdate struct {
	ChunkID string
	OldNode string
	NewNode string
}

func newMockPlacement() *mockPlacement {
	return &mockPlacement{
		chunks:   make(map[string][]string),
		replicas: make(map[string][]string),
	}
}

func (m *mockPlacement) ChunksOnNode(_ context.Context, nodeID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chunks[nodeID], nil
}

func (m *mockPlacement) ReplicaNodes(_ context.Context, chunkID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodes, ok := m.replicas[chunkID]
	if !ok {
		return nil, fmt.Errorf("chunk %s not found", chunkID)
	}
	return nodes, nil
}

func (m *mockPlacement) UpdatePlacement(_ context.Context, chunkID, oldNode, newNode string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.placements = append(m.placements, placementUpdate{chunkID, oldNode, newNode})
	return nil
}

type mockReplicator struct {
	mu          sync.Mutex
	calls       []replicateCall
	failChunkID string
}

type replicateCall struct {
	ChunkID    string
	SourceNode string
	DestNode   string
}

func (m *mockReplicator) ReplicateChunk(_ context.Context, chunkID, sourceNode, destNode string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, replicateCall{chunkID, sourceNode, destNode})
	if m.failChunkID == chunkID {
		return fmt.Errorf("replication failed for %s", chunkID)
	}
	return nil
}

type mockHealthChecker struct {
	healthy map[string]bool
}

func (m *mockHealthChecker) IsNodeHealthy(_ context.Context, nodeID string) bool {
	return m.healthy[nodeID]
}

// --- Tests ---

func TestHeartbeat(t *testing.T) {
	rm := NewRecoveryManager(newMockPlacement(), &mockReplicator{}, &mockHealthChecker{})

	rm.Heartbeat("node-1")

	rm.mu.Lock()
	info, ok := rm.nodes["node-1"]
	rm.mu.Unlock()

	if !ok {
		t.Fatal("expected node-1 to be tracked")
	}
	if info.Status != NodeHealthy {
		t.Errorf("expected NodeHealthy, got %d", info.Status)
	}
	if time.Since(info.LastSeen) > time.Second {
		t.Error("LastSeen should be recent")
	}
}

func TestCheckNodes_HealthyToSuspect(t *testing.T) {
	rm := NewRecoveryManager(newMockPlacement(), &mockReplicator{}, &mockHealthChecker{})

	rm.mu.Lock()
	rm.nodes["node-1"] = &NodeInfo{
		ID:       "node-1",
		LastSeen: time.Now().Add(-35 * time.Second),
		Status:   NodeHealthy,
	}
	rm.mu.Unlock()

	rm.CheckNodes(context.Background())

	rm.mu.Lock()
	status := rm.nodes["node-1"].Status
	rm.mu.Unlock()

	if status != NodeSuspect {
		t.Errorf("expected NodeSuspect, got %d", status)
	}
}

func TestCheckNodes_SuspectToDown(t *testing.T) {
	rm := NewRecoveryManager(newMockPlacement(), &mockReplicator{}, &mockHealthChecker{})

	rm.mu.Lock()
	rm.nodes["node-1"] = &NodeInfo{
		ID:       "node-1",
		LastSeen: time.Now().Add(-65 * time.Second),
		Status:   NodeSuspect,
	}
	rm.mu.Unlock()

	rm.CheckNodes(context.Background())

	rm.mu.Lock()
	status := rm.nodes["node-1"].Status
	rm.mu.Unlock()

	if status != NodeDown {
		t.Errorf("expected NodeDown, got %d", status)
	}
}

func TestRecoverNode(t *testing.T) {
	placement := newMockPlacement()
	placement.chunks["node-bad"] = []string{"chunk-1", "chunk-2", "chunk-3"}
	placement.replicas["chunk-1"] = []string{"node-bad", "node-a"}
	placement.replicas["chunk-2"] = []string{"node-bad", "node-b"}
	placement.replicas["chunk-3"] = []string{"node-bad", "node-a", "node-b"}

	replicator := &mockReplicator{}
	rm := NewRecoveryManager(placement, replicator, &mockHealthChecker{})

	// Register healthy nodes via heartbeat.
	rm.Heartbeat("node-a")
	rm.Heartbeat("node-b")
	rm.Heartbeat("node-c")

	// Mark the bad node as down.
	rm.mu.Lock()
	rm.nodes["node-bad"] = &NodeInfo{
		ID:       "node-bad",
		LastSeen: time.Now().Add(-120 * time.Second),
		Status:   NodeDown,
	}
	rm.mu.Unlock()

	err := rm.RecoverNode(context.Background(), "node-bad")
	if err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	pending := rm.PendingRecoveries()
	if pending != 3 {
		t.Errorf("expected 3 pending recoveries, got %d", pending)
	}
}

func TestProcessRecoveryQueue(t *testing.T) {
	placement := newMockPlacement()
	placement.chunks["node-bad"] = []string{"chunk-1", "chunk-2"}
	placement.replicas["chunk-1"] = []string{"node-bad", "node-a"}
	placement.replicas["chunk-2"] = []string{"node-bad", "node-b"}

	replicator := &mockReplicator{}
	rm := NewRecoveryManager(placement, replicator, &mockHealthChecker{})

	rm.Heartbeat("node-a")
	rm.Heartbeat("node-b")
	rm.Heartbeat("node-c")

	rm.mu.Lock()
	rm.nodes["node-bad"] = &NodeInfo{
		ID:       "node-bad",
		LastSeen: time.Now().Add(-120 * time.Second),
		Status:   NodeDown,
	}
	rm.mu.Unlock()

	if err := rm.RecoverNode(context.Background(), "node-bad"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	if err := rm.ProcessRecoveryQueue(context.Background()); err != nil {
		t.Fatalf("ProcessRecoveryQueue failed: %v", err)
	}

	if rm.PendingRecoveries() != 0 {
		t.Error("expected 0 pending recoveries after processing")
	}

	completed := rm.CompletedRecoveries()
	if completed != 2 {
		t.Errorf("expected 2 completed recoveries, got %d", completed)
	}

	replicator.mu.Lock()
	callCount := len(replicator.calls)
	replicator.mu.Unlock()
	if callCount != 2 {
		t.Errorf("expected 2 replication calls, got %d", callCount)
	}
}

func TestRecoveryPriority(t *testing.T) {
	placement := newMockPlacement()
	// chunk-many has 3 surviving replicas (priority=3), chunk-few has 1 (priority=1).
	placement.chunks["node-bad"] = []string{"chunk-many", "chunk-few"}
	placement.replicas["chunk-many"] = []string{"node-bad", "node-a", "node-b", "node-c"}
	placement.replicas["chunk-few"] = []string{"node-bad", "node-a"}

	replicator := &mockReplicator{}
	rm := NewRecoveryManager(placement, replicator, &mockHealthChecker{})

	rm.Heartbeat("node-a")
	rm.Heartbeat("node-b")
	rm.Heartbeat("node-c")
	rm.Heartbeat("node-d")

	rm.mu.Lock()
	rm.nodes["node-bad"] = &NodeInfo{
		ID:       "node-bad",
		LastSeen: time.Now().Add(-120 * time.Second),
		Status:   NodeDown,
	}
	rm.mu.Unlock()

	if err := rm.RecoverNode(context.Background(), "node-bad"); err != nil {
		t.Fatalf("RecoverNode failed: %v", err)
	}

	// After RecoverNode, tasks are sorted by priority ascending.
	rm.mu.Lock()
	if len(rm.pendingRecovery) != 2 {
		rm.mu.Unlock()
		t.Fatalf("expected 2 pending, got %d", len(rm.pendingRecovery))
	}
	first := rm.pendingRecovery[0]
	second := rm.pendingRecovery[1]
	rm.mu.Unlock()

	// chunk-few has 1 surviving replica, chunk-many has 3.
	if first.ChunkID != "chunk-few" {
		t.Errorf("expected chunk-few first (priority 1), got %s (priority %d)", first.ChunkID, first.Priority)
	}
	if second.ChunkID != "chunk-many" {
		t.Errorf("expected chunk-many second (priority 3), got %s (priority %d)", second.ChunkID, second.Priority)
	}
	if first.Priority >= second.Priority {
		t.Errorf("first priority (%d) should be less than second priority (%d)", first.Priority, second.Priority)
	}
}
