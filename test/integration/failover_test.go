//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/azrtydxb/novastor/internal/agent/failover"
	"github.com/azrtydxb/novastor/internal/metadata"
)

// --------------------------------------------------------------------------
// Mock SPDK client
// --------------------------------------------------------------------------

type spdkCall struct {
	NQN        string
	ANAGroupID uint32
	State      string
}

type mockSPDK struct {
	mu    sync.Mutex
	calls []spdkCall
}

func (m *mockSPDK) SetANAState(nqn string, anaGroupID uint32, state string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, spdkCall{NQN: nqn, ANAGroupID: anaGroupID, State: state})
	return nil
}

func (m *mockSPDK) getCalls() []spdkCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]spdkCall, len(m.calls))
	copy(out, m.calls)
	return out
}

func (m *mockSPDK) lastState() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return ""
	}
	return m.calls[len(m.calls)-1].State
}

// --------------------------------------------------------------------------
// Test
// --------------------------------------------------------------------------

func TestFailoverLifecycle(t *testing.T) {
	// ---------------------------------------------------------------
	// Step 1: Set up real Raft metadata store + gRPC server/client.
	// ---------------------------------------------------------------
	store, metaClient := setupMetadataService(t)
	ctx := context.Background()
	logger := zaptest.NewLogger(t)

	// ---------------------------------------------------------------
	// Step 2: Register 3 agent nodes with metadata.
	// ---------------------------------------------------------------
	nodes := []*metadata.NodeMeta{
		{
			NodeID:            "node-1",
			Address:           "10.0.0.1:9100",
			DiskCount:         4,
			TotalCapacity:     1_000_000_000_000,
			AvailableCapacity: 800_000_000_000,
			LastHeartbeat:     time.Now().Unix(),
			Status:            "ready",
		},
		{
			NodeID:            "node-2",
			Address:           "10.0.0.2:9100",
			DiskCount:         4,
			TotalCapacity:     1_000_000_000_000,
			AvailableCapacity: 800_000_000_000,
			LastHeartbeat:     time.Now().Unix(),
			Status:            "ready",
		},
		{
			NodeID:            "node-3",
			Address:           "10.0.0.3:9100",
			DiskCount:         4,
			TotalCapacity:     1_000_000_000_000,
			AvailableCapacity: 800_000_000_000,
			LastHeartbeat:     time.Now().Unix(),
			Status:            "ready",
		},
	}
	for _, n := range nodes {
		if err := metaClient.PutNodeMeta(ctx, n); err != nil {
			t.Fatalf("PutNodeMeta(%s) failed: %v", n.NodeID, err)
		}
	}

	// ---------------------------------------------------------------
	// Step 3: Create a volume with a placement map pointing to all 3 nodes.
	// ---------------------------------------------------------------
	volumeID := "vol-failover-001"
	replicaNodes := []string{"10.0.0.1:9100", "10.0.0.2:9100", "10.0.0.3:9100"}

	vol := &metadata.VolumeMeta{
		VolumeID:  volumeID,
		Pool:      "default-pool",
		SizeBytes: 10 * 1024 * 1024 * 1024, // 10 GiB
		ChunkIDs:  []string{"chunk-001", "chunk-002"},
	}
	if err := metaClient.PutVolumeMeta(ctx, vol); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	// Store placement maps for the volume's chunks.
	for _, cid := range vol.ChunkIDs {
		pm := &metadata.PlacementMap{
			ChunkID: cid,
			Nodes:   []string{"node-1", "node-2", "node-3"},
		}
		if err := metaClient.PutPlacementMap(ctx, pm); err != nil {
			t.Fatalf("PutPlacementMap(%s) failed: %v", cid, err)
		}
	}

	// ---------------------------------------------------------------
	// Step 4: Set initial volume ownership to node-1.
	// ---------------------------------------------------------------
	if err := store.SetVolumeOwner(&metadata.VolumeOwnership{
		VolumeID:   volumeID,
		OwnerAddr:  "10.0.0.1:9100",
		OwnerSince: time.Now().Unix(),
		Generation: 1,
	}); err != nil {
		t.Fatalf("SetVolumeOwner failed: %v", err)
	}

	// ---------------------------------------------------------------
	// Step 5: Create failover controllers for node-1 (owner) and
	//         node-2 (standby) with mock SPDK clients.
	// ---------------------------------------------------------------
	spdk1 := &mockSPDK{}
	spdk2 := &mockSPDK{}

	ctrl1 := failover.New("node-1", "10.0.0.1:9100", "10.0.0.1", metaClient, spdk1, logger)
	ctrl2 := failover.New("node-2", "10.0.0.2:9100", "10.0.0.2", metaClient, spdk2, logger)

	ctrl1.RegisterVolume(volumeID, replicaNodes, true)
	ctrl2.RegisterVolume(volumeID, replicaNodes, false)

	// ---------------------------------------------------------------
	// Step 6: Start both controllers.
	// ---------------------------------------------------------------
	ctxCtrl, cancelCtrl := context.WithCancel(ctx)
	defer cancelCtrl()

	if err := ctrl1.Start(ctxCtrl); err != nil {
		t.Fatalf("ctrl1.Start failed: %v", err)
	}
	defer ctrl1.Stop()

	if err := ctrl2.Start(ctxCtrl); err != nil {
		t.Fatalf("ctrl2.Start failed: %v", err)
	}
	defer ctrl2.Stop()

	// ---------------------------------------------------------------
	// Step 7: Verify initial ANA states.
	//   node-1 (owner)   -> optimized
	//   node-2 (standby) -> non_optimized
	// ---------------------------------------------------------------
	// Allow time for Start() to sync initial states.
	time.Sleep(1 * time.Second)

	if got := spdk1.lastState(); got != "optimized" {
		t.Fatalf("step 7: expected node-1 initial ANA state 'optimized', got %q (calls: %+v)", got, spdk1.getCalls())
	}
	if got := spdk2.lastState(); got != "non_optimized" {
		t.Fatalf("step 7: expected node-2 initial ANA state 'non_optimized', got %q (calls: %+v)", got, spdk2.getCalls())
	}
	t.Log("step 7 passed: initial ANA states verified")

	// ---------------------------------------------------------------
	// Step 8: Simulate node-1 death.
	//   Set node-1's heartbeat far in the past and status to "offline".
	// ---------------------------------------------------------------
	deadNode := &metadata.NodeMeta{
		NodeID:            "node-1",
		Address:           "10.0.0.1:9100",
		DiskCount:         4,
		TotalCapacity:     1_000_000_000_000,
		AvailableCapacity: 800_000_000_000,
		LastHeartbeat:     time.Now().Add(-30 * time.Second).Unix(), // stale
		Status:            "offline",
	}
	if err := store.PutNodeMeta(ctx, deadNode); err != nil {
		t.Fatalf("PutNodeMeta (mark node-1 dead) failed: %v", err)
	}
	t.Log("step 8 passed: node-1 marked as dead")

	// ---------------------------------------------------------------
	// Step 9: Node-2 requests ownership via the metadata client.
	// ---------------------------------------------------------------
	granted, newGen, err := metaClient.RequestOwnership(ctx, volumeID, "10.0.0.2:9100")
	if err != nil {
		t.Fatalf("RequestOwnership failed: %v", err)
	}
	if !granted {
		t.Fatal("step 9: expected ownership request to be granted, but it was denied")
	}
	if newGen < 2 {
		t.Fatalf("step 9: expected generation >= 2, got %d", newGen)
	}
	t.Logf("step 9 passed: ownership granted to node-2, generation=%d", newGen)

	// ---------------------------------------------------------------
	// Step 10: Verify node-2's controller promotes to "optimized".
	//   Wait for the watch loop to observe the ownership change.
	// ---------------------------------------------------------------
	time.Sleep(2 * time.Second)

	if got := spdk2.lastState(); got != "optimized" {
		t.Fatalf("step 10: expected node-2 ANA state 'optimized' after failover, got %q (calls: %+v)", got, spdk2.getCalls())
	}
	t.Log("step 10 passed: node-2 promoted to optimized")

	// Also verify node-1 demoted to non_optimized.
	if got := spdk1.lastState(); got != "non_optimized" {
		t.Fatalf("step 10: expected node-1 ANA state 'non_optimized' after losing ownership, got %q (calls: %+v)", got, spdk1.getCalls())
	}
	t.Log("step 10 (extra): node-1 correctly demoted to non_optimized")

	// ---------------------------------------------------------------
	// Step 11: Node-1 recovers — update heartbeat to now, set new
	//          ownership back to node-1.
	// ---------------------------------------------------------------
	recoveredNode := &metadata.NodeMeta{
		NodeID:            "node-1",
		Address:           "10.0.0.1:9100",
		DiskCount:         4,
		TotalCapacity:     1_000_000_000_000,
		AvailableCapacity: 800_000_000_000,
		LastHeartbeat:     time.Now().Unix(),
		Status:            "ready",
	}
	if err := store.PutNodeMeta(ctx, recoveredNode); err != nil {
		t.Fatalf("PutNodeMeta (recover node-1) failed: %v", err)
	}

	// Set ownership back to node-1 (simulating an operator or recovery action).
	if err := store.SetVolumeOwner(&metadata.VolumeOwnership{
		VolumeID:   volumeID,
		OwnerAddr:  "10.0.0.1:9100",
		OwnerSince: time.Now().Unix(),
		Generation: newGen + 1,
	}); err != nil {
		t.Fatalf("SetVolumeOwner (recovery) failed: %v", err)
	}
	t.Log("step 11 passed: node-1 recovered, ownership restored")

	// ---------------------------------------------------------------
	// Step 12: Verify node-1 promotes back, node-2 demotes.
	// ---------------------------------------------------------------
	time.Sleep(2 * time.Second)

	if got := spdk1.lastState(); got != "optimized" {
		t.Fatalf("step 12: expected node-1 ANA state 'optimized' after recovery, got %q (calls: %+v)", got, spdk1.getCalls())
	}
	if got := spdk2.lastState(); got != "non_optimized" {
		t.Fatalf("step 12: expected node-2 ANA state 'non_optimized' after node-1 recovery, got %q (calls: %+v)", got, spdk2.getCalls())
	}
	t.Log("step 12 passed: node-1 promoted, node-2 demoted")

	// ---------------------------------------------------------------
	// Step 13: Stop both controllers (deferred above).
	// ---------------------------------------------------------------
	t.Log("TestFailoverLifecycle: all steps passed")
}
