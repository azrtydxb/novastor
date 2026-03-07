package failover

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/azrtydxb/novastor/internal/metadata"
)

// --------------------------------------------------------------------------
// Mock implementations
// --------------------------------------------------------------------------

type setANACall struct {
	NQN        string
	ANAGroupID uint32
	State      string
}

type mockSPDKClient struct {
	mu    sync.Mutex
	calls []setANACall
	err   error
}

func (m *mockSPDKClient) SetANAState(nqn string, anaGroupID uint32, state string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, setANACall{NQN: nqn, ANAGroupID: anaGroupID, State: state})
	return m.err
}

func (m *mockSPDKClient) getCalls() []setANACall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]setANACall, len(m.calls))
	copy(out, m.calls)
	return out
}

type mockMetaClient struct {
	mu        sync.Mutex
	ownership *metadata.VolumeOwnership
	err       error

	// requestOwnership fields
	requestGranted    bool
	requestGeneration uint64
	requestErr        error
}

func (m *mockMetaClient) GetVolumeOwner(_ context.Context, _ string) (*metadata.VolumeOwnership, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ownership, m.err
}

func (m *mockMetaClient) RequestOwnership(_ context.Context, _, _ string) (bool, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requestGranted, m.requestGeneration, m.requestErr
}

func (m *mockMetaClient) setOwnership(o *metadata.VolumeOwnership) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ownership = o
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestController_PromotesToOwner(t *testing.T) {
	logger := zaptest.NewLogger(t)
	nodeAddr := "10.0.0.1:9100"
	volumeID := "vol-promote-001"

	mc := &mockMetaClient{
		ownership: &metadata.VolumeOwnership{
			VolumeID:   volumeID,
			OwnerAddr:  "10.0.0.2:9100", // initially owned by another node
			Generation: 1,
		},
	}
	sc := &mockSPDKClient{}

	c := New("node-1", nodeAddr, "10.0.0.1", mc, sc, logger)
	c.RegisterVolume(volumeID, []string{"10.0.0.1:9100", "10.0.0.2:9100"}, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer c.Stop()

	// Initial state should set non_optimized since we are not the owner.
	time.Sleep(100 * time.Millisecond)

	// Now change ownership to us.
	mc.setOwnership(&metadata.VolumeOwnership{
		VolumeID:   volumeID,
		OwnerAddr:  nodeAddr,
		Generation: 2,
	})

	// Wait for the watch loop to pick up the change.
	time.Sleep(2 * watchInterval)

	calls := sc.getCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one SetANAState call, got none")
	}

	// The last call should be the promote to optimized.
	lastCall := calls[len(calls)-1]
	expectedNQN := nqnPrefix + volumeID
	expectedGroupID := anaGroupForVolume(volumeID)

	if lastCall.NQN != expectedNQN {
		t.Errorf("expected NQN %q, got %q", expectedNQN, lastCall.NQN)
	}
	if lastCall.ANAGroupID != expectedGroupID {
		t.Errorf("expected ANA group ID %d, got %d", expectedGroupID, lastCall.ANAGroupID)
	}
	if lastCall.State != ANAOptimized {
		t.Errorf("expected state %q, got %q", ANAOptimized, lastCall.State)
	}

	// Verify local state was updated.
	c.mu.RLock()
	vs := c.volumes[volumeID]
	c.mu.RUnlock()
	if !vs.IsOwner {
		t.Error("expected volume to be marked as owner after promote")
	}
	if vs.ANAState != ANAOptimized {
		t.Errorf("expected ANA state %q, got %q", ANAOptimized, vs.ANAState)
	}
}

func TestController_DemotesOnRecovery(t *testing.T) {
	logger := zaptest.NewLogger(t)
	nodeAddr := "10.0.0.1:9100"
	volumeID := "vol-demote-001"

	mc := &mockMetaClient{
		ownership: &metadata.VolumeOwnership{
			VolumeID:   volumeID,
			OwnerAddr:  nodeAddr, // initially owned by us
			Generation: 1,
		},
	}
	sc := &mockSPDKClient{}

	c := New("node-1", nodeAddr, "10.0.0.1", mc, sc, logger)
	c.RegisterVolume(volumeID, []string{"10.0.0.1:9100", "10.0.0.2:9100"}, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer c.Stop()

	// Initial state should set optimized since we are the owner.
	time.Sleep(100 * time.Millisecond)

	// Now ownership moves to another node (e.g. after failover recovery).
	mc.setOwnership(&metadata.VolumeOwnership{
		VolumeID:   volumeID,
		OwnerAddr:  "10.0.0.2:9100",
		Generation: 2,
	})

	// Wait for the watch loop to detect the change.
	time.Sleep(2 * watchInterval)

	calls := sc.getCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one SetANAState call, got none")
	}

	// The last call should be the demote to non_optimized.
	lastCall := calls[len(calls)-1]
	expectedNQN := nqnPrefix + volumeID
	expectedGroupID := anaGroupForVolume(volumeID)

	if lastCall.NQN != expectedNQN {
		t.Errorf("expected NQN %q, got %q", expectedNQN, lastCall.NQN)
	}
	if lastCall.ANAGroupID != expectedGroupID {
		t.Errorf("expected ANA group ID %d, got %d", expectedGroupID, lastCall.ANAGroupID)
	}
	if lastCall.State != ANANonOptimized {
		t.Errorf("expected state %q, got %q", ANANonOptimized, lastCall.State)
	}

	// Verify local state was updated.
	c.mu.RLock()
	vs := c.volumes[volumeID]
	c.mu.RUnlock()
	if vs.IsOwner {
		t.Error("expected volume to not be marked as owner after demote")
	}
	if vs.ANAState != ANANonOptimized {
		t.Errorf("expected ANA state %q, got %q", ANANonOptimized, vs.ANAState)
	}
}

func TestController_SkipsIfNotInManagedSet(t *testing.T) {
	logger := zaptest.NewLogger(t)
	nodeAddr := "10.0.0.1:9100"

	// Metadata returns ownership for a volume, but we never register it.
	mc := &mockMetaClient{
		ownership: &metadata.VolumeOwnership{
			VolumeID:   "vol-unmanaged-001",
			OwnerAddr:  nodeAddr,
			Generation: 1,
		},
	}
	sc := &mockSPDKClient{}

	c := New("node-1", nodeAddr, "10.0.0.1", mc, sc, logger)
	// Deliberately do NOT register any volumes.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer c.Stop()

	// Wait a couple of watch intervals.
	time.Sleep(3 * watchInterval)

	calls := sc.getCalls()
	if len(calls) != 0 {
		t.Errorf("expected no SetANAState calls for unmanaged volumes, got %d", len(calls))
	}
}

func TestANAGroupForVolume(t *testing.T) {
	// Deterministic: same input always yields same output.
	id := "vol-test-deterministic"
	g1 := anaGroupForVolume(id)
	g2 := anaGroupForVolume(id)
	if g1 != g2 {
		t.Errorf("expected deterministic result, got %d and %d", g1, g2)
	}
	// Result must fit in a byte (0-255).
	if g1 > 0xFF {
		t.Errorf("expected ANA group ID <= 0xFF, got %d", g1)
	}
}
