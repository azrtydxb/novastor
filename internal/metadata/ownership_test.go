package metadata

import (
	"context"
	"testing"
	"time"
)

func TestSetAndGetVolumeOwner(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	owner := &VolumeOwnership{
		VolumeID:   "vol-1",
		OwnerAddr:  "10.0.0.1:9100",
		OwnerSince: time.Now().Unix(),
		Generation: 1,
	}
	if err := store.SetVolumeOwner(owner); err != nil {
		t.Fatalf("SetVolumeOwner: %v", err)
	}

	got, err := store.GetVolumeOwner("vol-1")
	if err != nil {
		t.Fatalf("GetVolumeOwner: %v", err)
	}
	if got.OwnerAddr != "10.0.0.1:9100" {
		t.Fatalf("expected owner 10.0.0.1:9100, got %s", got.OwnerAddr)
	}
	if got.Generation != 1 {
		t.Fatalf("expected generation 1, got %d", got.Generation)
	}
}

func TestGetVolumeOwner_NotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	got, err := store.GetVolumeOwner("nonexistent")
	if err != nil {
		t.Fatalf("GetVolumeOwner: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for nonexistent volume, got %+v", got)
	}
}

func TestRequestOwnership_GrantsWhenOwnerDead(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Set initial owner
	if err := store.SetVolumeOwner(&VolumeOwnership{
		VolumeID: "vol-1", OwnerAddr: "10.0.0.1:9100", Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Mark owner as dead via stale heartbeat
	if err := store.PutNodeMeta(ctx, &NodeMeta{
		NodeID: "node-1", Address: "10.0.0.1:9100",
		LastHeartbeat: time.Now().Add(-10 * time.Second).Unix(),
		Status:        "offline",
	}); err != nil {
		t.Fatal(err)
	}

	// Mark requester as alive
	if err := store.PutNodeMeta(ctx, &NodeMeta{
		NodeID: "node-2", Address: "10.0.0.2:9100",
		LastHeartbeat: time.Now().Unix(),
		Status:        "ready",
	}); err != nil {
		t.Fatal(err)
	}

	granted, gen, err := store.RequestOwnership("vol-1", "10.0.0.2:9100")
	if err != nil {
		t.Fatalf("RequestOwnership: %v", err)
	}
	if !granted {
		t.Fatal("expected ownership granted")
	}
	if gen != 2 {
		t.Fatalf("expected generation 2, got %d", gen)
	}
}

func TestRequestOwnership_DeniedWhenOwnerAlive(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	if err := store.SetVolumeOwner(&VolumeOwnership{
		VolumeID: "vol-1", OwnerAddr: "10.0.0.1:9100", Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutNodeMeta(ctx, &NodeMeta{
		NodeID: "node-1", Address: "10.0.0.1:9100",
		LastHeartbeat: time.Now().Unix(), Status: "ready",
	}); err != nil {
		t.Fatal(err)
	}

	granted, _, err := store.RequestOwnership("vol-1", "10.0.0.2:9100")
	if err != nil {
		t.Fatalf("RequestOwnership: %v", err)
	}
	if granted {
		t.Fatal("expected ownership denied (current owner alive)")
	}
}

func TestRequestOwnership_GrantsWhenNoCurrentOwner(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	granted, gen, err := store.RequestOwnership("vol-new", "10.0.0.1:9100")
	if err != nil {
		t.Fatalf("RequestOwnership: %v", err)
	}
	if !granted {
		t.Fatal("expected ownership granted for new volume")
	}
	if gen != 1 {
		t.Fatalf("expected generation 1, got %d", gen)
	}
}
