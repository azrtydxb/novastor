package metadata

import (
	"context"
	"os"
	"testing"
	"time"
)

func setupTestStore(t *testing.T) (*RaftStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "novastor-meta-test-*")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewRaftStore(RaftConfig{
		NodeID:   "test-node",
		DataDir:  dir,
		RaftAddr: "127.0.0.1:0",
		Backend:  "memory",
	})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewRaftStore failed: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsLeader() {
		store.Close()
		os.RemoveAll(dir)
		t.Fatal("node did not become leader")
	}
	return store, func() {
		store.Close()
		os.RemoveAll(dir)
	}
}

func TestRaftStore_PutGetVolumeMeta(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	meta := &VolumeMeta{
		VolumeID:  "vol-1",
		Pool:      "fast-pool",
		SizeBytes: 1024 * 1024 * 1024,
		ChunkIDs:  []string{"chunk-a", "chunk-b"},
	}
	if err := store.PutVolumeMeta(ctx, meta); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}
	got, err := store.GetVolumeMeta(ctx, "vol-1")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}
	if got.VolumeID != "vol-1" || got.Pool != "fast-pool" || len(got.ChunkIDs) != 2 {
		t.Errorf("unexpected volume meta: %+v", got)
	}
}

func TestRaftStore_GetVolumeMetaNotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	_, err := store.GetVolumeMeta(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent volume")
	}
}

func TestRaftStore_DeleteVolumeMeta(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	meta := &VolumeMeta{VolumeID: "vol-del", Pool: "pool"}
	_ = store.PutVolumeMeta(ctx, meta)
	if err := store.DeleteVolumeMeta(ctx, "vol-del"); err != nil {
		t.Fatalf("DeleteVolumeMeta failed: %v", err)
	}
	_, err := store.GetVolumeMeta(ctx, "vol-del")
	if err == nil {
		t.Error("volume should not exist after delete")
	}
}

func TestRaftStore_PutGetPlacementMap(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	pm := &PlacementMap{
		ChunkID: "chunk-1",
		Nodes:   []string{"node-a", "node-b", "node-c"},
	}
	if err := store.PutPlacementMap(ctx, pm); err != nil {
		t.Fatalf("PutPlacementMap failed: %v", err)
	}
	got, err := store.GetPlacementMap(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("GetPlacementMap failed: %v", err)
	}
	if len(got.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(got.Nodes))
	}
}

func TestRaftStore_VolumeMetaWithProtection(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	meta := &VolumeMeta{
		VolumeID:  "vol-protected",
		Pool:      "fast-pool",
		SizeBytes: 1024 * 1024 * 1024,
		ChunkIDs:  []string{"chunk-a", "chunk-b"},
		ProtectionProfile: &ProtectionProfile{
			Mode: ProtectionModeReplication,
			Replication: &ReplicationProfile{
				Factor:      3,
				WriteQuorum: 2,
			},
		},
		ComplianceInfo: &ComplianceInfo{
			State:             ComplianceStateCompliant,
			AvailableReplicas: 3,
			RequiredReplicas:  2,
			Reason:            "All replicas available",
		},
	}

	if err := store.PutVolumeMeta(ctx, meta); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	got, err := store.GetVolumeMeta(ctx, "vol-protected")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}

	if got.ProtectionProfile == nil {
		t.Fatal("expected protection profile to be persisted")
	}
	if got.ProtectionProfile.Mode != ProtectionModeReplication {
		t.Errorf("expected replication mode, got %v", got.ProtectionProfile.Mode)
	}
	if got.ProtectionProfile.Replication.Factor != 3 {
		t.Errorf("expected factor 3, got %d", got.ProtectionProfile.Replication.Factor)
	}
	if got.ComplianceInfo == nil {
		t.Fatal("expected compliance info to be persisted")
	}
	if got.ComplianceInfo.State != ComplianceStateCompliant {
		t.Errorf("expected compliant state, got %v", got.ComplianceInfo.State)
	}
}

func TestRaftStore_VolumeMetaWithErasureCoding(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	meta := &VolumeMeta{
		VolumeID:  "vol-ec",
		Pool:      "capacity-pool",
		SizeBytes: 2 * 1024 * 1024 * 1024,
		ChunkIDs:  []string{"chunk-1", "chunk-2", "chunk-3"},
		ProtectionProfile: &ProtectionProfile{
			Mode: ProtectionModeErasureCoding,
			ErasureCoding: &ErasureCodingProfile{
				DataShards:   4,
				ParityShards: 2,
			},
		},
	}

	if err := store.PutVolumeMeta(ctx, meta); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	got, err := store.GetVolumeMeta(ctx, "vol-ec")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}

	if got.ProtectionProfile.Mode != ProtectionModeErasureCoding {
		t.Errorf("expected erasure coding mode, got %v", got.ProtectionProfile.Mode)
	}
	if got.ProtectionProfile.ErasureCoding.DataShards != 4 {
		t.Errorf("expected 4 data shards, got %d", got.ProtectionProfile.ErasureCoding.DataShards)
	}
	if got.ProtectionProfile.ErasureCoding.ParityShards != 2 {
		t.Errorf("expected 2 parity shards, got %d", got.ProtectionProfile.ErasureCoding.ParityShards)
	}
}

func TestRaftStore_ListVolumesWithProtection(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// Create volumes with different protection profiles
	volumes := []*VolumeMeta{
		{
			VolumeID: "vol-repl",
			Pool:     "default",
			ChunkIDs: []string{"chunk-1"},
			ProtectionProfile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 3,
				},
			},
		},
		{
			VolumeID: "vol-ec",
			Pool:     "default",
			ChunkIDs: []string{"chunk-2"},
			ProtectionProfile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
		},
		{
			VolumeID: "vol-none",
			Pool:     "default",
			ChunkIDs: []string{"chunk-3"},
			// No protection profile
		},
	}

	for _, v := range volumes {
		if err := store.PutVolumeMeta(ctx, v); err != nil {
			t.Fatalf("PutVolumeMeta failed: %v", err)
		}
	}

	listed, err := store.ListVolumesMeta(ctx)
	if err != nil {
		t.Fatalf("ListVolumesMeta failed: %v", err)
	}

	if len(listed) != 3 {
		t.Errorf("expected 3 volumes, got %d", len(listed))
	}

	// Verify each volume's protection profile was persisted
	for _, v := range listed {
		switch v.VolumeID {
		case "vol-repl":
			if v.ProtectionProfile == nil || v.ProtectionProfile.Mode != ProtectionModeReplication {
				t.Errorf("vol-repl: expected replication mode")
			}
		case "vol-ec":
			if v.ProtectionProfile == nil || v.ProtectionProfile.Mode != ProtectionModeErasureCoding {
				t.Errorf("vol-ec: expected erasure coding mode")
			}
		case "vol-none":
			if v.ProtectionProfile != nil {
				t.Errorf("vol-none: expected no protection profile, got %v", v.ProtectionProfile)
			}
		}
	}
}
