package csi

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/piwi3910/novastor/internal/metadata"
)

// --- mock snapshot store ---

type mockSnapshotStore struct {
	mu        sync.Mutex
	volumes   map[string]*metadata.VolumeMeta
	snapshots map[string]*SnapshotMeta
}

func newMockSnapshotStore() *mockSnapshotStore {
	return &mockSnapshotStore{
		volumes:   make(map[string]*metadata.VolumeMeta),
		snapshots: make(map[string]*SnapshotMeta),
	}
}

func (m *mockSnapshotStore) PutVolumeMeta(_ context.Context, meta *metadata.VolumeMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volumes[meta.VolumeID] = meta
	return nil
}

func (m *mockSnapshotStore) GetVolumeMeta(_ context.Context, volumeID string) (*metadata.VolumeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vm, ok := m.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("volume %s not found", volumeID)
	}
	return vm, nil
}

func (m *mockSnapshotStore) DeleteVolumeMeta(_ context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.volumes, volumeID)
	return nil
}

func (m *mockSnapshotStore) ListVolumesMeta(_ context.Context) ([]*metadata.VolumeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*metadata.VolumeMeta, 0, len(m.volumes))
	for _, v := range m.volumes {
		result = append(result, v)
	}
	return result, nil
}

func (m *mockSnapshotStore) PutPlacementMap(_ context.Context, _ *metadata.PlacementMap) error {
	// No-op for tests.
	return nil
}

func (m *mockSnapshotStore) GetPlacementMap(_ context.Context, _ string) (*metadata.PlacementMap, error) {
	// No-op for tests.
	return nil, fmt.Errorf("placement map not found")
}

func (m *mockSnapshotStore) DeletePlacementMap(_ context.Context, _ string) error {
	// No-op for tests.
	return nil
}

func (m *mockSnapshotStore) PutSnapshotMeta(_ context.Context, meta *SnapshotMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots[meta.SnapshotID] = meta
	return nil
}

func (m *mockSnapshotStore) GetSnapshotMeta(_ context.Context, snapshotID string) (*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sm, ok := m.snapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("snapshot %s not found", snapshotID)
	}
	return sm, nil
}

func (m *mockSnapshotStore) DeleteSnapshotMeta(_ context.Context, snapshotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.snapshots, snapshotID)
	return nil
}

func (m *mockSnapshotStore) ListSnapshotMetas(_ context.Context) ([]*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*SnapshotMeta, 0, len(m.snapshots))
	for _, sm := range m.snapshots {
		result = append(result, sm)
	}
	return result, nil
}

// --- helpers ---

func setupSnapshotController() (*SnapshotController, *mockSnapshotStore) {
	store := newMockSnapshotStore()
	return NewSnapshotController(store), store
}

// seedVolume inserts a volume directly into the mock store for snapshot tests.
func seedVolume(store *mockSnapshotStore, volumeID string, sizeBytes uint64, chunkIDs []string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.volumes[volumeID] = &metadata.VolumeMeta{
		VolumeID:  volumeID,
		SizeBytes: sizeBytes,
		ChunkIDs:  chunkIDs,
	}
}

// --- CreateSnapshot tests ---

func TestCreateSnapshot_Success(t *testing.T) {
	sc, store := setupSnapshotController()

	seedVolume(store, "vol-1", 8*1024*1024, []string{"vol-1-chunk-0000", "vol-1-chunk-0001"})

	resp, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-1",
		SourceVolumeId: "vol-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	snap := resp.GetSnapshot()
	if snap.GetSnapshotId() == "" {
		t.Fatal("expected non-empty snapshot ID")
	}
	if snap.GetSourceVolumeId() != "vol-1" {
		t.Errorf("expected source volume vol-1, got %s", snap.GetSourceVolumeId())
	}
	if snap.GetSizeBytes() != 8*1024*1024 {
		t.Errorf("expected size 8MiB, got %d", snap.GetSizeBytes())
	}
	if !snap.GetReadyToUse() {
		t.Error("expected snapshot to be ready to use")
	}
	if snap.GetCreationTime() == nil {
		t.Error("expected non-nil creation time")
	}

	// Verify stored in mock.
	stored, err := store.GetSnapshotMeta(context.Background(), snap.GetSnapshotId())
	if err != nil {
		t.Fatalf("snapshot not found in store: %v", err)
	}
	if len(stored.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunk IDs, got %d", len(stored.ChunkIDs))
	}
}

func TestCreateSnapshot_NoName(t *testing.T) {
	sc, _ := setupSnapshotController()

	_, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: "vol-1",
	})
	if err == nil {
		t.Fatal("expected error for missing snapshot name")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateSnapshot_NoSourceVolume(t *testing.T) {
	sc, _ := setupSnapshotController()

	_, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snap-1",
	})
	if err == nil {
		t.Fatal("expected error for missing source volume ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateSnapshot_SourceVolumeNotFound(t *testing.T) {
	sc, _ := setupSnapshotController()

	_, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-1",
		SourceVolumeId: "nonexistent-vol",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent source volume")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

// --- DeleteSnapshot tests ---

func TestDeleteSnapshot_Success(t *testing.T) {
	sc, store := setupSnapshotController()

	seedVolume(store, "vol-1", 8*1024*1024, []string{"vol-1-chunk-0000", "vol-1-chunk-0001"})

	// Create a snapshot first.
	createResp, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-del",
		SourceVolumeId: "vol-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	snapID := createResp.GetSnapshot().GetSnapshotId()

	_, err = sc.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: snapID,
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot failed: %v", err)
	}

	// Verify removed from store.
	if _, err := store.GetSnapshotMeta(context.Background(), snapID); err == nil {
		t.Error("expected snapshot metadata to be deleted")
	}
}

func TestDeleteSnapshot_NotFound(t *testing.T) {
	sc, _ := setupSnapshotController()

	// Deleting a non-existent snapshot should succeed per CSI spec.
	_, err := sc.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: "does-not-exist",
	})
	if err != nil {
		t.Fatalf("expected idempotent delete to succeed, got: %v", err)
	}
}

// --- ListSnapshots tests ---

func TestListSnapshots_All(t *testing.T) {
	sc, store := setupSnapshotController()

	seedVolume(store, "vol-1", 4*1024*1024, []string{"c1"})
	seedVolume(store, "vol-2", 8*1024*1024, []string{"c2", "c3"})

	// Create two snapshots from different volumes.
	_, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-a",
		SourceVolumeId: "vol-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot snap-a failed: %v", err)
	}

	_, err = sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-b",
		SourceVolumeId: "vol-2",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot snap-b failed: %v", err)
	}

	resp, err := sc.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}

	if len(resp.GetEntries()) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(resp.GetEntries()))
	}
}

func TestListSnapshots_FilterByVolume(t *testing.T) {
	sc, store := setupSnapshotController()

	seedVolume(store, "vol-1", 4*1024*1024, []string{"c1"})
	seedVolume(store, "vol-2", 8*1024*1024, []string{"c2", "c3"})

	_, err := sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-a",
		SourceVolumeId: "vol-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot snap-a failed: %v", err)
	}

	_, err = sc.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snap-b",
		SourceVolumeId: "vol-2",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot snap-b failed: %v", err)
	}

	resp, err := sc.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "vol-1",
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}

	if len(resp.GetEntries()) != 1 {
		t.Fatalf("expected 1 snapshot for vol-1, got %d", len(resp.GetEntries()))
	}
	if resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId() != "vol-1" {
		t.Errorf("expected source volume vol-1, got %s", resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId())
	}
}
