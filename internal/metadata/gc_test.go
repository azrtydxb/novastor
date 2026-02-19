package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestGarbageCollector_RunOnce(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create test data: volumes, snapshots, and placement maps.
	volume1 := &VolumeMeta{
		VolumeID:  "vol-1",
		Pool:      "pool-a",
		SizeBytes: 1024,
		ChunkIDs:  []string{"chunk-1", "chunk-2"},
	}
	volume2 := &VolumeMeta{
		VolumeID:  "vol-2",
		Pool:      "pool-a",
		SizeBytes: 2048,
		ChunkIDs:  []string{"chunk-3"},
	}
	snapshot1 := &SnapshotMeta{
		SnapshotID:     "snap-1",
		SourceVolumeID: "vol-1",
		SizeBytes:      1024,
		ChunkIDs:       []string{"chunk-1", "chunk-2"},
		ReadyToUse:     true,
	}

	// Add placement maps for all chunks (including orphan).
	placements := []*PlacementMap{
		{ChunkID: "chunk-1", Nodes: []string{"node-a", "node-b", "node-c"}},
		{ChunkID: "chunk-2", Nodes: []string{"node-a", "node-b", "node-c"}},
		{ChunkID: "chunk-3", Nodes: []string{"node-a", "node-b", "node-c"}},
		{ChunkID: "chunk-orphan", Nodes: []string{"node-a", "node-b", "node-c"}}, // Not referenced by any metadata
	}

	for _, pm := range placements {
		if err := store.PutPlacementMap(ctx, pm); err != nil {
			t.Fatalf("failed to put placement map: %v", err)
		}
	}

	for _, vol := range []*VolumeMeta{volume1, volume2} {
		if err := store.PutVolumeMeta(ctx, vol); err != nil {
			t.Fatalf("failed to put volume meta: %v", err)
		}
	}

	if err := store.PutSnapshot(ctx, snapshot1); err != nil {
		t.Fatalf("failed to put snapshot: %v", err)
	}

	// Run GC.
	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify that orphan placement was deleted.
	if result.OrphanPlacementsDeleted != 1 {
		t.Errorf("expected 1 orphan placement deleted, got %d", result.OrphanPlacementsDeleted)
	}

	// Verify that the orphan placement no longer exists.
	_, err = store.GetPlacementMap(ctx, "chunk-orphan")
	if err == nil {
		t.Error("orphan placement map should have been deleted")
	}

	// Verify that referenced placements still exist.
	for _, chunkID := range []string{"chunk-1", "chunk-2", "chunk-3"} {
		_, err := store.GetPlacementMap(ctx, chunkID)
		if err != nil {
			t.Errorf("referenced placement for %s should still exist: %v", chunkID, err)
		}
	}
}

func TestGarbageCollector_DeleteStaleNodeMetadata(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().Unix()

	// Create test nodes: one fresh, one stale.
	freshNode := &NodeMeta{
		NodeID:            "node-fresh",
		Address:           "host1:9100",
		DiskCount:         1,
		TotalCapacity:     1000000000,
		AvailableCapacity: 500000000,
		LastHeartbeat:     now,
		Status:            "ready",
	}
	staleNode := &NodeMeta{
		NodeID:            "node-stale",
		Address:           "host2:9100",
		DiskCount:         1,
		TotalCapacity:     1000000000,
		AvailableCapacity: 500000000,
		LastHeartbeat:     now - int64((48 * time.Hour).Seconds()), // 48 hours ago
		Status:            "ready",
	}
	offlineNode := &NodeMeta{
		NodeID:            "node-offline",
		Address:           "host3:9100",
		DiskCount:         1,
		TotalCapacity:     1000000000,
		AvailableCapacity: 500000000,
		LastHeartbeat:     now - int64((1 * time.Hour).Seconds()), // Recently went offline
		Status:            "offline",
	}
	offlineStaleNode := &NodeMeta{
		NodeID:            "node-offline-stale",
		Address:           "host4:9100",
		DiskCount:         1,
		TotalCapacity:     1000000000,
		AvailableCapacity: 500000000,
		LastHeartbeat:     now - int64((10 * 24 * time.Hour).Seconds()), // 10 days ago, offline
		Status:            "offline",
	}

	for _, node := range []*NodeMeta{freshNode, staleNode, offlineNode, offlineStaleNode} {
		if err := store.PutNodeMeta(ctx, node); err != nil {
			t.Fatalf("failed to put node meta: %v", err)
		}
	}

	// Run GC with 24h TTL.
	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify that stale node was deleted (offline stale node with 10-day heartbeat should be deleted).
	if result.StaleNodesDeleted != 2 {
		t.Errorf("expected 2 stale nodes deleted, got %d", result.StaleNodesDeleted)
	}

	// Verify that fresh nodes still exist.
	_, err = store.GetNodeMeta(ctx, "node-fresh")
	if err != nil {
		t.Error("fresh node should still exist")
	}
	_, err = store.GetNodeMeta(ctx, "node-offline")
	if err != nil {
		t.Error("recently offline node should still exist")
	}

	// Verify that stale nodes were deleted.
	_, err = store.GetNodeMeta(ctx, "node-stale")
	if err == nil {
		t.Error("stale node should have been deleted")
	}
	_, err = store.GetNodeMeta(ctx, "node-offline-stale")
	if err == nil {
		t.Error("stale offline node should have been deleted")
	}
}

func TestGarbageCollector_BuildReferencedChunkSet(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create diverse metadata with chunks.
	volume := &VolumeMeta{
		VolumeID:  "vol-1",
		Pool:      "pool-a",
		SizeBytes: 1024,
		ChunkIDs:  []string{"chunk-vol-1", "chunk-vol-2"},
	}
	snapshot := &SnapshotMeta{
		SnapshotID:     "snap-1",
		SourceVolumeID: "vol-1",
		SizeBytes:      1024,
		ChunkIDs:       []string{"chunk-snap-1"},
		ReadyToUse:     true,
	}
	obj := &ObjectMeta{
		Bucket:   "bucket-1",
		Key:      "object-1",
		Size:     2048,
		ChunkIDs: []string{"chunk-obj-1"},
	}
	inode := &InodeMeta{
		Ino:      1,
		Type:     InodeTypeFile,
		Size:     512,
		Mode:     0644,
		ChunkIDs: []string{"chunk-file-1"},
	}

	if err := store.PutVolumeMeta(ctx, volume); err != nil {
		t.Fatalf("failed to put volume: %v", err)
	}
	if err := store.PutSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("failed to put snapshot: %v", err)
	}
	if err := store.PutObjectMeta(ctx, obj); err != nil {
		t.Fatalf("failed to put object: %v", err)
	}
	if err := store.CreateInode(ctx, inode); err != nil {
		t.Fatalf("failed to create inode: %v", err)
	}

	// Build referenced chunk set.
	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)
	referenced, err := gc.buildReferencedChunkSet(ctx)
	if err != nil {
		t.Fatalf("buildReferencedChunkSet failed: %v", err)
	}

	// Verify all expected chunks are referenced.
	expectedChunks := []string{
		"chunk-vol-1", "chunk-vol-2",
		"chunk-snap-1",
		"chunk-obj-1",
		"chunk-file-1",
	}
	for _, chunkID := range expectedChunks {
		if _, ok := referenced[chunkID]; !ok {
			t.Errorf("expected chunk %s to be in referenced set", chunkID)
		}
	}

	// Verify an unexpected chunk is not referenced.
	if _, ok := referenced["chunk-not-referenced"]; ok {
		t.Error("unexpected chunk should not be in referenced set")
	}
}

func TestGarbageCollector_NoOrphanPlacements(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create a volume and its placement maps - all chunks referenced.
	volume := &VolumeMeta{
		VolumeID:  "vol-1",
		Pool:      "pool-a",
		SizeBytes: 1024,
		ChunkIDs:  []string{"chunk-1", "chunk-2"},
	}
	if err := store.PutVolumeMeta(ctx, volume); err != nil {
		t.Fatalf("failed to put volume: %v", err)
	}

	for _, chunkID := range []string{"chunk-1", "chunk-2"} {
		pm := &PlacementMap{
			ChunkID: chunkID,
			Nodes:   []string{"node-a", "node-b", "node-c"},
		}
		if err := store.PutPlacementMap(ctx, pm); err != nil {
			t.Fatalf("failed to put placement: %v", err)
		}
	}

	// Run GC.
	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify no placements were deleted.
	if result.OrphanPlacementsDeleted != 0 {
		t.Errorf("expected 0 orphan placements deleted, got %d", result.OrphanPlacementsDeleted)
	}

	// Verify all placements still exist.
	for _, chunkID := range []string{"chunk-1", "chunk-2"} {
		_, err := store.GetPlacementMap(ctx, chunkID)
		if err != nil {
			t.Errorf("placement for %s should still exist: %v", chunkID, err)
		}
	}
}

func TestGarbageCollector_MultipartUploadChunks(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create a multipart upload with referenced chunks.
	mu := &MultipartUpload{
		UploadID:     "upload-1",
		Bucket:       "bucket-1",
		Key:          "object-1",
		CreationDate: time.Now().UnixNano(),
		Parts: []MultipartPart{
			{
				PartNumber: 1,
				Size:       1024,
				ETag:       "etag-1",
				ChunkIDs:   []string{"chunk-part-1"},
			},
			{
				PartNumber: 2,
				Size:       1024,
				ETag:       "etag-2",
				ChunkIDs:   []string{"chunk-part-2"},
			},
		},
	}

	if err := store.PutMultipartUpload(ctx, mu); err != nil {
		t.Fatalf("failed to put multipart upload: %v", err)
	}

	// Add placement maps including an orphan.
	for _, chunkID := range []string{"chunk-part-1", "chunk-part-2", "chunk-orphan"} {
		pm := &PlacementMap{
			ChunkID: chunkID,
			Nodes:   []string{"node-a", "node-b"},
		}
		if err := store.PutPlacementMap(ctx, pm); err != nil {
			t.Fatalf("failed to put placement: %v", err)
		}
	}

	// Run GC.
	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify only the orphan placement was deleted.
	if result.OrphanPlacementsDeleted != 1 {
		t.Errorf("expected 1 orphan placement deleted, got %d", result.OrphanPlacementsDeleted)
	}

	// Verify multipart chunks still have placements.
	for _, chunkID := range []string{"chunk-part-1", "chunk-part-2"} {
		_, err := store.GetPlacementMap(ctx, chunkID)
		if err != nil {
			t.Errorf("placement for multipart chunk %s should still exist: %v", chunkID, err)
		}
	}

	// Verify orphan placement was deleted.
	_, err = store.GetPlacementMap(ctx, "chunk-orphan")
	if err == nil {
		t.Error("orphan placement should have been deleted")
	}
}

func TestGarbageCollector_LastResult(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)

	// Initially, no last result.
	if result := gc.LastResult(); result != nil {
		t.Error("expected no last result initially, got non-nil")
	}

	// Run GC and verify result is stored.
	result, err := gc.RunOnce(ctx)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	lastResult := gc.LastResult()
	if lastResult == nil {
		t.Error("expected last result to be non-nil after GC run")
	} else if lastResult.OrphanPlacementsDeleted != result.OrphanPlacementsDeleted {
		t.Errorf("last result mismatch: got %d, want %d",
			lastResult.OrphanPlacementsDeleted, result.OrphanPlacementsDeleted)
	}
}

// BenchmarkGarbageCollector measures the performance of GC operations.
func BenchmarkGarbageCollector(b *testing.B) {
	store, cleanup := setupTestStore(&testing.T{})
	defer cleanup()

	ctx := context.Background()

	// Create a large dataset.
	const numVolumes = 100
	const chunksPerVolume = 10
	const numOrphans = 1000

	chunkID := 0
	for i := 0; i < numVolumes; i++ {
		vol := &VolumeMeta{
			VolumeID:  fmt.Sprintf("vol-%d", i),
			Pool:      "pool-a",
			SizeBytes: 1024 * 1024,
			ChunkIDs:  make([]string, chunksPerVolume),
		}
		for j := 0; j < chunksPerVolume; j++ {
			chunkIDStr := fmt.Sprintf("chunk-%d", chunkID)
			vol.ChunkIDs[j] = chunkIDStr
			pm := &PlacementMap{
				ChunkID: chunkIDStr,
				Nodes:   []string{"node-a", "node-b", "node-c"},
			}
			if err := store.PutPlacementMap(ctx, pm); err != nil {
				b.Fatalf("failed to put placement: %v", err)
			}
			chunkID++
		}
		if err := store.PutVolumeMeta(ctx, vol); err != nil {
			b.Fatalf("failed to put volume: %v", err)
		}
	}

	// Add orphan placement maps.
	for i := 0; i < numOrphans; i++ {
		chunkIDStr := fmt.Sprintf("chunk-orphan-%d", i)
		pm := &PlacementMap{
			ChunkID: chunkIDStr,
			Nodes:   []string{"node-a", "node-b", "node-c"},
		}
		if err := store.PutPlacementMap(ctx, pm); err != nil {
			b.Fatalf("failed to put orphan placement: %v", err)
		}
	}

	gc := NewGarbageCollector(store, 1*time.Hour, 24*time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := gc.RunOnce(ctx)
		if err != nil {
			b.Fatalf("GC failed: %v", err)
		}
	}
}
