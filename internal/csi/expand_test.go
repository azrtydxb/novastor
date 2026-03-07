package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/azrtydxb/novastor/internal/metadata"
)

// --- helpers ---

func setupExpandController() (*ExpandController, *mockMetadataStore) {
	store := newMockMetadataStore()
	placer := &mockPlacer{nodes: []string{"node-1", "node-2", "node-3"}}
	return NewExpandController(store, placer), store
}

// seedExpandVolume inserts a volume directly into the mock store for expand tests.
func seedExpandVolume(store *mockMetadataStore, volumeID string, sizeBytes uint64, chunkIDs []string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.volumes[volumeID] = &metadata.VolumeMeta{
		VolumeID:  volumeID,
		SizeBytes: sizeBytes,
		ChunkIDs:  chunkIDs,
	}
}

// --- ControllerExpandVolume tests ---

func TestControllerExpandVolume_Success(t *testing.T) {
	ec, store := setupExpandController()

	// Seed a 2-chunk (8 MiB) volume.
	seedExpandVolume(store, "vol-expand", 8*1024*1024, []string{"vol-expand-chunk-0000", "vol-expand-chunk-0001"})

	// Expand to 16 MiB (4 chunks).
	resp, err := ec.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "vol-expand",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 16 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume failed: %v", err)
	}

	if resp.GetCapacityBytes() != 16*1024*1024 {
		t.Errorf("expected capacity 16MiB, got %d", resp.GetCapacityBytes())
	}

	// Verify metadata was updated.
	vm, err := store.GetVolumeMeta(context.Background(), "vol-expand")
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	if vm.SizeBytes != 16*1024*1024 {
		t.Errorf("expected stored size 16MiB, got %d", vm.SizeBytes)
	}
	if len(vm.ChunkIDs) != 4 {
		t.Errorf("expected 4 chunks after expansion, got %d", len(vm.ChunkIDs))
	}

	// Verify original chunks are preserved.
	if vm.ChunkIDs[0] != "vol-expand-chunk-0000" || vm.ChunkIDs[1] != "vol-expand-chunk-0001" {
		t.Error("original chunk IDs should be preserved")
	}
}

func TestControllerExpandVolume_NoVolumeID(t *testing.T) {
	ec, _ := setupExpandController()

	_, err := ec.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 16 * 1024 * 1024,
		},
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestControllerExpandVolume_VolumeNotFound(t *testing.T) {
	ec, _ := setupExpandController()

	_, err := ec.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "nonexistent",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 16 * 1024 * 1024,
		},
	})
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestControllerExpandVolume_ShrinkNotAllowed(t *testing.T) {
	ec, store := setupExpandController()

	seedExpandVolume(store, "vol-shrink", 16*1024*1024, []string{"c0", "c1", "c2", "c3"})

	_, err := ec.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "vol-shrink",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 8 * 1024 * 1024, // smaller than current
		},
	})
	if err == nil {
		t.Fatal("expected error for shrink attempt")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestControllerExpandVolume_SameSize(t *testing.T) {
	ec, store := setupExpandController()

	seedExpandVolume(store, "vol-same", 8*1024*1024, []string{"c0", "c1"})

	resp, err := ec.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "vol-same",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 8 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("expected no-op for same size, got error: %v", err)
	}

	if resp.GetCapacityBytes() != 8*1024*1024 {
		t.Errorf("expected capacity 8MiB, got %d", resp.GetCapacityBytes())
	}

	// Verify chunks unchanged.
	vm, err := store.GetVolumeMeta(context.Background(), "vol-same")
	if err != nil {
		t.Fatalf("volume metadata not found: %v", err)
	}
	if len(vm.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunks (unchanged), got %d", len(vm.ChunkIDs))
	}
}
