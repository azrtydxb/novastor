package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// TestCSIControllerMetrics verifies that CSI controller operations
// execute successfully (metrics are observed in production code).
// The actual metric observation happens via defer in the business logic.
func TestCSIControllerMetrics(t *testing.T) {
	cs, store := setupController()

	// Create a volume - this observes VolumeProvisionDuration metric.
	createResp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "metrics-test-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	// Verify volume was created.
	if createResp.GetVolume().GetVolumeId() == "" {
		t.Fatal("expected non-empty volume ID")
	}

	// Delete the volume - this observes VolumeDeleteDuration metric.
	volumeID := createResp.GetVolume().GetVolumeId()
	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}

	// Verify volume was actually deleted from metadata store.
	if _, err := store.GetVolumeMeta(context.Background(), volumeID); err == nil {
		t.Error("expected volume metadata to be deleted")
	}
}
