//go:build e2e

package e2e

import (
	"context"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"

	novacsi "github.com/piwi3910/novastor/internal/csi"
	"github.com/piwi3910/novastor/internal/placement"
)

// TestBlockStorage_ProvisionWriteReadDelete tests the full block storage flow:
//  1. Start a metadata service (single-node Raft)
//  2. Start a chunk agent (with LocalStore)
//  3. Create a volume via CSI controller
//  4. Verify volume exists in metadata
//  5. Write chunk data and verify read-back
//  6. Delete the volume
//  7. Verify cleanup
func TestBlockStorage_ProvisionWriteReadDelete(t *testing.T) {
	// Set up a full cluster: metadata Raft + chunk agent.
	tc := newTestCluster(t)
	defer tc.Close()

	ctx := context.Background()

	// Create a placement engine with a single node.
	placer := placement.NewCRUSHPlacer([]placement.Node{{ID: "node-1", Weight: 1.0}})

	// Create CSI controller with real metadata client and placement engine.
	ctrl := novacsi.NewControllerServer(tc.metaClient, placer, nil)

	// Step 1: CreateVolume via CSI controller.
	createResp, err := ctrl.CreateVolume(ctx, &csipb.CreateVolumeRequest{
		Name: "test-volume-1",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 8 * 1024 * 1024, // 8 MiB = 2 chunks
		},
		VolumeCapabilities: []*csipb.VolumeCapability{
			{
				AccessMode: &csipb.VolumeCapability_AccessMode{
					Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volumeID := createResp.Volume.VolumeId
	if volumeID == "" {
		t.Fatal("CreateVolume returned empty volume ID")
	}
	t.Logf("Created volume: %s (capacity: %d bytes)", volumeID, createResp.Volume.CapacityBytes)

	if createResp.Volume.CapacityBytes != 8*1024*1024 {
		t.Errorf("Expected capacity 8 MiB, got %d", createResp.Volume.CapacityBytes)
	}

	// Step 2: Verify volume exists in metadata via gRPC client.
	volMeta, err := tc.metaClient.GetVolumeMeta(ctx, volumeID)
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}
	if volMeta.VolumeID != volumeID {
		t.Errorf("Expected volume ID %s, got %s", volumeID, volMeta.VolumeID)
	}
	if volMeta.SizeBytes != 8*1024*1024 {
		t.Errorf("Expected size 8 MiB, got %d", volMeta.SizeBytes)
	}
	if len(volMeta.ChunkIDs) != 2 {
		t.Errorf("Expected 2 chunk IDs, got %d", len(volMeta.ChunkIDs))
	}
	t.Logf("Volume metadata verified: %d chunks", len(volMeta.ChunkIDs))

	// Step 3: ValidateVolumeCapabilities.
	valResp, err := ctrl.ValidateVolumeCapabilities(ctx, &csipb.ValidateVolumeCapabilitiesRequest{
		VolumeId: volumeID,
		VolumeCapabilities: []*csipb.VolumeCapability{
			{
				AccessMode: &csipb.VolumeCapability_AccessMode{
					Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
	}
	if valResp.Confirmed == nil {
		t.Error("Expected capabilities to be confirmed, got nil")
	}

	// Step 4: Write actual chunk data via the agent client and verify read-back.
	testData := []byte("hello novastor block storage e2e test data!")
	chunkID := "e2e-test-chunk-001"
	checksum := computeCRC32C(testData)

	err = tc.agentClient.PutChunk(ctx, chunkID, testData, checksum)
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	// Verify the chunk exists.
	exists, err := tc.agentClient.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk failed: %v", err)
	}
	if !exists {
		t.Error("Expected chunk to exist after PutChunk")
	}

	// Read back the chunk and verify content.
	readData, readChecksum, err := tc.agentClient.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}
	if string(readData) != string(testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, readData)
	}
	if readChecksum != checksum {
		t.Errorf("Checksum mismatch: expected %d, got %d", checksum, readChecksum)
	}
	t.Logf("Chunk write/read verified: %d bytes, checksum=%d", len(readData), readChecksum)

	// Step 5: DeleteVolume via CSI controller.
	_, err = ctrl.DeleteVolume(ctx, &csipb.DeleteVolumeRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}

	// Step 6: Verify volume metadata is removed.
	_, err = tc.metaClient.GetVolumeMeta(ctx, volumeID)
	if err == nil {
		t.Error("Expected error when getting deleted volume metadata, got nil")
	}
	t.Logf("Volume deletion verified: metadata removed for %s", volumeID)

	// Step 7: Clean up chunk data.
	err = tc.agentClient.DeleteChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("DeleteChunk failed: %v", err)
	}

	exists, err = tc.agentClient.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk after delete failed: %v", err)
	}
	if exists {
		t.Error("Expected chunk to not exist after DeleteChunk")
	}
	t.Log("Chunk deletion verified")
}

// TestBlockStorage_DeleteIdempotent verifies that deleting a non-existent
// volume is idempotent per the CSI specification.
func TestBlockStorage_DeleteIdempotent(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.Close()

	ctx := context.Background()
	placer := placement.NewCRUSHPlacer([]placement.Node{{ID: "node-1", Weight: 1.0}})
	ctrl := novacsi.NewControllerServer(tc.metaClient, placer, nil)

	// Deleting a volume that never existed should succeed.
	_, err := ctrl.DeleteVolume(ctx, &csipb.DeleteVolumeRequest{
		VolumeId: "non-existent-volume-id",
	})
	if err != nil {
		t.Fatalf("DeleteVolume for non-existent volume should succeed, got: %v", err)
	}
	t.Log("Idempotent delete verified")
}
