//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"hash/crc32"
	"net"
	"testing"

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/chunk"
	"github.com/azrtydxb/novastor/internal/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// setupChunkAgent creates a LocalStore, starts a chunk gRPC server, and returns
// an agent.Client connected to it. Cleanup is registered with t.Cleanup.
func setupChunkAgent(t *testing.T) (*chunk.LocalStore, *agent.Client) {
	t.Helper()

	chunkDir := t.TempDir()

	localStore, err := chunk.NewLocalStore(chunkDir)
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	chunkLis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for chunk server: %v", err)
	}
	chunkAddr := chunkLis.Addr().String()

	chunkSrv := grpc.NewServer()
	chunkServer := agent.NewChunkServer(localStore)
	chunkServer.Register(chunkSrv)
	t.Cleanup(func() { chunkSrv.GracefulStop() })
	go func() {
		_ = chunkSrv.Serve(chunkLis)
	}()

	agentClient, err := agent.Dial(
		chunkAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial chunk server: %v", err)
	}
	t.Cleanup(func() { agentClient.Close() })

	return localStore, agentClient
}

func TestCSIAgent_WriteReadChunks(t *testing.T) {
	_, agentClient := setupChunkAgent(t)
	ctx := context.Background()

	// Create test data (smaller than 4MB chunk size).
	testData := make([]byte, 1024*512) // 512 KiB
	if _, err := rand.Read(testData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// Compute chunk ID and checksum.
	chunkID := string(chunk.NewChunkID(testData))
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(testData, table)

	// Write chunk via agent gRPC client.
	if err := agentClient.PutChunk(ctx, chunkID, testData, checksum); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	// Read chunk back.
	readData, readChecksum, err := agentClient.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	// Verify data integrity.
	if !bytes.Equal(testData, readData) {
		t.Error("read data does not match written data")
	}
	if readChecksum != checksum {
		t.Errorf("checksum mismatch: written=%d, read=%d", checksum, readChecksum)
	}
}

func TestCSIAgent_MultipleChunks(t *testing.T) {
	_, agentClient := setupChunkAgent(t)
	ctx := context.Background()

	// Simulate a volume with multiple chunks.
	type chunkEntry struct {
		id       string
		data     []byte
		checksum uint32
	}

	numChunks := 5
	entries := make([]chunkEntry, numChunks)
	table := crc32.MakeTable(crc32.Castagnoli)

	for i := 0; i < numChunks; i++ {
		data := make([]byte, 1024*(i+1)) // varying sizes
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("Failed to generate random data: %v", err)
		}
		entry := chunkEntry{
			id:       string(chunk.NewChunkID(data)),
			data:     data,
			checksum: crc32.Checksum(data, table),
		}
		entries[i] = entry

		if err := agentClient.PutChunk(ctx, entry.id, entry.data, entry.checksum); err != nil {
			t.Fatalf("PutChunk[%d] failed: %v", i, err)
		}
	}

	// Verify all chunks exist.
	for i, entry := range entries {
		exists, err := agentClient.HasChunk(ctx, entry.id)
		if err != nil {
			t.Fatalf("HasChunk[%d] failed: %v", i, err)
		}
		if !exists {
			t.Errorf("chunk %d should exist", i)
		}
	}

	// Read all chunks back and verify.
	for i, entry := range entries {
		readData, readChecksum, err := agentClient.GetChunk(ctx, entry.id)
		if err != nil {
			t.Fatalf("GetChunk[%d] failed: %v", i, err)
		}
		if !bytes.Equal(entry.data, readData) {
			t.Errorf("chunk %d data mismatch", i)
		}
		if readChecksum != entry.checksum {
			t.Errorf("chunk %d checksum mismatch: expected=%d, got=%d", i, entry.checksum, readChecksum)
		}
	}
}

func TestCSIAgent_DeleteChunks(t *testing.T) {
	_, agentClient := setupChunkAgent(t)
	ctx := context.Background()

	// Write a chunk.
	testData := []byte("chunk data for deletion test")
	chunkID := string(chunk.NewChunkID(testData))
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(testData, table)

	if err := agentClient.PutChunk(ctx, chunkID, testData, checksum); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	// Verify it exists.
	exists, err := agentClient.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk failed: %v", err)
	}
	if !exists {
		t.Fatal("chunk should exist after write")
	}

	// Delete chunk.
	if err := agentClient.DeleteChunk(ctx, chunkID); err != nil {
		t.Fatalf("DeleteChunk failed: %v", err)
	}

	// Verify it no longer exists.
	exists, err = agentClient.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk after delete failed: %v", err)
	}
	if exists {
		t.Error("chunk should not exist after deletion")
	}
}

func TestCSIAgent_VolumeChunkLifecycle(t *testing.T) {
	_, agentClient := setupChunkAgent(t)
	_, metaClient := setupMetadataService(t)
	ctx := context.Background()

	// Simulate creating a volume with chunk data.
	volumeData := make([]byte, 8*1024) // 8 KiB
	if _, err := rand.Read(volumeData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// Split into chunks and write.
	chunks := chunk.SplitData(volumeData)
	chunkIDs := make([]string, len(chunks))
	for i, c := range chunks {
		if err := agentClient.PutChunk(ctx, string(c.ID), c.Data, c.Checksum); err != nil {
			t.Fatalf("PutChunk failed: %v", err)
		}
		chunkIDs[i] = string(c.ID)
	}

	// Store volume metadata.
	volMeta := &metadata.VolumeMeta{
		VolumeID:  "vol-csi-test-1",
		Pool:      "default-pool",
		SizeBytes: uint64(len(volumeData)),
		ChunkIDs:  chunkIDs,
	}
	if err := metaClient.PutVolumeMeta(ctx, volMeta); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	// Retrieve volume metadata.
	gotVol, err := metaClient.GetVolumeMeta(ctx, "vol-csi-test-1")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}

	// Read all chunks back and reassemble the volume data.
	var reassembled []byte
	for _, cid := range gotVol.ChunkIDs {
		data, _, err := agentClient.GetChunk(ctx, cid)
		if err != nil {
			t.Fatalf("GetChunk(%s) failed: %v", cid, err)
		}
		reassembled = append(reassembled, data...)
	}

	if !bytes.Equal(volumeData, reassembled) {
		t.Error("reassembled volume data does not match original")
	}

	// Delete volume: remove metadata and clean up chunks.
	if err := metaClient.DeleteVolumeMeta(ctx, "vol-csi-test-1"); err != nil {
		t.Fatalf("DeleteVolumeMeta failed: %v", err)
	}
	for _, cid := range chunkIDs {
		if err := agentClient.DeleteChunk(ctx, cid); err != nil {
			t.Fatalf("DeleteChunk(%s) failed: %v", cid, err)
		}
	}

	// Verify chunks are cleaned up.
	for _, cid := range chunkIDs {
		exists, err := agentClient.HasChunk(ctx, cid)
		if err != nil {
			t.Fatalf("HasChunk(%s) after cleanup failed: %v", cid, err)
		}
		if exists {
			t.Errorf("chunk %s should not exist after volume deletion", cid)
		}
	}
}

func TestCSIAgent_LocalChunkStore(t *testing.T) {
	_, agentClient := setupChunkAgent(t)
	ctx := context.Background()

	// Test using LocalChunkStore adapter (used by S3 gateway).
	localChunkStore := agent.NewLocalChunkStore(agentClient)

	testData := []byte("test data for LocalChunkStore adapter")

	// PutChunkData computes chunk ID and checksum internally.
	chunkID, err := localChunkStore.PutChunkData(ctx, testData)
	if err != nil {
		t.Fatalf("PutChunkData failed: %v", err)
	}
	if chunkID == "" {
		t.Fatal("expected non-empty chunk ID")
	}

	// GetChunkData retrieves the data by ID.
	readData, err := localChunkStore.GetChunkData(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunkData failed: %v", err)
	}
	if !bytes.Equal(testData, readData) {
		t.Error("read data does not match written data via LocalChunkStore")
	}

	// DeleteChunkData removes the chunk.
	if err := localChunkStore.DeleteChunkData(ctx, chunkID); err != nil {
		t.Fatalf("DeleteChunkData failed: %v", err)
	}

	// Verify chunk is gone.
	_, err = localChunkStore.GetChunkData(ctx, chunkID)
	if err == nil {
		t.Error("expected error getting deleted chunk")
	}
}
