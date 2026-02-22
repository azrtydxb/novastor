//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// setupMetadataService creates a single-node Raft metadata store with a gRPC
// server and client. It returns the RaftStore and GRPCClient, registering
// cleanup with t.Cleanup.
func setupMetadataService(t *testing.T) (*metadata.RaftStore, *metadata.GRPCClient) {
	t.Helper()

	raftDir := t.TempDir()

	// Allocate a random port for Raft transport.
	raftAddr := allocAddr(t)

	store, err := metadata.NewRaftStore(metadata.RaftConfig{
		NodeID:   "test-node",
		DataDir:  raftDir,
		RaftAddr: raftAddr,
		Backend:  "memory",
	})
	if err != nil {
		t.Fatalf("NewRaftStore failed: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	waitForLeader(t, store, 10*time.Second)

	// Start gRPC metadata server.
	metaLis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for metadata server: %v", err)
	}
	metaAddr := metaLis.Addr().String()

	metaSrv := grpc.NewServer()
	metaGRPC := metadata.NewGRPCServer(store)
	metaGRPC.Register(metaSrv)
	t.Cleanup(func() { metaSrv.GracefulStop() })
	go func() {
		_ = metaSrv.Serve(metaLis)
	}()

	// Create gRPC client.
	metaClient, err := metadata.Dial(
		metaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial metadata server: %v", err)
	}
	t.Cleanup(func() { metaClient.Close() })

	return store, metaClient
}

func TestAgentMetadata_RegisterNode(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()

	// Register a node via PutNodeMeta.
	node := &metadata.NodeMeta{
		NodeID:            "agent-node-1",
		Address:           "192.168.1.10:9100",
		DiskCount:         4,
		TotalCapacity:     1_000_000_000_000,
		AvailableCapacity: 800_000_000_000,
		LastHeartbeat:     time.Now().Unix(),
		Status:            "ready",
	}
	if err := client.PutNodeMeta(ctx, node); err != nil {
		t.Fatalf("PutNodeMeta failed: %v", err)
	}

	// Verify node appears in ListNodeMetas.
	nodes, err := client.ListNodeMetas(ctx)
	if err != nil {
		t.Fatalf("ListNodeMetas failed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].NodeID != "agent-node-1" {
		t.Errorf("expected nodeID 'agent-node-1', got %q", nodes[0].NodeID)
	}
	if nodes[0].Address != "192.168.1.10:9100" {
		t.Errorf("expected address '192.168.1.10:9100', got %q", nodes[0].Address)
	}
	if nodes[0].DiskCount != 4 {
		t.Errorf("expected diskCount 4, got %d", nodes[0].DiskCount)
	}
}

func TestAgentMetadata_HeartbeatUpdate(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()

	initialTime := time.Now().Unix() - 60 // 1 minute ago

	node := &metadata.NodeMeta{
		NodeID:            "agent-node-hb",
		Address:           "192.168.1.20:9100",
		DiskCount:         2,
		TotalCapacity:     500_000_000_000,
		AvailableCapacity: 400_000_000_000,
		LastHeartbeat:     initialTime,
		Status:            "ready",
	}
	if err := client.PutNodeMeta(ctx, node); err != nil {
		t.Fatalf("PutNodeMeta failed: %v", err)
	}

	// Verify initial heartbeat timestamp.
	got, err := client.GetNodeMeta(ctx, "agent-node-hb")
	if err != nil {
		t.Fatalf("GetNodeMeta failed: %v", err)
	}
	if got.LastHeartbeat != initialTime {
		t.Errorf("expected initial heartbeat %d, got %d", initialTime, got.LastHeartbeat)
	}

	// Update heartbeat.
	updatedTime := time.Now().Unix()
	node.LastHeartbeat = updatedTime
	node.AvailableCapacity = 350_000_000_000
	if err := client.PutNodeMeta(ctx, node); err != nil {
		t.Fatalf("PutNodeMeta (update) failed: %v", err)
	}

	// Verify heartbeat timestamp updated.
	got, err = client.GetNodeMeta(ctx, "agent-node-hb")
	if err != nil {
		t.Fatalf("GetNodeMeta after update failed: %v", err)
	}
	if got.LastHeartbeat != updatedTime {
		t.Errorf("expected updated heartbeat %d, got %d", updatedTime, got.LastHeartbeat)
	}
	if got.AvailableCapacity != 350_000_000_000 {
		t.Errorf("expected available capacity 350GB, got %d", got.AvailableCapacity)
	}
}

func TestAgentMetadata_MultipleNodes(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()

	// Register multiple nodes.
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	for i, id := range nodeIDs {
		node := &metadata.NodeMeta{
			NodeID:            id,
			Address:           "192.168.1." + string(rune('1'+i)) + "0:9100",
			DiskCount:         2 + i,
			TotalCapacity:     int64(i+1) * 100_000_000_000,
			AvailableCapacity: int64(i+1) * 80_000_000_000,
			LastHeartbeat:     time.Now().Unix(),
			Status:            "ready",
		}
		if err := client.PutNodeMeta(ctx, node); err != nil {
			t.Fatalf("PutNodeMeta for %s failed: %v", id, err)
		}
	}

	// Verify all nodes appear.
	nodes, err := client.ListNodeMetas(ctx)
	if err != nil {
		t.Fatalf("ListNodeMetas failed: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// Verify individual node retrieval.
	for _, id := range nodeIDs {
		got, err := client.GetNodeMeta(ctx, id)
		if err != nil {
			t.Errorf("GetNodeMeta(%s) failed: %v", id, err)
		}
		if got.NodeID != id {
			t.Errorf("expected nodeID %q, got %q", id, got.NodeID)
		}
	}

	// Delete one node and verify.
	if err := client.DeleteNodeMeta(ctx, "node-b"); err != nil {
		t.Fatalf("DeleteNodeMeta failed: %v", err)
	}
	nodes, err = client.ListNodeMetas(ctx)
	if err != nil {
		t.Fatalf("ListNodeMetas after delete failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after delete, got %d", len(nodes))
	}
}

func TestAgentMetadata_PlacementMaps(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()

	// Store placement maps.
	placements := []*metadata.PlacementMap{
		{ChunkID: "chunk-001", Nodes: []string{"node-a", "node-b", "node-c"}},
		{ChunkID: "chunk-002", Nodes: []string{"node-b", "node-c", "node-d"}},
		{ChunkID: "chunk-003", Nodes: []string{"node-a", "node-c", "node-d"}},
	}

	for _, pm := range placements {
		if err := client.PutPlacementMap(ctx, pm); err != nil {
			t.Fatalf("PutPlacementMap for %s failed: %v", pm.ChunkID, err)
		}
	}

	// Verify individual retrieval.
	got, err := client.GetPlacementMap(ctx, "chunk-001")
	if err != nil {
		t.Fatalf("GetPlacementMap failed: %v", err)
	}
	if got.ChunkID != "chunk-001" {
		t.Errorf("expected chunkID 'chunk-001', got %q", got.ChunkID)
	}
	if len(got.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(got.Nodes))
	}

	// Verify list retrieval.
	allPMs, err := client.ListPlacementMaps(ctx)
	if err != nil {
		t.Fatalf("ListPlacementMaps failed: %v", err)
	}
	if len(allPMs) != 3 {
		t.Errorf("expected 3 placement maps, got %d", len(allPMs))
	}

	// Verify placement map for chunk-002.
	got2, err := client.GetPlacementMap(ctx, "chunk-002")
	if err != nil {
		t.Fatalf("GetPlacementMap for chunk-002 failed: %v", err)
	}
	if len(got2.Nodes) != 3 || got2.Nodes[0] != "node-b" {
		t.Errorf("unexpected placement map for chunk-002: %+v", got2)
	}
}

func TestAgentMetadata_VolumeMetaRoundTrip(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()

	// Store volume metadata with chunk mappings.
	vol := &metadata.VolumeMeta{
		VolumeID:  "vol-integ-1",
		Pool:      "fast-pool",
		SizeBytes: 10 * 1024 * 1024 * 1024, // 10 GiB
		ChunkIDs:  []string{"chunk-001", "chunk-002", "chunk-003"},
	}
	if err := client.PutVolumeMeta(ctx, vol); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	// Store placement maps for each chunk.
	for _, cid := range vol.ChunkIDs {
		pm := &metadata.PlacementMap{
			ChunkID: cid,
			Nodes:   []string{"node-a", "node-b", "node-c"},
		}
		if err := client.PutPlacementMap(ctx, pm); err != nil {
			t.Fatalf("PutPlacementMap failed: %v", err)
		}
	}

	// Retrieve and verify volume.
	gotVol, err := client.GetVolumeMeta(ctx, "vol-integ-1")
	if err != nil {
		t.Fatalf("GetVolumeMeta failed: %v", err)
	}
	if gotVol.Pool != "fast-pool" {
		t.Errorf("expected pool 'fast-pool', got %q", gotVol.Pool)
	}
	if len(gotVol.ChunkIDs) != 3 {
		t.Errorf("expected 3 chunkIDs, got %d", len(gotVol.ChunkIDs))
	}

	// Verify each chunk's placement.
	for _, cid := range gotVol.ChunkIDs {
		pm, err := client.GetPlacementMap(ctx, cid)
		if err != nil {
			t.Errorf("GetPlacementMap for %s failed: %v", cid, err)
		}
		if len(pm.Nodes) != 3 {
			t.Errorf("expected 3 replica nodes for %s, got %d", cid, len(pm.Nodes))
		}
	}

	// Delete volume and verify.
	if err := client.DeleteVolumeMeta(ctx, "vol-integ-1"); err != nil {
		t.Fatalf("DeleteVolumeMeta failed: %v", err)
	}
	_, err = client.GetVolumeMeta(ctx, "vol-integ-1")
	if err == nil {
		t.Error("expected error for deleted volume")
	}
}
