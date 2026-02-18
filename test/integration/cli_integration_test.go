//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
)

// buildCLI builds the novastorctl binary for testing.
func buildCLI(t *testing.T) string {
	t.Helper()

	// Find the repository root by looking for go.mod
	repoRoot := "../.."
	binaryPath := t.TempDir() + "/novastorctl"
	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/cli/")
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build CLI: %v\n%s", err, out)
	}
	return binaryPath
}

// runCLI runs the novastorctl binary with the given args and returns the output.
func runCLI(t *testing.T, binary string, metaAddr string, args ...string) (string, error) {
	t.Helper()

	allArgs := append([]string{"--meta-addr", metaAddr}, args...)
	cmd := exec.Command(binary, allArgs...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestCLI_Version(t *testing.T) {
	binary := buildCLI(t)

	out, err := runCLI(t, binary, "localhost:7001", "version")
	if err != nil {
		t.Fatalf("version command failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "novastorctl") {
		t.Errorf("version output does not contain 'novastorctl': %s", out)
	}
}

func TestCLI_Completion(t *testing.T) {
	binary := buildCLI(t)

	// Test bash completion
	out, err := runCLI(t, binary, "localhost:7001", "completion", "bash")
	if err != nil {
		t.Fatalf("completion bash failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "complete -o default -F __start_novastorctl novastorctl") {
		t.Errorf("bash completion output does not contain expected content: %s", out)
	}

	// Test zsh completion
	out, err = runCLI(t, binary, "localhost:7001", "completion", "zsh")
	if err != nil {
		t.Fatalf("completion zsh failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "#compdef novastorctl") {
		t.Errorf("zsh completion output does not contain expected content: %s", out)
	}
}

func TestCLI_Status_EmptyCluster(t *testing.T) {
	_, client := setupMetadataService(t)
	metaAddr := client.Addr()

	binary := buildCLI(t)

	out, err := runCLI(t, binary, metaAddr, "status")
	if err != nil {
		t.Fatalf("status command failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "NovaStor Cluster Status") {
		t.Errorf("status output does not contain expected header: %s", out)
	}
	if !strings.Contains(out, "Nodes:             0") {
		t.Errorf("status output does not show 0 nodes: %s", out)
	}
}

func TestCLI_Status_JSON(t *testing.T) {
	_, client := setupMetadataService(t)
	metaAddr := client.Addr()

	binary := buildCLI(t)

	out, err := runCLI(t, binary, metaAddr, "status", "--output", "json")
	if err != nil {
		t.Fatalf("status --output json failed: %v, output: %s", err, out)
	}

	var status map[string]any
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatalf("failed to parse status JSON: %v, output: %s", err, out)
	}
	if status["health"] != "OK" {
		t.Errorf("expected health OK, got %v", status["health"])
	}
	if int(status["nodes"].(float64)) != 0 {
		t.Errorf("expected 0 nodes, got %v", status["nodes"])
	}
}

func TestCLI_Node_RegisterAndList(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Register a node via gRPC
	node := &metadata.NodeMeta{
		NodeID:            "test-node-1",
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

	binary := buildCLI(t)

	// Test node list
	out, err := runCLI(t, binary, metaAddr, "node", "list")
	if err != nil {
		t.Fatalf("node list failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "test-node-1") {
		t.Errorf("node list output does not contain test-node-1: %s", out)
	}
	if !strings.Contains(out, "ready") {
		t.Errorf("node list output does not contain status: %s", out)
	}
}

func TestCLI_Node_JSON(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Register a node via gRPC
	node := &metadata.NodeMeta{
		NodeID:            "json-test-node",
		Address:           "192.168.1.20:9100",
		DiskCount:         2,
		TotalCapacity:     500_000_000_000,
		AvailableCapacity: 400_000_000_000,
		LastHeartbeat:     time.Now().Unix(),
		Status:            "ready",
	}
	if err := client.PutNodeMeta(ctx, node); err != nil {
		t.Fatalf("PutNodeMeta failed: %v", err)
	}

	binary := buildCLI(t)

	// Test node list with JSON output
	out, err := runCLI(t, binary, metaAddr, "node", "list", "--output", "json")
	if err != nil {
		t.Fatalf("node list --output json failed: %v, output: %s", err, out)
	}

	var nodes []map[string]any
	if err := json.Unmarshal([]byte(out), &nodes); err != nil {
		t.Fatalf("failed to parse node list JSON: %v, output: %s", err, out)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0]["nodeId"] != "json-test-node" {
		t.Errorf("expected nodeId json-test-node, got %v", nodes[0]["nodeId"])
	}
}

func TestCLI_Volume_CreateListDelete(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	binary := buildCLI(t)

	// Create a volume via CLI
	out, err := runCLI(t, binary, metaAddr, "volume", "create", "--pool", "test-pool", "--size", "1073741824")
	if err != nil {
		t.Fatalf("volume create failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "Volume created:") {
		t.Errorf("volume create output does not contain expected message: %s", out)
	}

	// Extract volume ID from output
	parts := strings.Split(out, ":")
	if len(parts) < 2 {
		t.Fatalf("could not extract volume ID from output: %s", out)
	}
	volID := strings.TrimSpace(parts[1])

	// List volumes
	out, err = runCLI(t, binary, metaAddr, "volume", "list")
	if err != nil {
		t.Fatalf("volume list failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, volID) {
		t.Errorf("volume list output does not contain created volume ID: %s", out)
	}

	// Get volume details
	out, err = runCLI(t, binary, metaAddr, "volume", "get", volID)
	if err != nil {
		t.Fatalf("volume get failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, volID) {
		t.Errorf("volume get output does not contain volume ID: %s", out)
	}

	// Delete volume
	out, err = runCLI(t, binary, metaAddr, "volume", "delete", volID)
	if err != nil {
		t.Fatalf("volume delete failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "Volume deleted:") {
		t.Errorf("volume delete output does not contain expected message: %s", out)
	}

	// Verify deletion
	_, err = client.GetVolumeMeta(ctx, volID)
	if err == nil {
		t.Error("expected error when getting deleted volume")
	}
}

func TestCLI_Volume_JSON(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Create a volume via gRPC
	vol := &metadata.VolumeMeta{
		VolumeID:  "json-vol-test",
		Pool:      "test-pool",
		SizeBytes: 5_368_709_120, // 5 GiB
		ChunkIDs:  []string{"chunk-1", "chunk-2"},
	}
	if err := client.PutVolumeMeta(ctx, vol); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	binary := buildCLI(t)

	// Test volume list with JSON output
	out, err := runCLI(t, binary, metaAddr, "volume", "list", "--output", "json")
	if err != nil {
		t.Fatalf("volume list --output json failed: %v, output: %s", err, out)
	}

	var vols []map[string]any
	if err := json.Unmarshal([]byte(out), &vols); err != nil {
		t.Fatalf("failed to parse volume list JSON: %v, output: %s", err, out)
	}
	if len(vols) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(vols))
	}
	if vols[0]["volumeId"] != "json-vol-test" {
		t.Errorf("expected volumeId json-vol-test, got %v", vols[0]["volumeId"])
	}
	if vols[0]["pool"] != "test-pool" {
		t.Errorf("expected pool test-pool, got %v", vols[0]["pool"])
	}
}

func TestCLI_Pool_List(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Register some nodes
	for i := 0; i < 3; i++ {
		node := &metadata.NodeMeta{
			NodeID:            "pool-node-" + string(rune('1'+i)),
			Address:           "192.168.1.1" + string(rune('0'+i)) + ":9100",
			DiskCount:         2,
			TotalCapacity:     100_000_000_000,
			AvailableCapacity: 80_000_000_000,
			LastHeartbeat:     time.Now().Unix(),
			Status:            "ready",
		}
		if err := client.PutNodeMeta(ctx, node); err != nil {
			t.Fatalf("PutNodeMeta failed: %v", err)
		}
	}

	binary := buildCLI(t)

	// Test pool list
	out, err := runCLI(t, binary, metaAddr, "pool", "list")
	if err != nil {
		t.Fatalf("pool list failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "3") {
		t.Errorf("pool list output does not show 3 nodes: %s", out)
	}
}

func TestCLI_Snapshot_CreateListDelete(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Create a volume first
	vol := &metadata.VolumeMeta{
		VolumeID:  "snap-test-vol",
		Pool:      "test-pool",
		SizeBytes: 1_073_741_824,
		ChunkIDs:  []string{"chunk-1"},
	}
	if err := client.PutVolumeMeta(ctx, vol); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	binary := buildCLI(t)

	// Create a snapshot via CLI
	out, err := runCLI(t, binary, metaAddr, "snapshot", "create", "--volume-id", "snap-test-vol")
	if err != nil {
		t.Fatalf("snapshot create failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "Snapshot created:") {
		t.Errorf("snapshot create output does not contain expected message: %s", out)
	}

	// Extract snapshot ID from output
	parts := strings.Split(out, ":")
	if len(parts) < 2 {
		t.Fatalf("could not extract snapshot ID from output: %s", out)
	}
	snapID := strings.TrimSpace(parts[1])

	// List snapshots
	out, err = runCLI(t, binary, metaAddr, "snapshot", "list")
	if err != nil {
		t.Fatalf("snapshot list failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, snapID) {
		t.Errorf("snapshot list output does not contain created snapshot ID: %s", out)
	}

	// Delete snapshot
	out, err = runCLI(t, binary, metaAddr, "snapshot", "delete", snapID)
	if err != nil {
		t.Fatalf("snapshot delete failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "Snapshot deleted:") {
		t.Errorf("snapshot delete output does not contain expected message: %s", out)
	}
}

func TestCLI_Snapshot_JSON(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Create a volume and snapshot via gRPC
	vol := &metadata.VolumeMeta{
		VolumeID:  "json-snap-vol",
		Pool:      "test-pool",
		SizeBytes: 1_073_741_824,
		ChunkIDs:  []string{"chunk-1"},
	}
	if err := client.PutVolumeMeta(ctx, vol); err != nil {
		t.Fatalf("PutVolumeMeta failed: %v", err)
	}

	snap := &metadata.SnapshotMeta{
		SnapshotID:     "json-snap-test",
		SourceVolumeID: "json-snap-vol",
		SizeBytes:      1_073_741_824,
		ChunkIDs:       []string{"chunk-1"},
		CreationTime:   time.Now().UnixNano(),
		ReadyToUse:     true,
	}
	if err := client.PutSnapshot(ctx, snap); err != nil {
		t.Fatalf("PutSnapshot failed: %v", err)
	}

	binary := buildCLI(t)

	// Test snapshot list with JSON output
	out, err := runCLI(t, binary, metaAddr, "snapshot", "list", "--output", "json")
	if err != nil {
		t.Fatalf("snapshot list --output json failed: %v, output: %s", err, out)
	}

	var snaps []map[string]any
	if err := json.Unmarshal([]byte(out), &snaps); err != nil {
		t.Fatalf("failed to parse snapshot list JSON: %v, output: %s", err, out)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0]["snapshotId"] != "json-snap-test" {
		t.Errorf("expected snapshotId json-snap-test, got %v", snaps[0]["snapshotId"])
	}
}

func TestCLI_Bucket_CreateListDelete(t *testing.T) {
	_, client := setupMetadataService(t)
	metaAddr := client.Addr()

	binary := buildCLI(t)

	// Create a bucket via CLI
	out, err := runCLI(t, binary, metaAddr, "bucket", "create", "test-bucket-cli")
	if err != nil {
		t.Fatalf("bucket create failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "Bucket created:") {
		t.Errorf("bucket create output does not contain expected message: %s", out)
	}

	// List buckets
	out, err = runCLI(t, binary, metaAddr, "bucket", "list")
	if err != nil {
		t.Fatalf("bucket list failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "test-bucket-cli") {
		t.Errorf("bucket list output does not contain created bucket name: %s", out)
	}

	// Delete bucket
	out, err = runCLI(t, binary, metaAddr, "bucket", "delete", "test-bucket-cli")
	if err != nil {
		t.Fatalf("bucket delete failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "Bucket deleted:") {
		t.Errorf("bucket delete output does not contain expected message: %s", out)
	}
}

func TestCLI_Bucket_JSON(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Create a bucket via gRPC
	bucket := &metadata.BucketMeta{
		Name:         "json-test-bucket",
		Owner:        "admin",
		CreationDate: time.Now().UnixNano(),
	}
	if err := client.PutBucketMeta(ctx, bucket); err != nil {
		t.Fatalf("PutBucketMeta failed: %v", err)
	}

	binary := buildCLI(t)

	// Test bucket list with JSON output
	out, err := runCLI(t, binary, metaAddr, "bucket", "list", "--output", "json")
	if err != nil {
		t.Fatalf("bucket list --output json failed: %v, output: %s", err, out)
	}

	var buckets []map[string]any
	if err := json.Unmarshal([]byte(out), &buckets); err != nil {
		t.Fatalf("failed to parse bucket list JSON: %v, output: %s", err, out)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}
	if buckets[0]["name"] != "json-test-bucket" {
		t.Errorf("expected name json-test-bucket, got %v", buckets[0]["name"])
	}
}

func TestCLI_Pool_JSON(t *testing.T) {
	_, client := setupMetadataService(t)
	ctx := context.Background()
	metaAddr := client.Addr()

	// Register a node
	node := &metadata.NodeMeta{
		NodeID:            "pool-json-node",
		Address:           "192.168.1.50:9100",
		DiskCount:         4,
		TotalCapacity:     2_000_000_000_000,
		AvailableCapacity: 1_500_000_000_000,
		LastHeartbeat:     time.Now().Unix(),
		Status:            "ready",
	}
	if err := client.PutNodeMeta(ctx, node); err != nil {
		t.Fatalf("PutNodeMeta failed: %v", err)
	}

	binary := buildCLI(t)

	// Test pool list with JSON output
	out, err := runCLI(t, binary, metaAddr, "pool", "list", "--output", "json")
	if err != nil {
		t.Fatalf("pool list --output json failed: %v, output: %s", err, out)
	}

	var pools []map[string]any
	if err := json.Unmarshal([]byte(out), &pools); err != nil {
		t.Fatalf("failed to parse pool list JSON: %v, output: %s", err, out)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool entry, got %d", len(pools))
	}
	if int(pools[0]["nodes"].(float64)) != 1 {
		t.Errorf("expected 1 node, got %v", pools[0]["nodes"])
	}
}

func TestCLI_InvalidMetadataAddress(t *testing.T) {
	binary := buildCLI(t)

	// Test with invalid metadata address
	_, err := runCLI(t, binary, "invalid:12345", "status")
	if err == nil {
		t.Error("expected error with invalid metadata address")
	}
}

func TestCLI_NodeGet_NotFound(t *testing.T) {
	_, client := setupMetadataService(t)
	metaAddr := client.Addr()

	binary := buildCLI(t)

	// Try to get a non-existent node
	out, err := runCLI(t, binary, metaAddr, "node", "get", "nonexistent-node")
	if err == nil {
		t.Error("expected error when getting non-existent node")
	}
	if !strings.Contains(out, "error") && !strings.Contains(out, "not found") {
		t.Logf("output for nonexistent node: %s", out)
	}
}
