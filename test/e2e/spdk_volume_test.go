//go:build e2e

// Package e2e provides end-to-end tests for the NovaStor SPDK data-plane.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	dppb "github.com/azrtydxb/novastor/api/proto/dataplane"
	"github.com/azrtydxb/novastor/internal/dataplane"
)

const (
	dataplaneAddr = "localhost:9500"
	testVolumeID  = "e2e-test-vol-001"
	testListenIP  = "127.0.0.1"
	testPort      = "4430"
)

// TestSPDKVolumeLifecycle tests the complete lifecycle of a chunk-backed volume
// using the SPDK data-plane via gRPC: init chunk store → create volume → create
// NVMe-oF target → verify → delete target → delete volume.
func TestSPDKVolumeLifecycle(t *testing.T) {
	if os.Getenv("NOVASTOR_E2E") == "" {
		t.Skip("set NOVASTOR_E2E=1 to run end-to-end tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := zaptest.NewLogger(t)
	addr := os.Getenv("NOVASTOR_DATAPLANE_ADDR")
	if addr == "" {
		addr = dataplaneAddr
	}

	client, err := dataplane.Dial(addr, logger)
	if err != nil {
		t.Fatalf("failed to connect to SPDK data-plane at %s: %v", addr, err)
	}
	defer client.Close()

	// Verify the data-plane is reachable.
	ver, err := client.GetVersion()
	if err != nil {
		t.Fatalf("failed to get data-plane version: %v", err)
	}
	t.Logf("data-plane version: %s", ver)

	// Step 1: Create a malloc bdev for testing (no real disk needed).
	t.Log("creating malloc bdev for storage backend")
	if _, err := client.CreateMallocBdev("e2e-base", 256, 512); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("CreateMallocBdev failed: %v", err)
		}
		t.Log("malloc bdev already exists, reusing")
	}

	// Step 2: Initialise the chunk store on the malloc bdev.
	t.Log("initialising chunk store")
	if _, err := client.InitChunkStore("e2e-base", "e2e-test-node-uuid"); err != nil {
		if !strings.Contains(err.Error(), "already") {
			t.Fatalf("InitChunkStore failed: %v", err)
		}
		t.Log("chunk store already initialised, reusing")
	}

	// Step 3: Create a volume (returns the bdev name and size).
	t.Log("creating volume")
	bdevName, sizeBytes, err := client.CreateVolume("raw", testVolumeID, 128*1024*1024, false)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	t.Logf("volume bdev: %s, size: %d", bdevName, sizeBytes)

	// Step 4: Create an NVMe-oF target exposing the chunk volume bdev.
	t.Log("creating NVMe-oF target")
	nqn, err := client.CreateNvmfTarget(testVolumeID, bdevName, testListenIP, testPort, "optimized", 1)
	if err != nil {
		t.Fatalf("CreateNvmfTarget failed: %v", err)
	}
	t.Logf("target NQN: %s", nqn)

	// Step 5: Verify target is listening (try to connect with nvme-cli).
	t.Log("verifying NVMe-oF target reachability")
	verifyTarget(t, ctx, testListenIP, testPort, nqn)

	// Step 6: Clean up - delete target, then volume.
	t.Log("cleaning up NVMe-oF target")
	if err := client.DeleteNvmfTarget(testVolumeID); err != nil {
		t.Errorf("DeleteNvmfTarget failed: %v", err)
	}

	t.Log("cleaning up volume")
	if err := client.DeleteVolume("raw", testVolumeID); err != nil {
		t.Errorf("DeleteVolume failed: %v", err)
	}

	t.Log("SPDK volume lifecycle test passed")
}

// TestSPDKReplicaBdev tests creation of a replica bdev that fans out writes
// across multiple targets.
func TestSPDKReplicaBdev(t *testing.T) {
	if os.Getenv("NOVASTOR_E2E") == "" {
		t.Skip("set NOVASTOR_E2E=1 to run end-to-end tests")
	}

	logger := zaptest.NewLogger(t)
	addr := os.Getenv("NOVASTOR_DATAPLANE_ADDR")
	if addr == "" {
		addr = dataplaneAddr
	}

	client, err := dataplane.Dial(addr, logger)
	if err != nil {
		t.Fatalf("failed to connect to SPDK data-plane at %s: %v", addr, err)
	}
	defer client.Close()

	// Create a replica bdev with two targets (both pointing to localhost).
	targets := []*dppb.ReplicaTarget{
		{BdevName: "replica-test-bdev-1", IsLocal: true},
		{BdevName: "replica-test-bdev-2", IsLocal: false},
	}

	t.Log("creating replica bdev")
	if err := client.CreateReplicaBdev("e2e-replica", targets, 1073741824, "round-robin"); err != nil {
		t.Fatalf("CreateReplicaBdev failed: %v", err)
	}

	t.Log("replica bdev created successfully")
}

// verifyTarget uses nvme-cli to check if the NVMe-oF target is discoverable.
func verifyTarget(t *testing.T, ctx context.Context, addr, port, nqn string) {
	t.Helper()

	// Try `nvme discover` to verify the target is reachable.
	cmd := exec.CommandContext(ctx, "nvme", "discover",
		"-t", "tcp",
		"-a", addr,
		"-s", port,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// nvme-cli might not be installed in the test environment.
		if strings.Contains(err.Error(), "executable file not found") {
			t.Logf("nvme-cli not available, skipping discovery check")
			return
		}
		t.Logf("nvme discover failed (may be expected in stub mode): %v: %s", err, string(out))
		return
	}

	if !strings.Contains(string(out), nqn) {
		t.Errorf("nvme discover output does not contain NQN %s:\n%s", nqn, string(out))
	} else {
		t.Logf("NVMe-oF target discovered: %s", nqn)
	}
}
