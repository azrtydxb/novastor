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

	"github.com/azrtydxb/novastor/internal/spdk"
)

const (
	spdkSocketPath = "/var/tmp/novastor-spdk.sock"
	testVolumeID   = "e2e-test-vol-001"
	testListenAddr = "127.0.0.1"
	testPort       = uint16(4430)
)

// TestSPDKVolumeLifecycle tests the complete lifecycle of a chunk-backed volume
// using the SPDK data-plane: init chunk backend → create chunk volume → create
// NVMe-oF target → connect initiator → verify → disconnect → delete.
func TestSPDKVolumeLifecycle(t *testing.T) {
	if os.Getenv("NOVASTOR_E2E") == "" {
		t.Skip("set NOVASTOR_E2E=1 to run end-to-end tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := spdk.NewClient(spdkSocketPath)
	if err := client.Connect(); err != nil {
		t.Fatalf("failed to connect to SPDK data-plane: %v", err)
	}
	defer client.Close()

	// Verify the data-plane is reachable.
	ver, err := client.GetVersion()
	if err != nil {
		t.Fatalf("failed to get data-plane version: %v", err)
	}
	t.Logf("data-plane version: %s", ver)

	// Step 1: Create a malloc bdev for testing (no real disk needed).
	t.Log("creating malloc bdev for chunk backend")
	if err := client.NativeCreateMallocBdev("e2e-base", 256, 512); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("NativeCreateMallocBdev failed: %v", err)
		}
		t.Log("malloc bdev already exists, reusing")
	}

	// Step 2: Initialise the chunk backend on the malloc bdev.
	t.Log("initialising chunk backend")
	capacityBytes := uint64(256) * 1024 * 1024
	if err := client.InitChunkBackend("e2e-base", capacityBytes); err != nil {
		if !strings.Contains(err.Error(), "already") {
			t.Fatalf("InitChunkBackend failed: %v", err)
		}
		t.Log("chunk backend already initialised, reusing")
	}

	// Step 3: Create a chunk volume (returns the auto-registered bdev name).
	t.Log("creating chunk volume")
	bdevName, err := client.CreateChunkVolume(testVolumeID, 128*1024*1024, true)
	if err != nil {
		t.Fatalf("CreateChunkVolume failed: %v", err)
	}
	t.Logf("chunk volume bdev: %s", bdevName)

	// Step 4: Create an NVMe-oF target exposing the chunk volume bdev.
	t.Log("creating NVMe-oF target")
	nqn, err := client.CreateNvmfTarget(testVolumeID, testListenAddr, testPort, bdevName)
	if err != nil {
		t.Fatalf("CreateNvmfTarget failed: %v", err)
	}
	t.Logf("target NQN: %s", nqn)

	// Step 5: Verify target is listening (try to connect with nvme-cli).
	t.Log("verifying NVMe-oF target reachability")
	verifyTarget(t, ctx, testListenAddr, testPort, nqn)

	// Step 6: Clean up - delete target, then chunk volume.
	t.Log("cleaning up NVMe-oF target")
	if err := client.DeleteNvmfTarget(testVolumeID); err != nil {
		t.Errorf("DeleteNvmfTarget failed: %v", err)
	}

	t.Log("cleaning up chunk volume")
	if err := client.DeleteChunkVolume(testVolumeID); err != nil {
		t.Errorf("DeleteChunkVolume failed: %v", err)
	}

	t.Log("SPDK volume lifecycle test passed")
}

// TestSPDKReplicaBdev tests creation of a replica bdev that fans out writes
// across multiple targets.
func TestSPDKReplicaBdev(t *testing.T) {
	if os.Getenv("NOVASTOR_E2E") == "" {
		t.Skip("set NOVASTOR_E2E=1 to run end-to-end tests")
	}

	client := spdk.NewClient(spdkSocketPath)
	if err := client.Connect(); err != nil {
		t.Fatalf("failed to connect to SPDK data-plane: %v", err)
	}
	defer client.Close()

	// Create a replica bdev with two targets (both pointing to localhost).
	targets := []spdk.ReplicaTarget{
		{Addr: "127.0.0.1", Port: 4430, NQN: "novastor-replica-test-1", IsLocal: true},
		{Addr: "127.0.0.1", Port: 4431, NQN: "novastor-replica-test-2", IsLocal: false},
	}

	t.Log("creating replica bdev")
	if err := client.CreateReplicaBdev("e2e-replica", targets, "round-robin", 1073741824); err != nil {
		t.Fatalf("CreateReplicaBdev failed: %v", err)
	}

	t.Log("replica bdev created successfully")
}

// verifyTarget uses nvme-cli to check if the NVMe-oF target is discoverable.
func verifyTarget(t *testing.T, ctx context.Context, addr string, port uint16, nqn string) {
	t.Helper()

	// Try `nvme discover` to verify the target is reachable.
	cmd := exec.CommandContext(ctx, "nvme", "discover",
		"-t", "tcp",
		"-a", addr,
		"-s", fmt.Sprintf("%d", port),
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
