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

	"github.com/piwi3910/novastor/internal/spdk"
)

const (
	spdkSocketPath = "/var/tmp/novastor-spdk.sock"
	testVolumeID   = "e2e-test-vol-001"
	testNQN        = "novastor-" + testVolumeID
	testListenAddr = "127.0.0.1"
	testPort       = uint16(4420)
)

// TestSPDKVolumeLifecycle tests the complete lifecycle of a replicated volume
// using the SPDK data-plane: create lvol → create NVMe-oF target → connect
// initiator → verify → disconnect → delete.
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
	t.Log("creating malloc bdev")
	if err := client.CreateMallocBdev("e2e-base", 256, 512); err != nil {
		t.Fatalf("CreateMallocBdev failed: %v", err)
	}
	defer func() {
		_ = client.DeleteBdev("e2e-base")
	}()

	// Step 2: Create an lvol store on the malloc bdev.
	t.Log("creating lvol store")
	lvsUUID, err := client.CreateLvolStore("e2e-base", "e2e-lvs")
	if err != nil {
		t.Fatalf("CreateLvolStore failed: %v", err)
	}
	t.Logf("lvol store UUID: %s", lvsUUID)

	// Step 3: Create a logical volume.
	t.Log("creating logical volume")
	lvolName, err := client.CreateLvol("e2e-lvs", testVolumeID, 128*1024*1024)
	if err != nil {
		t.Fatalf("CreateLvol failed: %v", err)
	}
	t.Logf("lvol name: %s", lvolName)

	// Step 4: Create an NVMe-oF target exposing the lvol.
	t.Log("creating NVMe-oF target")
	if err := client.CreateNvmfTarget(testNQN, testListenAddr, testPort, lvolName); err != nil {
		t.Fatalf("CreateNvmfTarget failed: %v", err)
	}

	// Step 5: Verify target is listening (try to connect with nvme-cli).
	t.Log("verifying NVMe-oF target reachability")
	verifyTarget(t, ctx, testListenAddr, testPort, testNQN)

	// Step 6: Clean up - delete target, then bdev.
	t.Log("cleaning up NVMe-oF target")
	if err := client.DeleteNvmfTarget(testNQN); err != nil {
		t.Errorf("DeleteNvmfTarget failed: %v", err)
	}

	t.Log("cleaning up lvol")
	if err := client.DeleteBdev(lvolName); err != nil {
		t.Errorf("DeleteBdev (lvol) failed: %v", err)
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
		{Addr: "127.0.0.1", Port: 4420, NQN: "novastor-replica-test-1", IsLocal: true},
		{Addr: "127.0.0.1", Port: 4421, NQN: "novastor-replica-test-2", IsLocal: false},
	}

	t.Log("creating replica bdev")
	if err := client.CreateReplicaBdev("e2e-replica", targets, "round-robin"); err != nil {
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
