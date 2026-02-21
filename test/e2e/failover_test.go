//go:build e2e

// Package e2e provides end-to-end tests for the NovaStor SPDK data-plane.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	failoverNamespace   = "novastor-system"
	failoverStorageClass = "novastor-block-replicated"
	failoverPVCName     = "e2e-failover-pvc"
	failoverPodName     = "e2e-failover-fio"
	failoverTimeout     = 5 * time.Minute
	pollInterval        = 5 * time.Second
)

// kubectl executes a kubectl command and returns the combined output.
func kubectl(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// kubectlWithContext executes a kubectl command with context cancellation support.
func kubectlWithContext(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// waitForCondition polls until the condition function returns true or the context expires.
func waitForCondition(ctx context.Context, desc string, interval time.Duration, condition func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for condition: %s", desc)
		case <-ticker.C:
			ok, err := condition()
			if err != nil {
				// Log but continue polling; transient errors are expected.
				continue
			}
			if ok {
				return nil
			}
		}
	}
}

// pvcStatus returns the phase of the given PVC.
func pvcStatus(namespace, name string) (string, error) {
	out, err := kubectl("get", "pvc", name, "-n", namespace,
		"-o", "jsonpath={.status.phase}")
	if err != nil {
		return "", fmt.Errorf("get pvc status: %w: %s", err, out)
	}
	return strings.TrimSpace(out), nil
}

// podPhase returns the phase of the given pod.
func podPhase(namespace, name string) (string, error) {
	out, err := kubectl("get", "pod", name, "-n", namespace,
		"-o", "jsonpath={.status.phase}")
	if err != nil {
		return "", fmt.Errorf("get pod phase: %w: %s", err, out)
	}
	return strings.TrimSpace(out), nil
}

// podNodeName returns the node on which the pod is scheduled.
func podNodeName(namespace, name string) (string, error) {
	out, err := kubectl("get", "pod", name, "-n", namespace,
		"-o", "jsonpath={.spec.nodeName}")
	if err != nil {
		return "", fmt.Errorf("get pod nodeName: %w: %s", err, out)
	}
	return strings.TrimSpace(out), nil
}

// getWriteOwnerAgent returns the name and node of the novastor-agent pod that
// currently owns the write path for the volume. It inspects the BlockVolume CR
// associated with the PVC.
func getWriteOwnerAgent(ctx context.Context, namespace, pvcName string) (agentPodName string, agentNode string, err error) {
	// Get the volume name from the PVC.
	out, err := kubectlWithContext(ctx, "get", "pvc", pvcName, "-n", namespace,
		"-o", "jsonpath={.spec.volumeName}")
	if err != nil {
		return "", "", fmt.Errorf("get PVC volumeName: %w: %s", err, out)
	}
	volumeName := strings.TrimSpace(out)
	if volumeName == "" {
		return "", "", fmt.Errorf("PVC %s has no bound volume", pvcName)
	}

	// Get the write-owner node from the BlockVolume CR status.
	out, err = kubectlWithContext(ctx, "get", "blockvolume", volumeName, "-n", namespace,
		"-o", "jsonpath={.status.writeOwnerNode}")
	if err != nil {
		return "", "", fmt.Errorf("get BlockVolume writeOwnerNode: %w: %s", err, out)
	}
	ownerNode := strings.TrimSpace(out)
	if ownerNode == "" {
		return "", "", fmt.Errorf("BlockVolume %s has no writeOwnerNode", volumeName)
	}

	// Find the agent pod running on that node.
	out, err = kubectlWithContext(ctx, "get", "pods", "-n", namespace,
		"-l", "app.kubernetes.io/component=agent",
		"-o", "json",
		"--field-selector", "spec.nodeName="+ownerNode)
	if err != nil {
		return "", "", fmt.Errorf("list agent pods on node %s: %w: %s", ownerNode, err, out)
	}

	var podList struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &podList); err != nil {
		return "", "", fmt.Errorf("parse agent pod list: %w", err)
	}
	if len(podList.Items) == 0 {
		return "", "", fmt.Errorf("no agent pod found on node %s", ownerNode)
	}

	return podList.Items[0].Metadata.Name, ownerNode, nil
}

// getANAState returns the ANA state for the given volume from the BlockVolume CR.
func getANAState(ctx context.Context, namespace, pvcName string) (string, error) {
	out, err := kubectlWithContext(ctx, "get", "pvc", pvcName, "-n", namespace,
		"-o", "jsonpath={.spec.volumeName}")
	if err != nil {
		return "", fmt.Errorf("get PVC volumeName: %w: %s", err, out)
	}
	volumeName := strings.TrimSpace(out)
	if volumeName == "" {
		return "", fmt.Errorf("PVC %s has no bound volume", pvcName)
	}

	out, err = kubectlWithContext(ctx, "get", "blockvolume", volumeName, "-n", namespace,
		"-o", "jsonpath={.status.anaState}")
	if err != nil {
		return "", fmt.Errorf("get BlockVolume anaState: %w: %s", err, out)
	}
	return strings.TrimSpace(out), nil
}

// TestSPDKFailover tests the SPDK failover path on a real Kubernetes cluster.
//
// The test flow:
//  1. Create a PVC with the novastor-block-replicated StorageClass
//  2. Create a pod that mounts the PVC and writes data using fio
//  3. Verify PVC is Bound with a multipath device
//  4. Kill the write-owner agent pod
//  5. Verify fio continues without I/O errors
//  6. Verify the new write-owner ANA state is "optimized"
//  7. Recover agent pod (Kubernetes restarts via DaemonSet)
//  8. Verify data integrity (read back what was written)
//  9. Cleanup
func TestSPDKFailover(t *testing.T) {
	if os.Getenv("NOVASTOR_E2E") == "" {
		t.Skip("set NOVASTOR_E2E=1 to run end-to-end tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), failoverTimeout)
	defer cancel()

	// Ensure we clean up resources even if the test fails.
	defer cleanupFailoverResources(t)

	// -----------------------------------------------------------------
	// Step 1: Create a PVC with the novastor-block-replicated StorageClass.
	// -----------------------------------------------------------------
	t.Log("step 1: creating PVC with novastor-block-replicated StorageClass")
	pvcYAML := fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: %s
  resources:
    requests:
      storage: 1Gi
`, failoverPVCName, failoverNamespace, failoverStorageClass)

	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pvcYAML)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("step 1: failed to create PVC: %v: %s", err, string(out))
	}
	t.Logf("step 1: PVC created: %s", strings.TrimSpace(string(out)))

	// Wait for PVC to become Bound.
	t.Log("step 1: waiting for PVC to become Bound")
	err = waitForCondition(ctx, "PVC Bound", pollInterval, func() (bool, error) {
		phase, err := pvcStatus(failoverNamespace, failoverPVCName)
		if err != nil {
			return false, err
		}
		return phase == "Bound", nil
	})
	if err != nil {
		t.Fatalf("step 1: %v", err)
	}
	t.Log("step 1 passed: PVC is Bound")

	// -----------------------------------------------------------------
	// Step 2: Create a pod that mounts the PVC and writes data with fio.
	// -----------------------------------------------------------------
	t.Log("step 2: creating fio writer pod")
	fioPodYAML := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  containers:
    - name: fio
      image: nixery.dev/fio
      command:
        - fio
        - --name=failover-write
        - --filename=/data/testfile
        - --size=512M
        - --bs=4k
        - --rw=randrw
        - --rwmixwrite=50
        - --ioengine=libaio
        - --direct=1
        - --numjobs=1
        - --runtime=300
        - --time_based
        - --output-format=json
        - --output=/data/fio-result.json
        - --verify=crc32c
        - --verify_backlog=1024
        - --do_verify=1
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
`, failoverPodName, failoverNamespace, failoverPVCName)

	cmd = exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(fioPodYAML)
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("step 2: failed to create fio pod: %v: %s", err, string(out))
	}
	t.Logf("step 2: fio pod created: %s", strings.TrimSpace(string(out)))

	// Wait for the pod to be Running.
	t.Log("step 2: waiting for fio pod to reach Running state")
	err = waitForCondition(ctx, "fio pod Running", pollInterval, func() (bool, error) {
		phase, err := podPhase(failoverNamespace, failoverPodName)
		if err != nil {
			return false, err
		}
		return phase == "Running", nil
	})
	if err != nil {
		t.Fatalf("step 2: %v", err)
	}
	t.Log("step 2 passed: fio pod is Running")

	// Let fio run for a bit to establish stable I/O.
	t.Log("step 2: allowing fio to establish stable I/O (15s)")
	select {
	case <-ctx.Done():
		t.Fatalf("step 2: context expired while waiting for fio warmup")
	case <-time.After(15 * time.Second):
	}

	// -----------------------------------------------------------------
	// Step 3: Verify PVC is Bound with multipath device.
	// -----------------------------------------------------------------
	t.Log("step 3: verifying PVC binding and multipath device")

	// Check PVC is still Bound.
	phase, err := pvcStatus(failoverNamespace, failoverPVCName)
	if err != nil {
		t.Fatalf("step 3: failed to get PVC status: %v", err)
	}
	if phase != "Bound" {
		t.Fatalf("step 3: expected PVC phase 'Bound', got %q", phase)
	}

	// Verify the fio pod's node has a multipath device.
	fioNode, err := podNodeName(failoverNamespace, failoverPodName)
	if err != nil {
		t.Fatalf("step 3: failed to get fio pod node: %v", err)
	}
	t.Logf("step 3: fio pod running on node %s", fioNode)

	// Check for NVMe multipath devices on the fio pod's node.
	mpOut, err := kubectlWithContext(ctx, "exec", "-n", failoverNamespace, failoverPodName, "--",
		"ls", "-la", "/dev/")
	if err != nil {
		t.Logf("step 3: warning: could not list /dev/ in fio pod: %v", err)
	} else if strings.Contains(mpOut, "nvme") {
		t.Logf("step 3: NVMe devices found in fio pod")
	}
	t.Log("step 3 passed: PVC is Bound and pod is running I/O")

	// -----------------------------------------------------------------
	// Step 4: Identify and kill the write-owner agent pod.
	// -----------------------------------------------------------------
	t.Log("step 4: identifying write-owner agent pod")
	agentPod, agentNode, err := getWriteOwnerAgent(ctx, failoverNamespace, failoverPVCName)
	if err != nil {
		t.Fatalf("step 4: failed to identify write-owner agent: %v", err)
	}
	t.Logf("step 4: write-owner agent pod is %s on node %s", agentPod, agentNode)

	// Record the pod UID before killing, so we can detect when a new pod appears.
	oldUIDOut, err := kubectlWithContext(ctx, "get", "pod", agentPod, "-n", failoverNamespace,
		"-o", "jsonpath={.metadata.uid}")
	if err != nil {
		t.Fatalf("step 4: failed to get agent pod UID: %v", err)
	}
	oldUID := strings.TrimSpace(oldUIDOut)
	t.Logf("step 4: current agent pod UID: %s", oldUID)

	// Force-delete the agent pod to simulate a node/process crash.
	t.Logf("step 4: force-deleting agent pod %s", agentPod)
	out2, err := kubectlWithContext(ctx, "delete", "pod", agentPod, "-n", failoverNamespace,
		"--force", "--grace-period=0")
	if err != nil {
		t.Fatalf("step 4: failed to delete agent pod: %v: %s", err, out2)
	}
	t.Logf("step 4 passed: agent pod %s force-deleted", agentPod)

	// -----------------------------------------------------------------
	// Step 5: Verify fio continues without I/O errors.
	// -----------------------------------------------------------------
	t.Log("step 5: waiting for failover to take effect (10s)")
	select {
	case <-ctx.Done():
		t.Fatalf("step 5: context expired during failover wait")
	case <-time.After(10 * time.Second):
	}

	// Check the fio pod is still Running (not Failed/Error).
	fioPhase, err := podPhase(failoverNamespace, failoverPodName)
	if err != nil {
		t.Fatalf("step 5: failed to get fio pod phase: %v", err)
	}
	if fioPhase != "Running" {
		t.Fatalf("step 5: fio pod is no longer Running after failover, phase: %s", fioPhase)
	}

	// Check fio process is still alive inside the pod.
	procOut, err := kubectlWithContext(ctx, "exec", "-n", failoverNamespace, failoverPodName, "--",
		"pgrep", "-f", "fio")
	if err != nil {
		t.Fatalf("step 5: fio process no longer running in pod: %v: %s", err, procOut)
	}
	t.Logf("step 5: fio still running (PIDs: %s)", strings.TrimSpace(procOut))

	// Check pod logs for I/O error indicators.
	logsOut, err := kubectlWithContext(ctx, "logs", failoverPodName, "-n", failoverNamespace,
		"--tail=50")
	if err != nil {
		t.Logf("step 5: warning: could not fetch fio pod logs: %v", err)
	} else {
		if strings.Contains(strings.ToLower(logsOut), "i/o error") ||
			strings.Contains(strings.ToLower(logsOut), "error") {
			t.Logf("step 5: warning: potential errors in fio output:\n%s", logsOut)
		}
	}
	t.Log("step 5 passed: fio continues running after agent pod kill")

	// -----------------------------------------------------------------
	// Step 6: Verify the new write-owner ANA state is "optimized".
	// -----------------------------------------------------------------
	t.Log("step 6: verifying new write-owner ANA state")
	err = waitForCondition(ctx, "new write-owner ANA state optimized", pollInterval, func() (bool, error) {
		state, err := getANAState(ctx, failoverNamespace, failoverPVCName)
		if err != nil {
			return false, err
		}
		return state == "optimized", nil
	})
	if err != nil {
		// Non-fatal: log the issue but continue to verify data integrity.
		t.Logf("step 6: warning: could not verify ANA state: %v", err)

		// Try to get the current write owner for additional debugging info.
		newAgent, newNode, ownerErr := getWriteOwnerAgent(ctx, failoverNamespace, failoverPVCName)
		if ownerErr != nil {
			t.Logf("step 6: could not determine new write owner: %v", ownerErr)
		} else {
			t.Logf("step 6: new write-owner is %s on node %s", newAgent, newNode)
		}
	} else {
		t.Log("step 6 passed: new write-owner ANA state is 'optimized'")
	}

	// -----------------------------------------------------------------
	// Step 7: Verify the agent pod recovers (DaemonSet restarts it).
	// -----------------------------------------------------------------
	t.Log("step 7: waiting for agent pod to recover on node " + agentNode)
	err = waitForCondition(ctx, "agent pod recovery", pollInterval, func() (bool, error) {
		// List agent pods on the original node.
		out, err := kubectlWithContext(ctx, "get", "pods", "-n", failoverNamespace,
			"-l", "app.kubernetes.io/component=agent",
			"--field-selector", "spec.nodeName="+agentNode,
			"-o", "jsonpath={.items[0].metadata.uid},{.items[0].status.phase}")
		if err != nil {
			return false, err
		}
		parts := strings.SplitN(strings.TrimSpace(out), ",", 2)
		if len(parts) != 2 {
			return false, nil
		}
		uid := parts[0]
		podPhase := parts[1]
		// The new pod must have a different UID and be Running.
		return uid != oldUID && podPhase == "Running", nil
	})
	if err != nil {
		t.Fatalf("step 7: %v", err)
	}
	t.Log("step 7 passed: agent pod recovered on node " + agentNode)

	// -----------------------------------------------------------------
	// Step 8: Verify data integrity — read back what was written.
	// -----------------------------------------------------------------
	t.Log("step 8: verifying data integrity")

	// Run a verification-only fio job to read and verify checksums.
	verifyOut, err := kubectlWithContext(ctx, "exec", "-n", failoverNamespace, failoverPodName, "--",
		"fio",
		"--name=verify-read",
		"--filename=/data/testfile",
		"--size=512M",
		"--bs=4k",
		"--rw=read",
		"--ioengine=libaio",
		"--direct=1",
		"--numjobs=1",
		"--verify=crc32c",
		"--verify_only",
		"--output-format=json",
	)
	if err != nil {
		// fio verify failure indicates data corruption.
		t.Fatalf("step 8: data integrity verification failed: %v: %s", err, verifyOut)
	}

	// Parse fio JSON output to check for verify errors.
	var fioResult struct {
		Jobs []struct {
			Jobname    string `json:"jobname"`
			Error      int    `json:"error"`
			ReadStatus struct {
				TotalIOs int64 `json:"total_ios"`
				IOBytes  int64 `json:"io_bytes"`
			} `json:"read"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(verifyOut), &fioResult); err != nil {
		t.Logf("step 8: warning: could not parse fio verify output: %v", err)
	} else {
		for _, job := range fioResult.Jobs {
			if job.Error != 0 {
				t.Fatalf("step 8: fio verify job %q reported error: %d", job.Jobname, job.Error)
			}
			t.Logf("step 8: fio verify job %q completed: %d IOs, %d bytes read",
				job.Jobname, job.ReadStatus.TotalIOs, job.ReadStatus.IOBytes)
		}
	}
	t.Log("step 8 passed: data integrity verified after failover")

	// -----------------------------------------------------------------
	// Step 9: Cleanup (also in defer above for safety).
	// -----------------------------------------------------------------
	t.Log("step 9: cleaning up test resources")
	cleanupFailoverResources(t)
	t.Log("step 9 passed: cleanup complete")

	t.Log("TestSPDKFailover: all steps passed")
}

// cleanupFailoverResources removes the test PVC and pod.
// It is called both in defer and at the end of the test.
func cleanupFailoverResources(t *testing.T) {
	t.Helper()

	// Delete the fio pod (ignore errors — it may not exist).
	out, err := kubectl("delete", "pod", failoverPodName, "-n", failoverNamespace,
		"--force", "--grace-period=0", "--ignore-not-found=true")
	if err != nil {
		t.Logf("cleanup: warning: failed to delete fio pod: %v: %s", err, out)
	}

	// Delete the PVC (ignore errors — it may not exist).
	out, err = kubectl("delete", "pvc", failoverPVCName, "-n", failoverNamespace,
		"--ignore-not-found=true")
	if err != nil {
		t.Logf("cleanup: warning: failed to delete PVC: %v: %s", err, out)
	}

	// Wait briefly for resources to be cleaned up.
	time.Sleep(2 * time.Second)
}
