package csi

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/logging"
)

// SPDKInitiator implements NVMeInitiator using the SPDK user-space data-plane.
// It connects to the volume's NVMe-oF target via SPDK's initiator (for remote
// targets) or uses the local bdev directly (for same-node targets), then
// creates a loopback NVMe-oF export so the kernel can present /dev/nvmeXnY.
//
// Replication fan-out is handled internally by the SPDK bdev's chunk engine,
// so the CSI connects to a single target (the owner) — no NVMe multipath.
type SPDKInitiator struct {
	client *dataplane.Client
	hostIP string
}

// NewSPDKInitiator returns an SPDKInitiator backed by the given gRPC dataplane client.
func NewSPDKInitiator(client *dataplane.Client, hostIP string) *SPDKInitiator {
	return &SPDKInitiator{client: client, hostIP: hostIP}
}

// Connect attaches to an NVMe-oF target via SPDK and exports a loopback
// device so the kernel can discover /dev/nvmeXnY.
func (s *SPDKInitiator) Connect(_ context.Context, addr, port, nqn string) (string, error) {
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return "", fmt.Errorf("parsing target port %q: %w", port, err)
	}

	bdevName, err := s.client.ConnectInitiator(addr, port, nqn)
	if err != nil {
		return "", fmt.Errorf("spdk connect initiator to %s:%s nqn %s: %w", addr, port, nqn, err)
	}

	localVolumeID := "local-" + nqn
	if _, err := s.client.ExportLocal(localVolumeID, bdevName); err != nil {
		_ = s.client.DisconnectInitiator(nqn)
		return "", fmt.Errorf("spdk export local for bdev %s: %w", bdevName, err)
	}

	devicePath, err := discoverNVMeDevice(context.Background(), nqn)
	if err != nil {
		return "", fmt.Errorf("discover device for nqn %s: %w", nqn, err)
	}
	return devicePath, nil
}

// Disconnect tears down the loopback export and disconnects the SPDK initiator.
func (s *SPDKInitiator) Disconnect(_ context.Context, nqn string) error {
	localVolumeID := "local-" + nqn
	if err := s.client.DeleteNvmfTarget(localVolumeID); err != nil {
		return fmt.Errorf("spdk delete local nvmf target %s: %w", localVolumeID, err)
	}
	if err := s.client.DisconnectInitiator(nqn); err != nil {
		return fmt.Errorf("spdk disconnect initiator nqn %s: %w", nqn, err)
	}
	return nil
}

// TargetInfo describes a single NVMe-oF target endpoint.
type TargetInfo struct {
	Addr    string `json:"addr"`
	Port    string `json:"port"`
	NQN     string `json:"nqn"`
	IsOwner bool   `json:"is_owner"`
}

// ConnectMultipath connects to the volume's owner target. Replication is
// handled internally by the SPDK bdev's chunk engine, so we connect to a
// single target — the owner node that hosts the volume bdev.
//
// Local target (same node): the bdev already exists in our SPDK process.
// We export it directly via loopback — no NVMe-oF initiator connection,
// no self-connect, no multi-core reactor needed.
//
// Remote target (different node): connect as an SPDK NVMe-oF initiator,
// then export the resulting bdev via loopback.
func (s *SPDKInitiator) ConnectMultipath(_ context.Context, targets []TargetInfo) (string, error) {
	if len(targets) == 0 {
		return "", fmt.Errorf("no targets provided")
	}

	// Clean stale NVMe-oF subsystems from previous volumes.
	cleanStaleNVMeSubsystems()

	// Connect to the owner first, then add replica paths for multipath.
	owner := targets[0]
	for _, t := range targets {
		if t.IsOwner {
			owner = t
			break
		}
	}
	if _, err := strconv.ParseUint(owner.Port, 10, 16); err != nil {
		owner.Port = "4430"
	}
	if err := nvmeConnect(owner.Addr, owner.Port, owner.NQN); err != nil {
		return "", fmt.Errorf("connect to owner target %s: %w", owner.Addr, err)
	}

	// Connect remaining replica paths — NVMe multipath groups them
	// under the same subsystem NQN automatically.
	for _, t := range targets {
		if t.Addr == owner.Addr {
			continue // Already connected.
		}
		port := t.Port
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			port = "4430"
		}
		if err := nvmeConnect(t.Addr, port, t.NQN); err != nil {
			// Non-fatal — multipath works with fewer paths.
			logging.L.Warn("multipath: failed to connect replica path",
				zap.String("addr", t.Addr),
				zap.Error(err))
		}
	}

	devicePath, err := discoverNVMeDevice(context.Background(), owner.NQN)
	if err != nil {
		return "", fmt.Errorf("discover device for %s: %w", owner.NQN, err)
	}
	return devicePath, nil
}

// DisconnectMultipath disconnects all NVMe-oF paths.
func (s *SPDKInitiator) DisconnectMultipath(_ context.Context, targets []TargetInfo) error {
	if len(targets) == 0 {
		return nil
	}
	// All targets share the same NQN. One disconnect-by-NQN removes all paths.
	return nvmeDisconnect(targets[0].NQN)
}

// cleanStaleNVMeSubsystems disconnects all NovaStor NVMe-oF subsystems
// that have no live controllers. These zombie entries accumulate when
// volumes are deleted or dataplanes restart, and can prevent new connects.
func cleanStaleNVMeSubsystems() {
	// Read all subsystems from sysfs
	entries, err := os.ReadDir("/sys/class/nvme-subsystem")
	if err != nil {
		return
	}
	for _, entry := range entries {
		subsysDir := "/sys/class/nvme-subsystem/" + entry.Name()

		// Check if it's a NovaStor subsystem
		nqnData, err := os.ReadFile(subsysDir + "/subsysnqn")
		if err != nil {
			continue
		}
		nqn := strings.TrimSpace(string(nqnData))
		if !strings.Contains(nqn, "novastor") {
			continue
		}

		// Check if any controllers are live
		ctrls, _ := filepath.Glob(subsysDir + "/nvme*")
		hasLive := false
		for _, ctrl := range ctrls {
			state, err := os.ReadFile(ctrl + "/state")
			if err == nil && strings.TrimSpace(string(state)) == "live" {
				hasLive = true
				break
			}
		}

		// Disconnect stale subsystems with no live controllers
		if !hasLive {
			_ = nvmeDisconnect(nqn)
		}
	}
}

// nvmeConnect runs `nvme connect` to attach the kernel to an existing
// SPDK NVMe-oF target. Used for local-node volumes where the SPDK target
// is already serving — avoids SPDK initiator self-connect.
func nvmeConnect(addr, port, nqn string) error {
	args := []string{"connect",
		"-t", "tcp",
		"-a", addr,
		"-s", port,
		"-n", nqn,
		"-i", fmt.Sprintf("%d", runtime.NumCPU()),
		"-k", "10",
	}
	// Use a stable hostnqn so the kernel groups all paths to the same
	// NQN under a single multipath subsystem.
	if hostnqn := getHostNQN(); hostnqn != "" {
		args = append(args, "-q", hostnqn)
	}
	cmd := exec.CommandContext(context.Background(), "nvme", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme connect failed: %w: %s", err, string(output))
	}
	return nil
}

// getHostNQN returns a stable host NQN for this node. All nvme connect
// calls must use the same hostnqn so the kernel groups them as paths
// under a single multipath subsystem.
// EnsureHostNQN writes a stable /etc/nvme/hostnqn if it doesn't exist.
// Must be called once at CSI startup, before any nvme connect calls.
func EnsureHostNQN() {
	_ = getHostNQN()
}

func getHostNQN() string {
	// Try the system hostnqn first.
	if data, err := os.ReadFile("/etc/nvme/hostnqn"); err == nil {
		if nqn := strings.TrimSpace(string(data)); nqn != "" {
			return nqn
		}
	}
	// Generate a deterministic UUID-format hostnqn from hostname and
	// persist it so subsequent calls (and kernel) use the same value.
	hostname, _ := os.Hostname()
	h := md5.Sum([]byte("novastor-" + hostname))
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
	nqn := "nqn.2014-08.org.nvmexpress:uuid:" + uuid
	_ = os.MkdirAll("/etc/nvme", 0755)
	_ = os.WriteFile("/etc/nvme/hostnqn", []byte(nqn+"\n"), 0644)
	// Write stable hostid too — the kernel uses this to identify the host
	// across multiple nvme connect calls. Without it, each connect generates
	// a random hostid and multipath can't group paths.
	_ = os.WriteFile("/etc/nvme/hostid", []byte(uuid+"\n"), 0644)
	return nqn
}

// nvmeDisconnect runs `nvme disconnect` by NQN.
func nvmeDisconnect(nqn string) error {
	cmd := exec.CommandContext(context.Background(), "nvme", "disconnect", "-n", nqn)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme disconnect failed: %w: %s", err, string(output))
	}
	return nil
}
