package csi

import (
	"context"
	"fmt"
	"strconv"

	"github.com/azrtydxb/novastor/internal/dataplane"
)

// SPDKInitiator implements NVMeInitiator using the SPDK user-space data-plane
// instead of the kernel nvme-cli tooling. It connects to remote NVMe-oF targets
// through the gRPC dataplane client and creates a local loopback export so the
// kernel can discover a /dev/nvmeXnY device.
type SPDKInitiator struct {
	client *dataplane.Client
	hostIP string // local node IP — connections to this address are skipped
}

// NewSPDKInitiator returns an SPDKInitiator backed by the given gRPC dataplane client.
// hostIP is the local node's IP address (retained for future path-preference hints).
func NewSPDKInitiator(client *dataplane.Client, hostIP string) *SPDKInitiator {
	return &SPDKInitiator{client: client, hostIP: hostIP}
}

// Connect attaches to a remote NVMe-oF target via the SPDK data-plane and
// creates a local loopback NVMe-oF export so the kernel can present a block
// device. The returned device path is a placeholder; actual device discovery
// happens through nvme-cli or sysfs after the loopback target is established.
func (s *SPDKInitiator) Connect(_ context.Context, addr, port, nqn string) (string, error) {
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return "", fmt.Errorf("parsing target port %q: %w", port, err)
	}

	// Connect to the remote NVMe-oF target through SPDK; this creates a
	// remote bdev inside the data-plane process.
	bdevName, err := s.client.ConnectInitiator(addr, port, nqn)
	if err != nil {
		return "", fmt.Errorf("spdk connect initiator to %s:%s nqn %s: %w", addr, port, nqn, err)
	}

	// Export the remote bdev as a local NVMe-oF loopback target so the
	// kernel NVMe driver can discover it as a standard block device.
	localVolumeID := "local-" + nqn
	if _, err := s.client.ExportLocal(localVolumeID, bdevName); err != nil {
		// Best-effort cleanup: disconnect the initiator we just connected.
		_ = s.client.DisconnectInitiator(nqn)
		return "", fmt.Errorf("spdk export local for bdev %s: %w", bdevName, err)
	}

	// The actual NVMe device path (e.g. /dev/nvme1n1) is discovered by the
	// kernel after it connects to the loopback target. Return a placeholder
	// path; the caller is expected to use nvme-cli discovery or sysfs polling
	// to find the real device.
	return "/dev/nvme-fabrics", nil
}

// Disconnect tears down the local loopback export and disconnects from the
// remote NVMe-oF target through the SPDK data-plane.
func (s *SPDKInitiator) Disconnect(_ context.Context, nqn string) error {
	// Remove the local loopback NVMe-oF target first so the kernel releases
	// the block device before we disconnect the remote initiator session.
	localVolumeID := "local-" + nqn
	if err := s.client.DeleteNvmfTarget(localVolumeID); err != nil {
		return fmt.Errorf("spdk delete local nvmf target %s: %w", localVolumeID, err)
	}

	if err := s.client.DisconnectInitiator(nqn); err != nil {
		return fmt.Errorf("spdk disconnect initiator nqn %s: %w", nqn, err)
	}

	return nil
}

// TargetInfo describes a single NVMe-oF target endpoint for multipath.
type TargetInfo struct {
	Addr    string `json:"addr"`
	Port    string `json:"port"`
	NQN     string `json:"nqn"`
	IsOwner bool   `json:"is_owner"`
}

// ConnectMultipath connects to multiple NVMe-oF targets and exports each as
// a local loopback target so the kernel NVMe multipath layer aggregates them.
func (s *SPDKInitiator) ConnectMultipath(_ context.Context, targets []TargetInfo) (string, error) {
	if len(targets) == 0 {
		return "", fmt.Errorf("no targets provided")
	}

	var connectedNQNs []string
	var exportFailures int

	for i, t := range targets {
		if _, err := strconv.ParseUint(t.Port, 10, 16); err != nil {
			t.Port = "4430"
		}
		_, err := s.client.ConnectInitiator(t.Addr, t.Port, t.NQN)
		if err != nil {
			// Cleanup already-connected
			for _, nqn := range connectedNQNs {
				_ = s.client.DisconnectInitiator(nqn)
			}
			return "", fmt.Errorf("connect target %s: %w", t.Addr, err)
		}
		connectedNQNs = append(connectedNQNs, t.NQN)

		// Export each as loopback
		localVolumeID := fmt.Sprintf("local-%s-path%d", t.NQN, i)
		bdevName := fmt.Sprintf("initiator_%s_path%d", t.NQN, i)
		if _, err := s.client.ExportLocal(localVolumeID, bdevName); err != nil {
			// Non-fatal for individual paths, but track failures.
			exportFailures++
		}
	}

	if exportFailures == len(targets) {
		// Cleanup all connections since no paths could be exported.
		for _, nqn := range connectedNQNs {
			_ = s.client.DisconnectInitiator(nqn)
		}
		return "", fmt.Errorf("all %d NVMe-oF path exports failed", len(targets))
	}

	// Discover multipath device via sysfs
	devicePath, err := discoverMultipathDevice(targets[0].NQN)
	if err != nil {
		return "", fmt.Errorf("discover multipath device: %w", err)
	}

	return devicePath, nil
}

// DisconnectMultipath tears down all local loopback exports and disconnects
// from all remote NVMe-oF targets.
func (s *SPDKInitiator) DisconnectMultipath(_ context.Context, targets []TargetInfo) error {
	var lastErr error
	for i, t := range targets {
		localVolumeID := fmt.Sprintf("local-%s-path%d", t.NQN, i)
		if err := s.client.DeleteNvmfTarget(localVolumeID); err != nil {
			lastErr = err
		}
		if err := s.client.DisconnectInitiator(t.NQN); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
