package csi

import (
	"context"
	"fmt"
	"strconv"

	"github.com/azrtydxb/novastor/internal/spdk"
)

// localLoopbackAddr is the address used for local NVMe-oF loopback exports.
const localLoopbackAddr = "127.0.0.1"

// localLoopbackPort is the port used for local NVMe-oF loopback exports.
const localLoopbackPort uint16 = 4421

// SPDKInitiator implements NVMeInitiator using the SPDK user-space data-plane
// instead of the kernel nvme-cli tooling. It connects to remote NVMe-oF targets
// through the SPDK JSON-RPC client and creates a local loopback export so the
// kernel can discover a /dev/nvmeXnY device.
type SPDKInitiator struct {
	client *spdk.Client
}

// NewSPDKInitiator returns an SPDKInitiator backed by the given SPDK JSON-RPC client.
func NewSPDKInitiator(client *spdk.Client) *SPDKInitiator {
	return &SPDKInitiator{client: client}
}

// Connect attaches to a remote NVMe-oF target via the SPDK data-plane and
// creates a local loopback NVMe-oF export so the kernel can present a block
// device. The returned device path is a placeholder; actual device discovery
// happens through nvme-cli or sysfs after the loopback target is established.
func (s *SPDKInitiator) Connect(_ context.Context, addr, port, nqn string) (string, error) {
	targetPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", fmt.Errorf("parsing target port %q: %w", port, err)
	}

	// Connect to the remote NVMe-oF target through SPDK; this creates a
	// remote bdev inside the data-plane process.
	bdevName, err := s.client.ConnectInitiator(addr, uint16(targetPort), nqn)
	if err != nil {
		return "", fmt.Errorf("spdk connect initiator to %s:%s nqn %s: %w", addr, port, nqn, err)
	}

	// Export the remote bdev as a local NVMe-oF loopback target so the
	// kernel NVMe driver can discover it as a standard block device.
	localNQN := "nqn.2024-01.io.novastor:local-" + nqn
	if err := s.client.ExportLocal(localNQN, localLoopbackAddr, localLoopbackPort, bdevName); err != nil {
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
	localNQN := "nqn.2024-01.io.novastor:local-" + nqn
	if err := s.client.DeleteNvmfTarget(localNQN); err != nil {
		return fmt.Errorf("spdk delete local nvmf target %s: %w", localNQN, err)
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

	basePort := localLoopbackPort
	var connectedNQNs []string
	var exportFailures int

	for i, t := range targets {
		port, err := strconv.ParseUint(t.Port, 10, 16)
		if err != nil {
			port = 4420
		}
		bdevName, err := s.client.ConnectInitiator(t.Addr, uint16(port), t.NQN)
		if err != nil {
			// Cleanup already-connected
			for _, nqn := range connectedNQNs {
				_ = s.client.DisconnectInitiator(nqn)
			}
			return "", fmt.Errorf("connect target %s: %w", t.Addr, err)
		}
		connectedNQNs = append(connectedNQNs, t.NQN)

		// Export each as loopback on sequential ports
		localNQN := fmt.Sprintf("nqn.2024-01.io.novastor:local-%s-path%d", t.NQN, i)
		loopPort := basePort + uint16(i)
		if err := s.client.ExportLocal(localNQN, localLoopbackAddr, loopPort, bdevName); err != nil {
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
		localNQN := fmt.Sprintf("nqn.2024-01.io.novastor:local-%s-path%d", t.NQN, i)
		if err := s.client.DeleteNvmfTarget(localNQN); err != nil {
			lastErr = err
		}
		if err := s.client.DisconnectInitiator(t.NQN); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
