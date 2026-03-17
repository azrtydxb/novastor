package csi

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
//
// For the local node (t.Addr == s.hostIP), the bdev already exists in the
// local SPDK process — we export it directly via loopback without connecting
// as an NVMe-oF initiator (avoids single-reactor self-connect deadlock).
//
// For remote nodes, we connect as an NVMe-oF initiator first, then export
// the resulting bdev via loopback.
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

		localVolumeID := fmt.Sprintf("local-%s-path%d", t.NQN, i)

		if t.Addr == s.hostIP {
			// Local path: the bdev "novastor_<volumeID>" is already
			// registered in our SPDK process. Export it directly as a
			// loopback NVMe-oF target — no ConnectInitiator needed.
			volumeID := extractVolumeID(t.NQN)
			localBdevName := "novastor_" + volumeID
			if _, err := s.client.ExportLocal(localVolumeID, localBdevName); err != nil {
				exportFailures++
			}
		} else {
			// Remote path: connect as NVMe-oF initiator, then export.
			bdevName, err := s.client.ConnectInitiator(t.Addr, t.Port, t.NQN)
			if err != nil {
				for _, nqn := range connectedNQNs {
					_ = s.client.DisconnectInitiator(nqn)
				}
				return "", fmt.Errorf("connect target %s: %w", t.Addr, err)
			}
			connectedNQNs = append(connectedNQNs, t.NQN)

			if _, err := s.client.ExportLocal(localVolumeID, bdevName); err != nil {
				exportFailures++
			}
		}
	}

	if exportFailures == len(targets) {
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

// extractVolumeID extracts the volume UUID from an NQN string.
// NQN format: "nqn.2024-01.io.novastor:volume-<uuid>"
func extractVolumeID(nqn string) string {
	const prefix = "volume-"
	idx := strings.LastIndex(nqn, prefix)
	if idx >= 0 {
		return nqn[idx+len(prefix):]
	}
	// Fallback: return the part after the last colon.
	if colon := strings.LastIndex(nqn, ":"); colon >= 0 {
		return nqn[colon+1:]
	}
	return nqn
}

// DisconnectMultipath tears down all local loopback exports and disconnects
// from remote NVMe-oF targets. Local-node paths only have loopback exports
// (no initiator connection to tear down).
func (s *SPDKInitiator) DisconnectMultipath(_ context.Context, targets []TargetInfo) error {
	var lastErr error
	for i, t := range targets {
		localVolumeID := fmt.Sprintf("local-%s-path%d", t.NQN, i)
		if err := s.client.DeleteNvmfTarget(localVolumeID); err != nil {
			lastErr = err
		}
		// Only disconnect initiator for remote paths — local paths
		// used the existing bdev directly (no initiator connection).
		if t.Addr != s.hostIP {
			if err := s.client.DisconnectInitiator(t.NQN); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}
