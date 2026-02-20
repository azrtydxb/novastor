package csi

import (
	"context"
	"fmt"
	"strconv"

	"github.com/piwi3910/novastor/internal/spdk"
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
