package csi

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"

	"github.com/azrtydxb/novastor/internal/dataplane"
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

	// Find the owner target (primary I/O path).
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

	localVolumeID := "local-" + owner.NQN

	if owner.Addr == s.hostIP {
		// Local: the SPDK NVMe-oF target is already serving this bdev
		// on hostIP:4430 (created by the agent). Connect the kernel
		// directly to the existing target — no SPDK initiator needed,
		// no self-connect, just kernel-to-SPDK NVMe-oF.
		if err := nvmeConnect(owner.Addr, owner.Port, owner.NQN); err != nil {
			return "", fmt.Errorf("connect kernel to local SPDK target %s: %w", owner.Addr, err)
		}
	} else {
		// Remote: connect as SPDK NVMe-oF initiator, then export.
		bdevName, err := s.client.ConnectInitiator(owner.Addr, owner.Port, owner.NQN)
		if err != nil {
			return "", fmt.Errorf("connect target %s: %w", owner.Addr, err)
		}
		if _, err := s.client.ExportLocal(localVolumeID, bdevName); err != nil {
			_ = s.client.DisconnectInitiator(owner.NQN)
			return "", fmt.Errorf("export remote bdev %s: %w", bdevName, err)
		}
	}

	// Discover the kernel block device via sysfs.
	devicePath, err := discoverNVMeDevice(context.Background(), owner.NQN)
	if err != nil {
		return "", fmt.Errorf("discover device for %s: %w", owner.NQN, err)
	}

	return devicePath, nil
}

// DisconnectMultipath tears down the loopback export and any SPDK initiator.
func (s *SPDKInitiator) DisconnectMultipath(_ context.Context, targets []TargetInfo) error {
	var lastErr error
	// Find owner.
	owner := targets[0]
	for _, t := range targets {
		if t.IsOwner {
			owner = t
			break
		}
	}

	if owner.Addr == s.hostIP {
		// Local: kernel connected directly to SPDK target.
		if err := nvmeDisconnect(owner.NQN); err != nil {
			lastErr = err
		}
	} else {
		// Remote: tear down loopback export + SPDK initiator.
		localVolumeID := "local-" + owner.NQN
		if err := s.client.DeleteNvmfTarget(localVolumeID); err != nil {
			lastErr = err
		}
		if err := s.client.DisconnectInitiator(owner.NQN); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// nvmeConnect runs `nvme connect` to attach the kernel to an existing
// SPDK NVMe-oF target. Used for local-node volumes where the SPDK target
// is already serving — avoids SPDK initiator self-connect.
func nvmeConnect(addr, port, nqn string) error {
	cmd := exec.CommandContext(context.Background(), "nvme", "connect",
		"-t", "tcp",
		"-a", addr,
		"-s", port,
		"-n", nqn,
		"-i", "4",
		"-k", "10",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme connect failed: %w: %s", err, string(output))
	}
	return nil
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
