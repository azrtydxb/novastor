package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// NVMeInitiator abstracts NVMe-oF connect/disconnect operations for testability.
type NVMeInitiator interface {
	// Connect connects to an NVMe-oF target and returns the device path.
	Connect(ctx context.Context, addr, port, nqn string) (devicePath string, err error)
	// Disconnect disconnects from an NVMe-oF target by subsystem NQN.
	Disconnect(ctx context.Context, nqn string) error
}

// StubInitiator is a test-friendly NVMe initiator that creates directories
// as stand-ins for block devices.
type StubInitiator struct {
	// BasePath is the directory under which stub devices are created.
	BasePath string
}

// Connect creates a stub directory to simulate an NVMe-oF device attachment.
func (s *StubInitiator) Connect(_ context.Context, addr, port, nqn string) (string, error) {
	devicePath := filepath.Join(s.BasePath, "nvme-"+nqn)
	if err := os.MkdirAll(devicePath, 0750); err != nil {
		return "", fmt.Errorf("creating stub device dir: %w", err)
	}
	// Write a marker file to indicate the device is connected.
	markerPath := filepath.Join(devicePath, "connected")
	if err := os.WriteFile(markerPath, []byte(fmt.Sprintf("%s:%s", addr, port)), 0600); err != nil {
		return "", fmt.Errorf("writing connection marker: %w", err)
	}
	return devicePath, nil
}

// Disconnect removes the stub directory to simulate an NVMe-oF device detachment.
func (s *StubInitiator) Disconnect(_ context.Context, nqn string) error {
	devicePath := filepath.Join(s.BasePath, "nvme-"+nqn)
	return os.RemoveAll(devicePath)
}

// LinuxInitiator uses the nvme-cli tool to connect to NVMe-oF targets.
// This is the production implementation for Linux hosts.
type LinuxInitiator struct{}

// Connect runs `nvme connect` to attach an NVMe-oF target over TCP.
func (l *LinuxInitiator) Connect(ctx context.Context, addr, port, nqn string) (string, error) {
	cmd := exec.CommandContext(ctx, "nvme", "connect",
		"-t", "tcp",
		"-a", addr,
		"-s", port,
		"-n", nqn,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("nvme connect failed: %w: %s", err, string(output))
	}
	// The device path is typically /dev/nvmeXnY. In a real implementation,
	// we would parse the nvme-cli output or scan /sys to find the new device.
	// For now, return a conventional path.
	return fmt.Sprintf("/dev/nvme-fabrics/%s", nqn), nil
}

// Disconnect runs `nvme disconnect` to detach an NVMe-oF target.
func (l *LinuxInitiator) Disconnect(ctx context.Context, nqn string) error {
	cmd := exec.CommandContext(ctx, "nvme", "disconnect", "-n", nqn)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme disconnect failed: %w: %s", err, string(output))
	}
	return nil
}
