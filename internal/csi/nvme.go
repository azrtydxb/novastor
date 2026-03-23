package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
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

// nvmeFabricsRoot is the sysfs path for NVMe fabrics controllers.
const nvmeFabricsRoot = "/sys/class/nvme"

// Connect runs `nvme connect` to attach an NVMe-oF target over TCP and
// discovers the actual block device path by scanning sysfs.
func (l *LinuxInitiator) Connect(ctx context.Context, addr, port, nqn string) (string, error) {
	// Clean stale NVMe-oF subsystems before connecting.
	cleanStaleNVMeSubsystems()

	// Use as many I/O queues as online CPUs for maximum parallelism.
	nrQueues := runtime.NumCPU()
	if nrQueues < 1 {
		nrQueues = 1
	}
	cmd := exec.CommandContext(ctx, "nvme", "connect",
		"-t", "tcp",
		"-a", addr,
		"-s", port,
		"-n", nqn,
		"-i", strconv.Itoa(nrQueues),
		"-k", "10", // 10-second keep-alive timeout
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("nvme connect failed: %w: %s", err, string(output))
	}

	// Poll sysfs until the new controller appears (up to 10 seconds).
	devicePath, err := discoverNVMeDevice(ctx, nqn)
	if err != nil {
		return "", fmt.Errorf("discovering nvme device for nqn %s: %w", nqn, err)
	}
	return devicePath, nil
}

// discoverNVMeDevice scans /sys/class/nvme for a controller whose subsystem
// NQN matches the requested nqn, then derives the block device path from the
// controller name (e.g. nvme0 -> /dev/nvme0n1).
func discoverNVMeDevice(ctx context.Context, nqn string) (string, error) {
	const (
		pollInterval = 200 * time.Millisecond
		timeout      = 10 * time.Second
	)

	deadline := time.Now().Add(timeout)
	for {
		path, err := findNVMeDeviceByNQN(nqn)
		if err == nil {
			return path, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out waiting for nvme device with nqn %s", nqn)
		}
		time.Sleep(pollInterval)
	}
}

// findNVMeDeviceByNQN reads /sys/class/nvme/*/subsysnqn files to find a
// controller whose NQN matches, then returns the first namespace device path.
func findNVMeDeviceByNQN(nqn string) (string, error) {
	entries, err := os.ReadDir(nvmeFabricsRoot)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", nvmeFabricsRoot, err)
	}

	for _, entry := range entries {
		ctrlName := entry.Name() // e.g. "nvme0"
		nqnFile := filepath.Join(nvmeFabricsRoot, ctrlName, "subsysnqn")
		data, err := os.ReadFile(nqnFile)
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(data)) != nqn {
			continue
		}

		// Derive the block device path from the controller name.
		// NVMe-TCP controllers may expose the block device as:
		//   /dev/<ctrlName>n1           (single-path, e.g. nvme0n1)
		//   /dev/<ctrlName>n1 via multipath (kernel uses nvme-subsystem symlink)
		// The canonical namespace block device is always /dev/<ctrlName>n1.
		devPath := filepath.Join("/dev", ctrlName+"n1")
		if _, statErr := os.Stat(devPath); statErr == nil {
			return devPath, nil
		}

		// Controller found but namespace not yet visible in /dev.
		return "", fmt.Errorf("controller %s found but %s not yet available", ctrlName, devPath)
	}

	return "", fmt.Errorf("no nvme controller found for nqn %s", nqn)
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
