package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// isFormatted returns true if the block device at devicePath already contains
// a filesystem, as determined by blkid.
func isFormatted(ctx context.Context, devicePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "blkid", "-p", "-o", "value", "-s", "TYPE", devicePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// blkid exits non-zero when no filesystem is found.
		exitErr, ok := err.(*exec.ExitError)
		if ok && exitErr.ExitCode() == 2 {
			return false, nil
		}
		return false, fmt.Errorf("blkid %s: %w: %s", devicePath, err, string(out))
	}
	fsType := strings.TrimSpace(string(out))
	return fsType != "", nil
}

// formatDevice formats devicePath with the given filesystem type.
// This is a destructive operation and should only be called on unformatted devices.
func formatDevice(ctx context.Context, devicePath, fsType string) error {
	var cmd *exec.Cmd
	switch fsType {
	case "ext4":
		// -E nodiscard: skip full-volume discard during format.
		// Our NVMe-oF target advertises WRITE_ZEROES/TRIM support,
		// which causes mkfs to issue a full-volume discard that is
		// extremely slow over NVMe-oF (writes real zeros).
		// Unwritten blocks already return zeros, so discard is unnecessary.
		cmd = exec.CommandContext(ctx, "mkfs.ext4", "-E", "nodiscard", devicePath)
	default:
		return fmt.Errorf("unsupported filesystem type: %s", fsType)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs.%s %s: %w: %s", fsType, devicePath, err, string(out))
	}
	return nil
}

// mountDevice mounts devicePath to targetPath using the given filesystem type.
func mountDevice(ctx context.Context, devicePath, targetPath, fsType string) error {
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return fmt.Errorf("creating mount target %s: %w", targetPath, err)
	}
	cmd := exec.CommandContext(ctx, "mount", "-t", fsType, devicePath, targetPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount -t %s %s %s: %w: %s", fsType, devicePath, targetPath, err, string(out))
	}
	return nil
}

// unmountPath unmounts the filesystem at targetPath.
func unmountPath(ctx context.Context, targetPath string) error {
	cmd := exec.CommandContext(ctx, "umount", targetPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount %s: %w: %s", targetPath, err, string(out))
	}
	return nil
}
