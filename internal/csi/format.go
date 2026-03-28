package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/logging"
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
//
// On distributed storage, mkfs can take minutes (writes fan out to remote
// backends via NDP). To survive kubelet gRPC deadline expiry, mkfs runs in
// a background goroutine with a 10-minute timeout. If already in progress,
// returns a retriable error so kubelet retries and checks isFormatted.
func formatDevice(_ context.Context, devicePath, fsType string) error {
	formatMu.Lock()
	if _, running := formatInProgress[devicePath]; running {
		formatMu.Unlock()
		return fmt.Errorf("mkfs already in progress for %s (will complete in background)", devicePath)
	}
	formatInProgress[devicePath] = struct{}{}
	formatMu.Unlock()

	mkfsCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	var cmd *exec.Cmd
	switch fsType {
	case "ext4":
		// UNMAP/WRITE_ZEROES complete instantly (reactor no-op, thin provisioning).
		// Let mkfs use discard for fast initialization — the full-volume discard
		// completes in milliseconds since our UNMAP is a no-op.
		cmd = exec.CommandContext(mkfsCtx, "mkfs.ext4", "-E", "lazy_itable_init=1,lazy_journal_init=1", devicePath)
	default:
		cancel()
		formatMu.Lock()
		delete(formatInProgress, devicePath)
		formatMu.Unlock()
		return fmt.Errorf("unsupported filesystem type: %s", fsType)
	}

	// Run mkfs in the background — survives gRPC deadline expiry.
	go func() {
		defer cancel()
		defer func() {
			formatMu.Lock()
			delete(formatInProgress, devicePath)
			formatMu.Unlock()
		}()
		out, err := cmd.CombinedOutput()
		if err != nil {
			logging.L.Error("background mkfs failed",
				zap.String("device", devicePath),
				zap.Error(err),
				zap.String("output", string(out)))
		} else {
			logging.L.Info("background mkfs completed",
				zap.String("device", devicePath))
		}
	}()

	// Wait briefly — small volumes format quickly.
	time.Sleep(5 * time.Second)

	// Check if already done.
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer checkCancel()
	formatted, _ := isFormatted(checkCtx, devicePath)
	if formatted {
		return nil
	}

	// Not done yet — return error so kubelet retries. The background
	// goroutine will eventually complete mkfs.
	return fmt.Errorf("mkfs in progress for %s (background, will retry)", devicePath)
}

var (
	formatInProgress = make(map[string]struct{})
	formatMu         sync.Mutex
)

// mountDevice mounts devicePath to targetPath using the given filesystem type.
func mountDevice(ctx context.Context, devicePath, targetPath, fsType string) error {
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return fmt.Errorf("creating mount target %s: %w", targetPath, err)
	}
	// Mount with errors=continue so ext4 doesn't remount read-only on
	// transient I/O errors during NVMe multipath failover. The ~10s window
	// while the dead path is being removed can produce I/O errors that
	// would otherwise make the filesystem permanently read-only.
	cmd := exec.CommandContext(ctx, "mount", "-t", fsType, "-o", "errors=continue", devicePath, targetPath)
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
