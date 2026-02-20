package spdk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ProcessManager starts, monitors, and stops the novastor-dataplane binary.
type ProcessManager struct {
	binaryPath string
	socketPath string
	configPath string
	logger     *zap.Logger

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{}
}

// NewProcessManager creates a manager for the SPDK data-plane process.
func NewProcessManager(binaryPath, socketPath, configPath string, logger *zap.Logger) *ProcessManager {
	return &ProcessManager{
		binaryPath: binaryPath,
		socketPath: socketPath,
		configPath: configPath,
		logger:     logger,
	}
}

// Start launches the data-plane binary and waits for the socket to appear.
func (pm *ProcessManager) Start(ctx context.Context) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.cmd != nil {
		return fmt.Errorf("data-plane process already running")
	}

	args := []string{
		"--socket-path", pm.socketPath,
	}
	if pm.configPath != "" {
		args = append(args, "--config", pm.configPath)
	}

	cmd := exec.CommandContext(ctx, pm.binaryPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting data-plane binary %s: %w", pm.binaryPath, err)
	}

	pm.cmd = cmd
	pm.done = make(chan struct{})

	// Monitor the process in a goroutine.
	go func() {
		defer close(pm.done)
		if err := cmd.Wait(); err != nil {
			pm.logger.Error("data-plane process exited", zap.Error(err))
		} else {
			pm.logger.Info("data-plane process exited cleanly")
		}
		pm.mu.Lock()
		pm.cmd = nil
		pm.mu.Unlock()
	}()

	// Wait for the socket to appear.
	if err := pm.waitForSocket(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("waiting for data-plane socket: %w", err)
	}

	pm.logger.Info("data-plane process started",
		zap.String("binary", pm.binaryPath),
		zap.String("socket", pm.socketPath),
		zap.Int("pid", cmd.Process.Pid),
	)
	return nil
}

// Stop gracefully terminates the data-plane process.
func (pm *ProcessManager) Stop() error {
	pm.mu.Lock()
	cmd := pm.cmd
	done := pm.done
	pm.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	pm.logger.Info("stopping data-plane process", zap.Int("pid", cmd.Process.Pid))

	// Send SIGTERM first.
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		pm.logger.Warn("failed to send interrupt to data-plane", zap.Error(err))
		_ = cmd.Process.Kill()
	}

	// Wait with a timeout.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		pm.logger.Warn("data-plane did not exit in time, killing")
		_ = cmd.Process.Kill()
		<-done
	}

	// Clean up socket.
	_ = os.Remove(pm.socketPath)
	return nil
}

// IsRunning reports whether the data-plane process is still alive.
func (pm *ProcessManager) IsRunning() bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.cmd != nil
}

// waitForSocket polls until the Unix socket file appears.
func (pm *ProcessManager) waitForSocket(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, err := os.Stat(pm.socketPath); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("socket %s did not appear within %s", pm.socketPath, timeout)
}
