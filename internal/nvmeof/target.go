// Package nvmeof provides NVMe-oF target management for NovaStor.
// This package handles the creation and management of NVMe-over-Fabrics
// targets for exporting block volumes.
package nvmeof

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// DefaultBasePath is the default nvmet configfs mount point.
	DefaultBasePath = "/sys/kernel/config/nvmet"

	// subsystemPrefix is prepended to volume IDs for subsystem naming.
	subsystemPrefix = "novastor-"
)

// ConfigFS abstracts filesystem operations so the nvmet configfs interface
// can be tested without root privileges or kernel support.
type ConfigFS interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(path string, data []byte, perm os.FileMode) error
	Remove(path string) error
	Symlink(oldname, newname string) error
	ReadDir(path string) ([]fs.DirEntry, error)
}

// RealConfigFS implements ConfigFS using the real operating system calls.
type RealConfigFS struct{}

// MkdirAll creates a directory including all necessary parents.
func (RealConfigFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// WriteFile writes data to a file with the given permissions.
func (RealConfigFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// Remove removes the named file or directory.
func (RealConfigFS) Remove(path string) error {
	return os.Remove(path)
}

// Symlink creates a symbolic link from oldname to newname.
func (RealConfigFS) Symlink(oldname, newname string) error {
	return os.Symlink(oldname, newname)
}

// ReadDir reads the directory and returns a list of directory entries.
func (RealConfigFS) ReadDir(path string) ([]fs.DirEntry, error) {
	return os.ReadDir(path)
}

// TargetManager manages NVMe-oF/TCP targets using the Linux nvmet configfs
// interface. It creates and tears down subsystems, namespaces, ports, and the
// symlinks that bind them together.
type TargetManager struct {
	configFS ConfigFS
	basePath string
}

// NewTargetManager creates a TargetManager with the real OS configfs and the
// default nvmet base path.
func NewTargetManager() *TargetManager {
	return &TargetManager{
		configFS: RealConfigFS{},
		basePath: DefaultBasePath,
	}
}

// NewTargetManagerWithFS creates a TargetManager with a custom ConfigFS
// implementation and base path, primarily for testing.
func NewTargetManagerWithFS(cfs ConfigFS, basePath string) *TargetManager {
	return &TargetManager{
		configFS: cfs,
		basePath: basePath,
	}
}

// subsystemPath returns the configfs path for a subsystem.
func (tm *TargetManager) subsystemPath(volumeID string) string {
	return filepath.Join(tm.basePath, "subsystems", subsystemPrefix+volumeID)
}

// namespacePath returns the configfs path for namespace 1 within a subsystem.
func (tm *TargetManager) namespacePath(volumeID string) string {
	return filepath.Join(tm.subsystemPath(volumeID), "namespaces", "1")
}

// portPath returns the configfs path for a port.
func (tm *TargetManager) portPath(port int) string {
	return filepath.Join(tm.basePath, "ports", strconv.Itoa(port))
}

// CreateTarget sets up an NVMe-oF/TCP target for the given volume. It creates
// the nvmet subsystem, namespace, port directory, and a symlink connecting the
// port to the subsystem.
func (tm *TargetManager) CreateTarget(volumeID string, port int, size int64) error {
	subsysDir := tm.subsystemPath(volumeID)
	nsDir := tm.namespacePath(volumeID)
	portDir := tm.portPath(port)

	// Create subsystem directory.
	if err := tm.configFS.MkdirAll(subsysDir, 0o755); err != nil {
		return fmt.Errorf("creating subsystem directory: %w", err)
	}

	// Allow any host to connect.
	attrPath := filepath.Join(subsysDir, "attr_allow_any_host")
	if err := tm.configFS.WriteFile(attrPath, []byte("1"), 0o644); err != nil {
		return fmt.Errorf("setting attr_allow_any_host: %w", err)
	}

	// Create namespace directory.
	if err := tm.configFS.MkdirAll(nsDir, 0o755); err != nil {
		return fmt.Errorf("creating namespace directory: %w", err)
	}

	// Write namespace size.
	sizePath := filepath.Join(nsDir, "device_size")
	sizeStr := strconv.FormatInt(size, 10)
	if err := tm.configFS.WriteFile(sizePath, []byte(sizeStr), 0o644); err != nil {
		return fmt.Errorf("writing namespace device_size: %w", err)
	}

	// Enable the namespace.
	enablePath := filepath.Join(nsDir, "enable")
	if err := tm.configFS.WriteFile(enablePath, []byte("1"), 0o644); err != nil {
		return fmt.Errorf("enabling namespace: %w", err)
	}

	// Create port directory.
	if err := tm.configFS.MkdirAll(portDir, 0o755); err != nil {
		return fmt.Errorf("creating port directory: %w", err)
	}

	// Configure port transport type.
	trTypePath := filepath.Join(portDir, "addr_trtype")
	if err := tm.configFS.WriteFile(trTypePath, []byte("tcp"), 0o644); err != nil {
		return fmt.Errorf("setting port transport type: %w", err)
	}

	// Configure port address family.
	adrFamPath := filepath.Join(portDir, "addr_adrfam")
	if err := tm.configFS.WriteFile(adrFamPath, []byte("ipv4"), 0o644); err != nil {
		return fmt.Errorf("setting port address family: %w", err)
	}

	// Configure port service ID (port number as string).
	trSvcIDPath := filepath.Join(portDir, "addr_trsvcid")
	if err := tm.configFS.WriteFile(trSvcIDPath, []byte(strconv.Itoa(port)), 0o644); err != nil {
		return fmt.Errorf("setting port service id: %w", err)
	}

	// Create the subsystems directory under the port for the symlink.
	portSubsysDir := filepath.Join(portDir, "subsystems")
	if err := tm.configFS.MkdirAll(portSubsysDir, 0o755); err != nil {
		return fmt.Errorf("creating port subsystems directory: %w", err)
	}

	// Symlink from port/subsystems/<nqn> -> subsystem directory.
	linkPath := filepath.Join(portSubsysDir, subsystemPrefix+volumeID)
	if err := tm.configFS.Symlink(subsysDir, linkPath); err != nil {
		return fmt.Errorf("creating subsystem symlink: %w", err)
	}

	return nil
}

// CreateTargetWithDevice sets up an NVMe-oF/TCP target backed by a real block
// device (or loop device). Unlike CreateTarget (which writes a size hint),
// this function writes the device_path attribute that nvmet actually requires
// to expose a namespace. listenAddr is written to addr_traddr so the kernel
// binds the port to a specific IP (required when the node has multiple interfaces).
func (tm *TargetManager) CreateTargetWithDevice(volumeID string, port int, devicePath, listenAddr string) error {
	subsysDir := tm.subsystemPath(volumeID)
	nsDir := tm.namespacePath(volumeID)
	portDir := tm.portPath(port)

	// Create subsystem directory.
	if err := tm.configFS.MkdirAll(subsysDir, 0o755); err != nil {
		return fmt.Errorf("creating subsystem directory: %w", err)
	}

	// Allow any host to connect.
	attrPath := filepath.Join(subsysDir, "attr_allow_any_host")
	if err := tm.configFS.WriteFile(attrPath, []byte("1"), 0o644); err != nil {
		return fmt.Errorf("setting attr_allow_any_host: %w", err)
	}

	// Create namespace directory.
	if err := tm.configFS.MkdirAll(nsDir, 0o755); err != nil {
		return fmt.Errorf("creating namespace directory: %w", err)
	}

	// Write namespace device_path — the correct nvmet attribute for a real device.
	devPathAttr := filepath.Join(nsDir, "device_path")
	if err := tm.configFS.WriteFile(devPathAttr, []byte(devicePath), 0o644); err != nil {
		return fmt.Errorf("writing namespace device_path: %w", err)
	}

	// Enable the namespace.
	enablePath := filepath.Join(nsDir, "enable")
	if err := tm.configFS.WriteFile(enablePath, []byte("1"), 0o644); err != nil {
		return fmt.Errorf("enabling namespace: %w", err)
	}

	// Create or reuse the shared port. The port directory persists across agent
	// pod restarts (it lives in the host kernel's configfs). If it exists but
	// was configured with a specific addr_traddr (e.g. an old pod IP) and has
	// no active subsystem symlinks, the kernel rejects new symlinks with
	// EADDRNOTAVAIL. In that case we delete and recreate the port directory.
	portSubsysDir := filepath.Join(portDir, "subsystems")
	needPortCreate := false
	if _, statErr := os.Stat(filepath.Join(portDir, "addr_trtype")); os.IsNotExist(statErr) {
		needPortCreate = true
	} else {
		// Port exists — check if addr_traddr matches what we want.
		addrData, readErr := os.ReadFile(filepath.Join(portDir, "addr_traddr"))
		curAddr := strings.TrimSpace(string(addrData))
		if readErr == nil && curAddr != listenAddr {
			// Wrong listen address. Reset the port if it has no active symlinks.
			existingLinks, _ := os.ReadDir(portSubsysDir)
			if len(existingLinks) == 0 {
				if err := tm.configFS.Remove(portDir); err == nil {
					needPortCreate = true
				}
			}
		}
	}

	if needPortCreate {
		if err := tm.configFS.MkdirAll(portDir, 0o755); err != nil {
			return fmt.Errorf("creating port directory: %w", err)
		}

		trTypePath := filepath.Join(portDir, "addr_trtype")
		if err := tm.configFS.WriteFile(trTypePath, []byte("tcp"), 0o644); err != nil {
			return fmt.Errorf("setting port transport type: %w", err)
		}

		adrFamPath := filepath.Join(portDir, "addr_adrfam")
		if err := tm.configFS.WriteFile(adrFamPath, []byte("ipv4"), 0o644); err != nil {
			return fmt.Errorf("setting port address family: %w", err)
		}

		trAddrPath := filepath.Join(portDir, "addr_traddr")
		if err := tm.configFS.WriteFile(trAddrPath, []byte(listenAddr), 0o644); err != nil {
			return fmt.Errorf("setting port listen address: %w", err)
		}

		trSvcIDPath := filepath.Join(portDir, "addr_trsvcid")
		if err := tm.configFS.WriteFile(trSvcIDPath, []byte(strconv.Itoa(port)), 0o644); err != nil {
			return fmt.Errorf("setting port service id: %w", err)
		}
	}

	// Ensure the port's subsystems virtual directory is accessible.
	if err := tm.configFS.MkdirAll(portSubsysDir, 0o755); err != nil {
		return fmt.Errorf("creating port subsystems directory: %w", err)
	}

	// Symlink from port/subsystems/<nqn> -> subsystem directory.
	linkPath := filepath.Join(portSubsysDir, subsystemPrefix+volumeID)
	if err := tm.configFS.Symlink(subsysDir, linkPath); err != nil {
		return fmt.Errorf("creating subsystem symlink: %w", err)
	}

	return nil
}

// DeleteTarget tears down the NVMe-oF/TCP target for the given volume. It
// removes the symlink, port, namespace, and subsystem in reverse order.
func (tm *TargetManager) DeleteTarget(volumeID string) error {
	subsysDir := tm.subsystemPath(volumeID)
	nsDir := tm.namespacePath(volumeID)

	// Find and remove symlinks in any ports that reference this subsystem.
	portsDir := filepath.Join(tm.basePath, "ports")
	portEntries, err := tm.configFS.ReadDir(portsDir)
	if err != nil {
		return fmt.Errorf("reading ports directory: %w", err)
	}
	for _, pe := range portEntries {
		linkPath := filepath.Join(portsDir, pe.Name(), "subsystems", subsystemPrefix+volumeID)
		// Best-effort removal; the symlink may not exist for every port.
		_ = tm.configFS.Remove(linkPath)
	}

	// Remove namespace directory.
	if err := tm.configFS.Remove(nsDir); err != nil {
		return fmt.Errorf("removing namespace directory: %w", err)
	}

	// Remove namespaces parent directory.
	nsParent := filepath.Join(tm.subsystemPath(volumeID), "namespaces")
	if err := tm.configFS.Remove(nsParent); err != nil {
		return fmt.Errorf("removing namespaces directory: %w", err)
	}

	// Remove subsystem directory.
	if err := tm.configFS.Remove(subsysDir); err != nil {
		return fmt.Errorf("removing subsystem directory: %w", err)
	}

	return nil
}

// ListTargets returns the volume IDs of all active NVMe-oF targets by
// scanning the subsystems directory for entries with the novastor- prefix.
func (tm *TargetManager) ListTargets() ([]string, error) {
	subsysDir := filepath.Join(tm.basePath, "subsystems")
	entries, err := tm.configFS.ReadDir(subsysDir)
	if err != nil {
		return nil, fmt.Errorf("reading subsystems directory: %w", err)
	}

	var targets []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, subsystemPrefix) {
			volumeID := strings.TrimPrefix(name, subsystemPrefix)
			targets = append(targets, volumeID)
		}
	}
	return targets, nil
}
