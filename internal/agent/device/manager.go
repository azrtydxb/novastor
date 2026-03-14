// Package device manages NVMe device unbinding/binding for SPDK and hugepage
// allocation. SPDK requires devices to be unbound from the kernel NVMe driver
// and bound to vfio-pci or uio_pci_generic before it can use them.
package device

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

const (
	// sysClassNVMe is the sysfs path for NVMe devices.
	sysClassNVMe = "/sys/class/nvme"

	// sysBusPCI is the sysfs path for PCI device management.
	sysBusPCI = "/sys/bus/pci"

	// vfioPCIDriver is the preferred user-space driver for SPDK.
	vfioPCIDriver = "vfio-pci"

	// uioPCIGenericDriver is the fallback user-space driver.
	uioPCIGenericDriver = "uio_pci_generic"
)

// NVMeDevice represents a discovered NVMe device.
type NVMeDevice struct {
	// PCIeAddr is the PCI BDF address (e.g., "0000:00:04.0").
	PCIeAddr string

	// Model is the NVMe model string.
	Model string

	// Serial is the NVMe serial number.
	Serial string

	// SizeBytes is the total size in bytes.
	SizeBytes uint64

	// Driver is the currently bound driver (e.g., "nvme", "vfio-pci").
	Driver string

	// InUse indicates if the device has mounted partitions.
	InUse bool
}

// Manager handles NVMe device discovery, driver unbinding/binding, and
// hugepage management for SPDK.
type Manager struct {
	logger *zap.Logger
}

// NewManager creates a new device manager.
func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		logger: logger.Named("device-manager"),
	}
}

// DiscoverNVMeDevices discovers all NVMe devices on the system.
func (m *Manager) DiscoverNVMeDevices() ([]NVMeDevice, error) {
	entries, err := os.ReadDir(sysClassNVMe)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No NVMe subsystem
		}
		return nil, fmt.Errorf("reading %s: %w", sysClassNVMe, err)
	}

	var devices []NVMeDevice
	for _, entry := range entries {
		dev, err := m.readNVMeDevice(entry.Name())
		if err != nil {
			m.logger.Warn("failed to read NVMe device",
				zap.String("device", entry.Name()),
				zap.Error(err),
			)
			continue
		}
		devices = append(devices, *dev)
	}
	return devices, nil
}

// UnbindFromKernel unbinds an NVMe device from the kernel driver and binds it
// to vfio-pci (or uio_pci_generic as fallback) for SPDK use.
func (m *Manager) UnbindFromKernel(pcieAddr string) error {
	m.logger.Info("unbinding NVMe device from kernel", zap.String("pcieAddr", pcieAddr))

	// Check current driver.
	currentDriver, err := m.currentDriver(pcieAddr)
	if err != nil {
		return fmt.Errorf("checking current driver: %w", err)
	}

	if currentDriver == vfioPCIDriver || currentDriver == uioPCIGenericDriver {
		m.logger.Info("device already bound to user-space driver",
			zap.String("pcieAddr", pcieAddr),
			zap.String("driver", currentDriver),
		)
		return nil
	}

	// Unbind from current driver.
	if currentDriver != "" {
		unbindPath := filepath.Join(sysBusPCI, "devices", pcieAddr, "driver", "unbind")
		if err := os.WriteFile(unbindPath, []byte(pcieAddr), 0o200); err != nil {
			return fmt.Errorf("unbinding %s from %s: %w", pcieAddr, currentDriver, err)
		}
		m.logger.Info("unbound from kernel driver",
			zap.String("pcieAddr", pcieAddr),
			zap.String("driver", currentDriver),
		)
	}

	// Write the device's vendor:device ID to new_id for the target driver.
	vendorDevice, err := m.vendorDeviceID(pcieAddr)
	if err != nil {
		return fmt.Errorf("reading vendor/device ID: %w", err)
	}

	// Try vfio-pci first, fall back to uio_pci_generic.
	targetDriver := vfioPCIDriver
	if err := m.bindToDriver(pcieAddr, vendorDevice, targetDriver); err != nil {
		m.logger.Warn("vfio-pci bind failed, trying uio_pci_generic",
			zap.Error(err),
		)
		targetDriver = uioPCIGenericDriver
		if err := m.bindToDriver(pcieAddr, vendorDevice, targetDriver); err != nil {
			return fmt.Errorf("binding to %s: %w", targetDriver, err)
		}
	}

	m.logger.Info("device bound to user-space driver",
		zap.String("pcieAddr", pcieAddr),
		zap.String("driver", targetDriver),
	)
	return nil
}

// BindToKernel rebinds an NVMe device back to the kernel nvme driver.
func (m *Manager) BindToKernel(pcieAddr string) error {
	m.logger.Info("rebinding NVMe device to kernel", zap.String("pcieAddr", pcieAddr))

	currentDriver, err := m.currentDriver(pcieAddr)
	if err != nil {
		return fmt.Errorf("checking current driver: %w", err)
	}

	if currentDriver == "nvme" {
		return nil // Already on kernel driver.
	}

	// Unbind from current driver.
	if currentDriver != "" {
		unbindPath := filepath.Join(sysBusPCI, "devices", pcieAddr, "driver", "unbind")
		if err := os.WriteFile(unbindPath, []byte(pcieAddr), 0o200); err != nil {
			return fmt.Errorf("unbinding %s: %w", pcieAddr, err)
		}
	}

	// Bind to nvme driver.
	bindPath := filepath.Join(sysBusPCI, "drivers", "nvme", "bind")
	if err := os.WriteFile(bindPath, []byte(pcieAddr), 0o200); err != nil {
		return fmt.Errorf("binding %s to nvme driver: %w", pcieAddr, err)
	}

	return nil
}

// readNVMeDevice reads device information from sysfs.
func (m *Manager) readNVMeDevice(name string) (*NVMeDevice, error) {
	devPath := filepath.Join(sysClassNVMe, name)

	// Read PCI address from the device symlink.
	realPath, err := filepath.EvalSymlinks(devPath)
	if err != nil {
		return nil, fmt.Errorf("resolving symlink: %w", err)
	}

	// Extract PCIe address from the path.
	pcieAddr := extractPCIeAddr(realPath)
	if pcieAddr == "" {
		return nil, fmt.Errorf("could not extract PCIe address from %s", realPath)
	}

	model := readSysfsString(filepath.Join(devPath, "model"))
	serial := readSysfsString(filepath.Join(devPath, "serial"))

	driver, _ := m.currentDriver(pcieAddr)

	return &NVMeDevice{
		PCIeAddr: pcieAddr,
		Model:    model,
		Serial:   serial,
		Driver:   driver,
	}, nil
}

// currentDriver returns the name of the driver currently bound to the PCI device.
func (m *Manager) currentDriver(pcieAddr string) (string, error) {
	driverLink := filepath.Join(sysBusPCI, "devices", pcieAddr, "driver")
	target, err := os.Readlink(driverLink)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil // No driver bound.
		}
		return "", err
	}
	return filepath.Base(target), nil
}

// vendorDeviceID reads the vendor and device ID from sysfs.
func (m *Manager) vendorDeviceID(pcieAddr string) (string, error) {
	vendorPath := filepath.Join(sysBusPCI, "devices", pcieAddr, "vendor")
	devicePath := filepath.Join(sysBusPCI, "devices", pcieAddr, "device")

	vendor := readSysfsString(vendorPath)
	device := readSysfsString(devicePath)
	if vendor == "" || device == "" {
		return "", fmt.Errorf("could not read vendor/device for %s", pcieAddr)
	}

	// Strip "0x" prefix.
	vendor = strings.TrimPrefix(vendor, "0x")
	device = strings.TrimPrefix(device, "0x")
	return vendor + " " + device, nil
}

// bindToDriver binds a PCI device to the specified driver.
func (m *Manager) bindToDriver(pcieAddr, vendorDevice, driver string) error {
	// Write vendor:device to new_id.
	newIDPath := filepath.Join(sysBusPCI, "drivers", driver, "new_id")
	if err := os.WriteFile(newIDPath, []byte(vendorDevice), 0o200); err != nil {
		return fmt.Errorf("writing new_id for %s: %w", driver, err)
	}

	// Bind the device.
	bindPath := filepath.Join(sysBusPCI, "drivers", driver, "bind")
	if err := os.WriteFile(bindPath, []byte(pcieAddr), 0o200); err != nil {
		// EBUSY is OK — means it was already auto-bound by new_id.
		if !os.IsPermission(err) {
			return fmt.Errorf("binding %s to %s: %w", pcieAddr, driver, err)
		}
	}
	return nil
}

// extractPCIeAddr extracts the PCI BDF address from a sysfs path.
func extractPCIeAddr(path string) string {
	// Return the last PCI BDF address in the path (closest to the leaf device).
	var last string
	for p := range strings.SplitSeq(path, "/") {
		// PCI BDF format: DDDD:BB:DD.F
		if len(p) >= 12 && p[4] == ':' && p[7] == ':' && p[10] == '.' {
			last = p
		}
	}
	return last
}

// readSysfsString reads a sysfs attribute as a trimmed string.
func readSysfsString(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
