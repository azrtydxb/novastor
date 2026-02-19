// Package disk provides disk discovery and filtering for NovaStor.
// This package handles detection and classification of storage devices
// available on each node.
package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DeviceType represents the type of storage device.
type DeviceType int

const (
	// TypeUnknown represents an unknown device type.
	TypeUnknown DeviceType = iota
	// TypeNVMe represents an NVMe device.
	TypeNVMe
	// TypeSSD represents an SSD device.
	TypeSSD
	// TypeHDD represents an HDD device.
	TypeHDD
)

func (dt DeviceType) String() string {
	switch dt {
	case TypeNVMe:
		return "nvme"
	case TypeSSD:
		return "ssd"
	case TypeHDD:
		return "hdd"
	default:
		return "unknown"
	}
}

// DeviceInfo contains information about a storage device.
type DeviceInfo struct {
	Path       string
	SizeBytes  uint64
	DeviceType DeviceType
	Model      string
	Serial     string
	Rotational bool
}

func (d DeviceInfo) String() string {
	return fmt.Sprintf("%s (%s, %.1f GB, %s)", d.Path, d.DeviceType, float64(d.SizeBytes)/1e9, d.Model)
}

// FilterOptions contains criteria for filtering devices.
type FilterOptions struct {
	DeviceType   DeviceType
	MinSizeBytes uint64
}

// FilterDevices filters a list of devices based on the given options.
func FilterDevices(devices []DeviceInfo, opts FilterOptions) []DeviceInfo {
	var result []DeviceInfo
	for _, d := range devices {
		if opts.DeviceType != TypeUnknown && d.DeviceType != opts.DeviceType {
			continue
		}
		if opts.MinSizeBytes > 0 && d.SizeBytes < opts.MinSizeBytes {
			continue
		}
		result = append(result, d)
	}
	return result
}

// DiscoverDevices scans the system for available block devices and returns
// their information. It excludes loop, ram, and device-mapper devices.
func DiscoverDevices() ([]DeviceInfo, error) {
	sysBlock := "/sys/block"
	entries, err := os.ReadDir(sysBlock)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", sysBlock, err)
	}
	var devices []DeviceInfo
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") || strings.HasPrefix(name, "dm-") {
			continue
		}
		dev := DeviceInfo{Path: filepath.Join("/dev", name)}
		sizeData, err := os.ReadFile(filepath.Join(sysBlock, name, "size"))
		if err == nil {
			var sectors uint64
			fmt.Sscanf(strings.TrimSpace(string(sizeData)), "%d", &sectors)
			dev.SizeBytes = sectors * 512
		}
		if strings.HasPrefix(name, "nvme") {
			dev.DeviceType = TypeNVMe
		} else {
			rotData, err := os.ReadFile(filepath.Join(sysBlock, name, "queue", "rotational"))
			if err == nil {
				if strings.TrimSpace(string(rotData)) == "1" {
					dev.DeviceType = TypeHDD
					dev.Rotational = true
				} else {
					dev.DeviceType = TypeSSD
				}
			}
		}
		modelData, err := os.ReadFile(filepath.Join(sysBlock, name, "device", "model"))
		if err == nil {
			dev.Model = strings.TrimSpace(string(modelData))
		}
		devices = append(devices, dev)
	}
	return devices, nil
}
