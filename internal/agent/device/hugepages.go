package device

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

const (
	// Default hugepage size for SPDK (2MB hugepages).
	defaultHugepageSize = "2048kB"

	// sysHugepages is the sysfs path for hugepage management.
	sysHugepages = "/sys/kernel/mm/hugepages"

	// hugepageMountPoint is where hugetlbfs should be mounted for SPDK.
	hugepageMountPoint = "/dev/hugepages"
)

// HugepageInfo contains information about hugepage allocation.
type HugepageInfo struct {
	// PageSize is the hugepage size string (e.g., "2048kB", "1048576kB").
	PageSize string

	// TotalPages is the total number of hugepages allocated.
	TotalPages int

	// FreePages is the number of free hugepages.
	FreePages int

	// TotalBytes is the total hugepage memory in bytes.
	TotalBytes uint64
}

// EnsureHugepages ensures that at least `minPages` hugepages of the given size
// are allocated. Returns the current hugepage state after any adjustments.
func (m *Manager) EnsureHugepages(minPages int, pageSize string) (*HugepageInfo, error) {
	if pageSize == "" {
		pageSize = defaultHugepageSize
	}

	m.logger.Info("ensuring hugepages",
		zap.Int("minPages", minPages),
		zap.String("pageSize", pageSize),
	)

	hpDir := filepath.Join(sysHugepages, "hugepages-"+pageSize)
	if _, err := os.Stat(hpDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("hugepage size %s not supported (missing %s)", pageSize, hpDir)
	}

	// Read current allocation.
	current, err := readHugepageInt(filepath.Join(hpDir, "nr_hugepages"))
	if err != nil {
		return nil, fmt.Errorf("reading current hugepage count: %w", err)
	}

	free, err := readHugepageInt(filepath.Join(hpDir, "free_hugepages"))
	if err != nil {
		return nil, fmt.Errorf("reading free hugepage count: %w", err)
	}

	m.logger.Info("current hugepage state",
		zap.Int("total", current),
		zap.Int("free", free),
		zap.String("pageSize", pageSize),
	)

	// Allocate more if needed.
	if current < minPages {
		m.logger.Info("allocating additional hugepages",
			zap.Int("current", current),
			zap.Int("requested", minPages),
		)

		nrPath := filepath.Join(hpDir, "nr_hugepages")
		if err := os.WriteFile(nrPath, []byte(strconv.Itoa(minPages)), 0o644); err != nil {
			return nil, fmt.Errorf("allocating %d hugepages: %w", minPages, err)
		}

		// Re-read to verify.
		current, err = readHugepageInt(filepath.Join(hpDir, "nr_hugepages"))
		if err != nil {
			return nil, fmt.Errorf("re-reading hugepage count: %w", err)
		}
		free, _ = readHugepageInt(filepath.Join(hpDir, "free_hugepages"))

		if current < minPages {
			m.logger.Warn("could not allocate all requested hugepages",
				zap.Int("requested", minPages),
				zap.Int("allocated", current),
			)
		}
	}

	pageSizeBytes := parsePageSizeBytes(pageSize)
	return &HugepageInfo{
		PageSize:   pageSize,
		TotalPages: current,
		FreePages:  free,
		TotalBytes: uint64(current) * pageSizeBytes,
	}, nil
}

// GetHugepageInfo returns the current hugepage allocation state.
func (m *Manager) GetHugepageInfo(pageSize string) (*HugepageInfo, error) {
	if pageSize == "" {
		pageSize = defaultHugepageSize
	}

	hpDir := filepath.Join(sysHugepages, "hugepages-"+pageSize)
	if _, err := os.Stat(hpDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("hugepage size %s not supported", pageSize)
	}

	total, err := readHugepageInt(filepath.Join(hpDir, "nr_hugepages"))
	if err != nil {
		return nil, err
	}
	free, err := readHugepageInt(filepath.Join(hpDir, "free_hugepages"))
	if err != nil {
		return nil, err
	}

	pageSizeBytes := parsePageSizeBytes(pageSize)
	return &HugepageInfo{
		PageSize:   pageSize,
		TotalPages: total,
		FreePages:  free,
		TotalBytes: uint64(total) * pageSizeBytes,
	}, nil
}

// EnsureHugetlbfsMount checks that hugetlbfs is mounted at the expected path.
func (m *Manager) EnsureHugetlbfsMount() error {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return fmt.Errorf("reading /proc/mounts: %w", err)
	}

	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[1] == hugepageMountPoint && fields[2] == "hugetlbfs" {
			m.logger.Debug("hugetlbfs already mounted", zap.String("path", hugepageMountPoint))
			return nil
		}
	}

	return fmt.Errorf("hugetlbfs not mounted at %s — mount with: mount -t hugetlbfs nodev %s",
		hugepageMountPoint, hugepageMountPoint)
}

// readHugepageInt reads an integer from a sysfs file.
func readHugepageInt(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

// parsePageSizeBytes converts a page size string like "2048kB" to bytes.
func parsePageSizeBytes(pageSize string) uint64 {
	s := strings.TrimSuffix(pageSize, "kB")
	kb, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 2 * 1024 * 1024 // Default 2MB
	}
	return kb * 1024
}
