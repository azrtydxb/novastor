package device

import (
	"testing"
)

func TestExtractPCIeAddr(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "standard sysfs path",
			path:     "/sys/devices/pci0000:00/0000:00:04.0/nvme/nvme0",
			expected: "0000:00:04.0",
		},
		{
			name:     "nested PCI bridge",
			path:     "/sys/devices/pci0000:00/0000:00:01.0/0000:01:00.0/nvme/nvme1",
			expected: "0000:01:00.0",
		},
		{
			name:     "no PCI address",
			path:     "/sys/devices/virtual/nvme/nvme0",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPCIeAddr(tt.path)
			if got != tt.expected {
				t.Errorf("extractPCIeAddr(%q) = %q, want %q", tt.path, got, tt.expected)
			}
		})
	}
}

func TestReadSysfsString(t *testing.T) {
	// Non-existent path should return empty string.
	got := readSysfsString("/nonexistent/path")
	if got != "" {
		t.Errorf("expected empty string for nonexistent path, got %q", got)
	}
}

func TestParsePageSizeBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected uint64
	}{
		{"2048kB", 2 * 1024 * 1024},
		{"1048576kB", 1024 * 1024 * 1024},
		{"invalid", 2 * 1024 * 1024}, // fallback
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parsePageSizeBytes(tt.input)
			if got != tt.expected {
				t.Errorf("parsePageSizeBytes(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}
