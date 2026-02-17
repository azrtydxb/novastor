package disk

import "testing"

func TestDeviceInfo_String(t *testing.T) {
	d := DeviceInfo{
		Path:       "/dev/sda",
		SizeBytes:  1024 * 1024 * 1024 * 100,
		DeviceType: TypeSSD,
		Model:      "Samsung 980 Pro",
	}
	s := d.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestDeviceType_String(t *testing.T) {
	tests := []struct {
		dt   DeviceType
		want string
	}{
		{TypeNVMe, "nvme"},
		{TypeSSD, "ssd"},
		{TypeHDD, "hdd"},
		{TypeUnknown, "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.dt.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFilterDevices(t *testing.T) {
	devices := []DeviceInfo{
		{Path: "/dev/nvme0n1", SizeBytes: 500e9, DeviceType: TypeNVMe},
		{Path: "/dev/sda", SizeBytes: 100e9, DeviceType: TypeSSD},
		{Path: "/dev/sdb", SizeBytes: 1e12, DeviceType: TypeHDD},
		{Path: "/dev/nvme1n1", SizeBytes: 250e9, DeviceType: TypeNVMe},
	}
	t.Run("filter by type", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{DeviceType: TypeNVMe})
		if len(filtered) != 2 {
			t.Errorf("expected 2 NVMe devices, got %d", len(filtered))
		}
	})
	t.Run("filter by min size", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{MinSizeBytes: 200e9})
		if len(filtered) != 3 {
			t.Errorf("expected 3 devices >= 200GB, got %d", len(filtered))
		}
	})
	t.Run("filter by type and size", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{DeviceType: TypeNVMe, MinSizeBytes: 400e9})
		if len(filtered) != 1 {
			t.Errorf("expected 1 NVMe >= 400GB, got %d", len(filtered))
		}
	})
	t.Run("no filter", func(t *testing.T) {
		filtered := FilterDevices(devices, FilterOptions{})
		if len(filtered) != 4 {
			t.Errorf("expected all 4 devices, got %d", len(filtered))
		}
	})
}
