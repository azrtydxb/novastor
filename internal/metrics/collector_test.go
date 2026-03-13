package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAgentCollector(t *testing.T) {
	collector := NewAgentCollector()
	require.NotNil(t, collector)
	assert.NotNil(t, collector.ChunkCountFn)
	assert.NotNil(t, collector.DiskStatsFn)
}

func TestAgentCollector_Collect_ChunkCount(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock chunk count function
	collector.ChunkCountFn = func() int64 {
		return 12345
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_DiskStats(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock disk stats function
	collector.DiskStatsFn = func() []DiskStats {
		return []DiskStats{
			{Device: "sda1", Total: 1000000000, Used: 500000000, Free: 500000000},
			{Device: "sdb1", Total: 2000000000, Used: 1000000000, Free: 1000000000},
		}
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_NilFunctions(t *testing.T) {
	collector := &AgentCollector{
		ChunkCountFn: nil,
		DiskStatsFn:  nil,
	}

	// Collect should not panic with nil functions
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_EmptyDiskStats(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock disk stats function returning empty slice
	collector.DiskStatsFn = func() []DiskStats {
		return []DiskStats{}
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_NilDiskStats(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock disk stats function returning nil
	collector.DiskStatsFn = func() []DiskStats {
		return nil
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_MultipleDevices(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock disk stats function with many devices
	collector.DiskStatsFn = func() []DiskStats {
		stats := make([]DiskStats, 10)
		for i := 0; i < 10; i++ {
			stats[i] = DiskStats{
				Device: "sd" + string(rune('a'+i)) + "1",
				Total:  uint64((i + 1) * 1000000000),
				Used:   uint64(i * 500000000),
				Free:   uint64(500000000),
			}
		}
		return stats
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_ZeroValues(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock functions returning zero values
	collector.ChunkCountFn = func() int64 {
		return 0
	}
	collector.DiskStatsFn = func() []DiskStats {
		return []DiskStats{
			{Device: "sda1", Total: 0, Used: 0, Free: 0},
		}
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestAgentCollector_Collect_LargeValues(t *testing.T) {
	collector := NewAgentCollector()

	// Set up mock functions with large values
	collector.ChunkCountFn = func() int64 {
		return int64(1 << 50) // Very large chunk count
	}
	collector.DiskStatsFn = func() []DiskStats {
		return []DiskStats{
			{Device: "sda1", Total: 1 << 60, Used: 1 << 59, Free: 1 << 59}, // Very large disk values
		}
	}

	// Collect should not panic
	assert.NotPanics(t, func() {
		collector.Collect()
	})
}

func TestDiskStats(t *testing.T) {
	stats := DiskStats{
		Device: "sda1",
		Total:  1000000000,
		Used:   500000000,
		Free:   500000000,
	}

	assert.Equal(t, "sda1", stats.Device)
	assert.Equal(t, uint64(1000000000), stats.Total)
	assert.Equal(t, uint64(500000000), stats.Used)
	assert.Equal(t, uint64(500000000), stats.Free)
}

func TestDiskStats_Empty(t *testing.T) {
	stats := DiskStats{}

	assert.Equal(t, "", stats.Device)
	assert.Equal(t, uint64(0), stats.Total)
	assert.Equal(t, uint64(0), stats.Used)
	assert.Equal(t, uint64(0), stats.Free)
}

func TestAgentCollector_DefaultFunctions(t *testing.T) {
	collector := NewAgentCollector()

	// Default chunk count function should return 0
	count := collector.ChunkCountFn()
	assert.Equal(t, int64(0), count)

	// Default disk stats function should return nil
	stats := collector.DiskStatsFn()
	assert.Nil(t, stats)
}

func TestAgentCollector_CustomChunkCountFn(t *testing.T) {
	collector := NewAgentCollector()

	expectedCount := int64(999)
	collector.ChunkCountFn = func() int64 {
		return expectedCount
	}

	count := collector.ChunkCountFn()
	assert.Equal(t, expectedCount, count)
}

func TestAgentCollector_CustomDiskStatsFn(t *testing.T) {
	collector := NewAgentCollector()

	expectedStats := []DiskStats{
		{Device: "nvme0n1", Total: 500000000000, Used: 250000000000, Free: 250000000000},
	}
	collector.DiskStatsFn = func() []DiskStats {
		return expectedStats
	}

	stats := collector.DiskStatsFn()
	assert.Equal(t, expectedStats, stats)
}

// Benchmark tests
func BenchmarkAgentCollector_Collect(b *testing.B) {
	collector := NewAgentCollector()
	collector.ChunkCountFn = func() int64 { return 12345 }
	collector.DiskStatsFn = func() []DiskStats {
		return []DiskStats{
			{Device: "sda1", Total: 1000000000, Used: 500000000, Free: 500000000},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.Collect()
	}
}

func BenchmarkAgentCollector_Collect_ManyDevices(b *testing.B) {
	collector := NewAgentCollector()
	collector.ChunkCountFn = func() int64 { return 12345 }
	collector.DiskStatsFn = func() []DiskStats {
		stats := make([]DiskStats, 100)
		for i := 0; i < 100; i++ {
			stats[i] = DiskStats{
				Device: "sd" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
				Total:  uint64((i + 1) * 1000000000),
				Used:   uint64(i * 500000000),
				Free:   uint64(500000000),
			}
		}
		return stats
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.Collect()
	}
}

func BenchmarkNewAgentCollector(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewAgentCollector()
	}
}
