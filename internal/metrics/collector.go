package metrics

// AgentCollector collects metrics from a storage agent and updates the
// corresponding Prometheus gauges and counters. The function fields are
// wired to real data sources (chunk store, disk manager, etc.) by the
// caller.
type AgentCollector struct {
	// ChunkCountFn returns the current number of chunks stored locally.
	ChunkCountFn func() int64

	// DiskStatsFn returns per-device disk statistics: device name, total
	// bytes, used bytes, and free bytes.
	DiskStatsFn func() []DiskStats
}

// DiskStats holds capacity information for a single storage device.
type DiskStats struct {
	Device string
	Total  uint64
	Used   uint64
	Free   uint64
}

// NewAgentCollector creates an AgentCollector with no-op data sources.
// Callers should replace the function fields with real implementations
// before invoking Collect.
func NewAgentCollector() *AgentCollector {
	return &AgentCollector{
		ChunkCountFn: func() int64 { return 0 },
		DiskStatsFn:  func() []DiskStats { return nil },
	}
}

// Collect gathers current metrics from the agent and updates Prometheus
// gauges. This method is safe to call periodically from a background
// goroutine.
func (c *AgentCollector) Collect() {
	if c.ChunkCountFn != nil {
		ChunkCount.Set(float64(c.ChunkCountFn()))
	}

	if c.DiskStatsFn != nil {
		for _, ds := range c.DiskStatsFn() {
			DiskBytesTotal.WithLabelValues(ds.Device).Set(float64(ds.Total))
			DiskBytesUsed.WithLabelValues(ds.Device).Set(float64(ds.Used))
			DiskBytesFree.WithLabelValues(ds.Device).Set(float64(ds.Free))
		}
	}
}
