package chunk

import (
	"context"
	"testing"

	"github.com/piwi3910/novastor/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// TestChunkStoreMetrics verifies that chunk store operations
// properly increment their corresponding Prometheus metrics.
//
// Note: This test creates its own registry to avoid interference with
// the global registry. The metrics are global variables, so the test
// verifies they are being incremented rather than checking exact values.
func TestChunkStoreMetrics(t *testing.T) {
	// Create a custom registry for this test to isolate from other tests.
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.ChunkOpsTotal)
	reg.MustRegister(metrics.ChunkBytesTotal)

	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	ctx := context.Background()

	// Write a chunk with computed checksum.
	data := []byte("test data for metrics")
	c := &Chunk{
		ID:   ChunkID("test-chunk-metrics"),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	// Get baseline write count from our custom registry.
	writeBefore := getMetricValue(reg, "novastor_agent_chunk_ops_total", "operation", "write")

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Check that write operation incremented the metric.
	writeAfter := getMetricValue(reg, "novastor_agent_chunk_ops_total", "operation", "write")
	if writeAfter != writeBefore+1 {
		t.Errorf("expected write count to increment by 1, got %f -> %f", writeBefore, writeAfter)
	}

	// Get baseline read count.
	readBefore := getMetricValue(reg, "novastor_agent_chunk_ops_total", "operation", "read")

	// Read the chunk and check metrics.
	_, err = store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Check that read operation incremented the metric.
	readAfter := getMetricValue(reg, "novastor_agent_chunk_ops_total", "operation", "read")
	if readAfter != readBefore+1 {
		t.Errorf("expected read count to increment by 1, got %f -> %f", readBefore, readAfter)
	}

	// Get baseline delete count.
	deleteBefore := getMetricValue(reg, "novastor_agent_chunk_ops_total", "operation", "delete")

	// Delete the chunk and check metrics.
	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Check that delete operation incremented the metric.
	deleteAfter := getMetricValue(reg, "novastor_agent_chunk_ops_total", "operation", "delete")
	if deleteAfter != deleteBefore+1 {
		t.Errorf("expected delete count to increment by 1, got %f -> %f", deleteBefore, deleteAfter)
	}
}

// getMetricValue retrieves the current value of a metric with the given name and label.
func getMetricValue(gatherer prometheus.Gatherer, metricName, labelName, labelValue string) float64 {
	metricFamilies, err := gatherer.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range metricFamilies {
		if mf.GetName() == metricName {
			for _, m := range mf.GetMetric() {
				for _, label := range m.Label {
					if label.GetName() == labelName && label.GetValue() == labelValue {
						return m.Counter.GetValue()
					}
				}
			}
		}
	}
	return 0
}
