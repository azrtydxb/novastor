package chunk

import (
	"context"
	"testing"

	"github.com/piwi3910/novastor/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// TestChunkStoreMetrics verifies that chunk store operations
// properly increment their corresponding Prometheus metrics.
func TestChunkStoreMetrics(t *testing.T) {
	// Use a test registry that doesn't interfere with the global one.
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
		ID:   ChunkID("test-chunk"),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Check write operation metric.
	writeOps, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	foundWriteOp := false
	for _, mf := range writeOps {
		if mf.GetName() == "novastor_agent_chunk_ops_total" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.Label {
					if label.GetName() == "operation" && label.GetValue() == "write" {
						if m.Counter.GetValue() != 1 {
							t.Errorf("expected 1 write operation, got %f", m.Counter.GetValue())
						}
						foundWriteOp = true
					}
				}
			}
		}
	}
	if !foundWriteOp {
		t.Error("write operation metric not found or not incremented")
	}

	// Read the chunk and check metrics.
	_, err = store.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Check read operation metric.
	readOps, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	foundReadOp := false
	for _, mf := range readOps {
		if mf.GetName() == "novastor_agent_chunk_ops_total" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.Label {
					if label.GetName() == "operation" && label.GetValue() == "read" {
						if m.Counter.GetValue() != 1 {
							t.Errorf("expected 1 read operation, got %f", m.Counter.GetValue())
						}
						foundReadOp = true
					}
				}
			}
		}
	}
	if !foundReadOp {
		t.Error("read operation metric not found or not incremented")
	}

	// Delete the chunk and check metrics.
	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Check delete operation metric.
	deleteOps, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	foundDeleteOp := false
	for _, mf := range deleteOps {
		if mf.GetName() == "novastor_agent_chunk_ops_total" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.Label {
					if label.GetName() == "operation" && label.GetValue() == "delete" {
						if m.Counter.GetValue() != 1 {
							t.Errorf("expected 1 delete operation, got %f", m.Counter.GetValue())
						}
						foundDeleteOp = true
					}
				}
			}
		}
	}
	if !foundDeleteOp {
		t.Error("delete operation metric not found or not incremented")
	}
}
