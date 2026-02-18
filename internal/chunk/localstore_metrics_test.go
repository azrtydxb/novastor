package chunk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/piwi3910/novastor/internal/metrics"
)

func TestLocalStore_MetricsInstrumentation(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "test-metrics-store")
	defer os.RemoveAll(dir)

	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	// Register metrics
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.ChunkOpsTotal)
	reg.MustRegister(metrics.ChunkBytesTotal)

	ctx := context.Background()
	data := []byte("test data for metrics")
	chunk := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	chunk.Checksum = chunk.ComputeChecksum()

	// Test Put operation metrics
	initialWrites := testutil.ToFloat64(metrics.ChunkOpsTotal.WithLabelValues("write"))
	initialWriteBytes := testutil.ToFloat64(metrics.ChunkBytesTotal.WithLabelValues("write"))

	if err := store.Put(ctx, chunk); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	finalWrites := testutil.ToFloat64(metrics.ChunkOpsTotal.WithLabelValues("write"))
	finalWriteBytes := testutil.ToFloat64(metrics.ChunkBytesTotal.WithLabelValues("write"))

	if finalWrites != initialWrites+1 {
		t.Errorf("ChunkOpsTotal[write] not incremented: got %.0f, want %.0f", finalWrites, initialWrites+1)
	}

	expectedBytes := float64(len(data))
	if finalWriteBytes != initialWriteBytes+expectedBytes {
		t.Errorf("ChunkBytesTotal[write] not incremented correctly: got %.0f, want %.0f", finalWriteBytes, initialWriteBytes+expectedBytes)
	}

	// Test Get operation metrics
	initialReads := testutil.ToFloat64(metrics.ChunkOpsTotal.WithLabelValues("read"))
	initialReadBytes := testutil.ToFloat64(metrics.ChunkBytesTotal.WithLabelValues("read"))

	_, err = store.Get(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	finalReads := testutil.ToFloat64(metrics.ChunkOpsTotal.WithLabelValues("read"))
	finalReadBytes := testutil.ToFloat64(metrics.ChunkBytesTotal.WithLabelValues("read"))

	if finalReads != initialReads+1 {
		t.Errorf("ChunkOpsTotal[read] not incremented: got %.0f, want %.0f", finalReads, initialReads+1)
	}

	if finalReadBytes != initialReadBytes+expectedBytes {
		t.Errorf("ChunkBytesTotal[read] not incremented correctly: got %.0f, want %.0f", finalReadBytes, initialReadBytes+expectedBytes)
	}

	// Test Delete operation metrics
	initialDeletes := testutil.ToFloat64(metrics.ChunkOpsTotal.WithLabelValues("delete"))

	if err := store.Delete(ctx, chunk.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	finalDeletes := testutil.ToFloat64(metrics.ChunkOpsTotal.WithLabelValues("delete"))

	if finalDeletes != initialDeletes+1 {
		t.Errorf("ChunkOpsTotal[delete] not incremented: got %.0f, want %.0f", finalDeletes, initialDeletes+1)
	}
}
