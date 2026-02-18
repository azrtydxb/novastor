package datamover

import (
	"context"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/chunk"
	"github.com/piwi3910/novastor/internal/metadata"
)

// mockReplicator is a test double for ShardReplicator.
type mockReplicator struct {
	replicateChunkFunc   func(ctx context.Context, chunkID string, sourceNode, destNode string) error
	replicateShardFunc   func(ctx context.Context, chunkID string, shardIndex int, sourceNode, destNode string) error
	reconstructShardFunc func(ctx context.Context, chunkID string, shardIndex int, sourcePlacements []*metadata.ShardPlacement, destNode string, ec *chunk.ErasureCoder) error
	deleteChunkFunc      func(ctx context.Context, chunkID string, node string) error
}

func (m *mockReplicator) ReplicateChunk(ctx context.Context, chunkID string, sourceNode, destNode string) error {
	if m.replicateChunkFunc != nil {
		return m.replicateChunkFunc(ctx, chunkID, sourceNode, destNode)
	}
	return nil
}

func (m *mockReplicator) ReplicateShard(ctx context.Context, chunkID string, shardIndex int, sourceNode, destNode string) error {
	if m.replicateShardFunc != nil {
		return m.replicateShardFunc(ctx, chunkID, shardIndex, sourceNode, destNode)
	}
	return nil
}

func (m *mockReplicator) ReconstructShard(ctx context.Context, chunkID string, shardIndex int, sourcePlacements []*metadata.ShardPlacement, destNode string, ec *chunk.ErasureCoder) error {
	if m.reconstructShardFunc != nil {
		return m.reconstructShardFunc(ctx, chunkID, shardIndex, sourcePlacements, destNode, ec)
	}
	return nil
}

func (m *mockReplicator) DeleteChunk(ctx context.Context, chunkID string, node string) error {
	if m.deleteChunkFunc != nil {
		return m.deleteChunkFunc(ctx, chunkID, node)
	}
	return nil
}

func TestThrottledReplicator_BandwidthLimit(t *testing.T) {
	// Test that throttling is configured correctly
	inner := &mockReplicator{
		replicateChunkFunc: func(ctx context.Context, chunkID string, sourceNode, destNode string) error {
			return nil
		},
	}

	bandwidth := int64(10 * 1024 * 1024) // 10MB/s
	tr := NewThrottledReplicator(inner, bandwidth)

	if got := tr.GetBandwidth(); got != bandwidth {
		t.Errorf("expected bandwidth %d, got %d", bandwidth, got)
	}

	// Test that small operations work without delay
	ctx := context.Background()
	if err := tr.ReplicateChunk(ctx, "test-chunk", "node1", "node2"); err != nil {
		t.Fatalf("ReplicateChunk failed: %v", err)
	}
}

func TestThrottledReplicator_SetBandwidth(t *testing.T) {
	inner := &mockReplicator{}
	tr := NewThrottledReplicator(inner, 1000)

	if got := tr.GetBandwidth(); got != 1000 {
		t.Errorf("expected bandwidth 1000, got %d", got)
	}

	tr.SetBandwidth(2000)
	if got := tr.GetBandwidth(); got != 2000 {
		t.Errorf("expected bandwidth 2000, got %d", got)
	}
}

func TestNewDefaultErasureCoderFactory(t *testing.T) {
	f := NewDefaultErasureCoderFactory()

	ec1, err := f.GetErasureCoder(4, 2)
	if err != nil {
		t.Fatalf("GetErasureCoder failed: %v", err)
	}

	ec2, err := f.GetErasureCoder(4, 2)
	if err != nil {
		t.Fatalf("GetErasureCoder failed: %v", err)
	}

	// Should return the same cached instance
	if ec1 != ec2 {
		t.Error("expected same erasure coder instance for same parameters")
	}

	ec3, err := f.GetErasureCoder(6, 3)
	if err != nil {
		t.Fatalf("GetErasureCoder failed: %v", err)
	}

	// Different parameters should return different instance
	if ec1 == ec3 {
		t.Error("expected different erasure coder instance for different parameters")
	}

	// Verify the coder has correct shard counts
	if ec1.ShardCount() != 6 {
		t.Errorf("expected 6 shards for 4+2, got %d", ec1.ShardCount())
	}
	if ec3.ShardCount() != 9 {
		t.Errorf("expected 9 shards for 6+3, got %d", ec3.ShardCount())
	}
}

func TestLockID(t *testing.T) {
	tests := []struct {
		chunkID  string
		destNode string
		expected string
	}{
		{"chunk-1", "node-1", "chunk-1:node-1"},
		{"chunk-2", "node-2", "chunk-2:node-2"},
		{"abc:def", "ghi:jkl", "abc:def:ghi:jkl"},
	}

	for _, tt := range tests {
		t.Run(tt.chunkID+"_"+tt.destNode, func(t *testing.T) {
			got := LockID(tt.chunkID, tt.destNode)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.WorkerCount != 4 {
		t.Errorf("expected WorkerCount=4, got %d", cfg.WorkerCount)
	}
	if cfg.BandwidthBytesPerSecond != 100*1024*1024 {
		t.Errorf("expected BandwidthBytesPerSecond=100MB, got %d", cfg.BandwidthBytesPerSecond)
	}
	if cfg.RetryMaxAttempts != 5 {
		t.Errorf("expected RetryMaxAttempts=5, got %d", cfg.RetryMaxAttempts)
	}
	if cfg.RetryBackoffBase != 10*time.Second {
		t.Errorf("expected RetryBackoffBase=10s, got %v", cfg.RetryBackoffBase)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("expected HeartbeatInterval=30s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.LockTTL != 5*time.Minute {
		t.Errorf("expected LockTTL=5m, got %v", cfg.LockTTL)
	}
}
