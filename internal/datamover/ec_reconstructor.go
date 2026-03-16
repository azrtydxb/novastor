// Package datamover provides data movement functionality for NovaStor.
// This package handles reconstruction of erasure-coded chunks and
// data rebalancing between storage nodes.
//
// ARCHITECTURE VIOLATION: EC reconstruction (ECReconstructor and agentReplicator) is
// implemented in Go, but per the NovaStor layered architecture, ALL data-path I/O
// (including erasure coding encode/decode/reconstruct) MUST happen in the Rust SPDK
// dataplane. The Go agent is management/config only.
//
// This Go implementation should be replaced with gRPC calls to the Rust dataplane's
// EC reconstruction service. The dataplane already owns the chunk engine and has access
// to the Reed-Solomon codec via the klauspost/reedsolomon Rust equivalent. The correct
// flow is:
//  1. Go controller detects a missing/degraded shard via policy engine
//  2. Go controller issues a ReconstructShard gRPC call to the Rust dataplane
//  3. Rust dataplane reads available shards, reconstructs, and writes the result
//  4. Rust dataplane reports completion back to Go via gRPC response
//
// Until the Rust dataplane EC reconstruction gRPC service is implemented, this Go code
// exists as a placeholder to define the interface contract. It MUST NOT be used in
// production — it violates invariant #3 (Go agent never touches data).
package datamover

import (
	"context"
	"fmt"
	"sync"

	"github.com/azrtydxb/novastor/internal/chunk"
	"github.com/azrtydxb/novastor/internal/metadata"
)

// AgentClient defines the interface for communicating with agent nodes.
type AgentClient interface {
	// GetChunk retrieves a whole chunk from a node
	GetChunk(ctx context.Context, nodeID string, chunkID string) (*chunk.Chunk, error)

	// PutChunk stores a whole chunk on a node
	PutChunk(ctx context.Context, nodeID string, c *chunk.Chunk) error

	// GetChunkShard retrieves a specific EC shard from a node
	GetChunkShard(ctx context.Context, nodeID string, chunkID string, shardIndex int) ([]byte, error)

	// PutChunkShard stores a specific EC shard on a node
	PutChunkShard(ctx context.Context, nodeID string, chunkID string, shardIndex int, data []byte) error

	// DeleteChunk removes a chunk from a node
	DeleteChunk(ctx context.Context, nodeID string, chunkID string) error
}

// ECReconstructor implements EC shard reconstruction.
type ECReconstructor struct {
	agentClient AgentClient
}

// NewECReconstructor creates a new EC reconstructor.
func NewECReconstructor(agentClient AgentClient) *ECReconstructor {
	return &ECReconstructor{agentClient: agentClient}
}

// ReconstructShard reads available EC shards from their nodes, reconstructs
// the missing shard using the erasure coder, and writes it to the destination node.
func (r *ECReconstructor) ReconstructShard(
	ctx context.Context,
	chunkID string,
	missingShardIndex int,
	sourcePlacements []*metadata.ShardPlacement,
	destNode string,
	ec *chunk.ErasureCoder,
) error {
	totalShards := ec.ShardCount()
	if missingShardIndex < 0 || missingShardIndex >= totalShards {
		return fmt.Errorf("invalid shard index %d, must be 0..%d", missingShardIndex, totalShards-1)
	}

	// Build shard array: nil for missing shards, []byte for available ones
	shards := make([][]byte, totalShards)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var lastErr error

	// Read all available shards in parallel
	for _, sp := range sourcePlacements {
		if sp.ShardIndex < 0 || sp.ShardIndex >= totalShards {
			continue // Invalid shard index
		}
		if sp.NodeID == destNode {
			continue // Don't read from destination node
		}

		wg.Add(1)
		go func(sp *metadata.ShardPlacement) {
			defer wg.Done()

			shardData, err := r.agentClient.GetChunkShard(ctx, sp.NodeID, sp.ChunkID, sp.ShardIndex)
			if err != nil {
				mu.Lock()
				lastErr = fmt.Errorf("reading shard %d from %s: %w", sp.ShardIndex, sp.NodeID, err)
				mu.Unlock()
				return
			}

			mu.Lock()
			shards[sp.ShardIndex] = shardData
			mu.Unlock()
		}(sp)
	}
	wg.Wait()

	if lastErr != nil {
		return lastErr
	}

	// Check we have enough shards to reconstruct
	availableCount := 0
	for _, s := range shards {
		if s != nil {
			availableCount++
		}
	}
	if availableCount < ec.DataShards() {
		return fmt.Errorf("not enough shards to reconstruct: have %d, need %d", availableCount, ec.DataShards())
	}

	// Reconstruct the missing shard(s)
	// The Reconstruct method fills in nil entries
	if err := ec.Reconstruct(shards); err != nil {
		return fmt.Errorf("reconstructing shards: %w", err)
	}

	// Write the reconstructed shard to the destination
	reconstructedShard := shards[missingShardIndex]
	if reconstructedShard == nil {
		return fmt.Errorf("reconstruction failed: shard %d is still nil", missingShardIndex)
	}

	if err := r.agentClient.PutChunkShard(ctx, destNode, chunkID, missingShardIndex, reconstructedShard); err != nil {
		return fmt.Errorf("writing reconstructed shard %d to %s: %w", missingShardIndex, destNode, err)
	}

	return nil
}

// agentReplicator implements ShardReplicator using an AgentClient.
type agentReplicator struct {
	client AgentClient
}

// NewAgentReplicator creates a new ShardReplicator backed by an AgentClient.
func NewAgentReplicator(client AgentClient) ShardReplicator {
	return &agentReplicator{client: client}
}

func (r *agentReplicator) ReplicateChunk(ctx context.Context, chunkID string, sourceNode, destNode string) error {
	// Read chunk from source
	c, err := r.client.GetChunk(ctx, sourceNode, chunkID)
	if err != nil {
		return fmt.Errorf("reading chunk %s from %s: %w", chunkID, sourceNode, err)
	}

	// Write chunk to destination
	if err := r.client.PutChunk(ctx, destNode, c); err != nil {
		return fmt.Errorf("writing chunk %s to %s: %w", chunkID, destNode, err)
	}

	return nil
}

func (r *agentReplicator) ReplicateShard(ctx context.Context, chunkID string, shardIndex int, sourceNode, destNode string) error {
	// Read shard from source
	shardData, err := r.client.GetChunkShard(ctx, sourceNode, chunkID, shardIndex)
	if err != nil {
		return fmt.Errorf("reading shard %d of chunk %s from %s: %w", shardIndex, chunkID, sourceNode, err)
	}

	// Write shard to destination
	if err := r.client.PutChunkShard(ctx, destNode, chunkID, shardIndex, shardData); err != nil {
		return fmt.Errorf("writing shard %d of chunk %s to %s: %w", shardIndex, chunkID, destNode, err)
	}

	return nil
}

func (r *agentReplicator) ReconstructShard(ctx context.Context, chunkID string, shardIndex int, sourcePlacements []*metadata.ShardPlacement, destNode string, ec *chunk.ErasureCoder) error {
	reconstructor := NewECReconstructor(r.client)
	return reconstructor.ReconstructShard(ctx, chunkID, shardIndex, sourcePlacements, destNode, ec)
}

func (r *agentReplicator) DeleteChunk(ctx context.Context, chunkID string, node string) error {
	return r.client.DeleteChunk(ctx, node, chunkID)
}
