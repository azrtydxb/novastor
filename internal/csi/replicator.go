package csi

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/logging"
)

// ChunkReplicator replicates chunks between agent nodes by address.
type ChunkReplicator interface {
	// ReplicateChunk copies a chunk from sourceAddr to destAddr.
	ReplicateChunk(ctx context.Context, chunkID, sourceAddr, destAddr string) error
	// Close releases all cached connections.
	Close()
}

// GRPCAddrReplicator implements ChunkReplicator using agent gRPC clients,
// connecting to agents by their network address directly.
type GRPCAddrReplicator struct {
	mu       sync.Mutex
	clients  map[string]*agent.Client
	dialOpts []grpc.DialOption
}

// NewGRPCAddrReplicator creates a replicator that dials agents using the
// provided gRPC dial options (e.g. TLS credentials).
func NewGRPCAddrReplicator(opts ...grpc.DialOption) *GRPCAddrReplicator {
	return &GRPCAddrReplicator{
		clients:  make(map[string]*agent.Client),
		dialOpts: opts,
	}
}

// getOrDialClient returns a cached agent.Client for the address, dialing
// a new connection if one does not already exist.
func (r *GRPCAddrReplicator) getOrDialClient(addr string) (*agent.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[addr]; ok {
		return c, nil
	}
	c, err := agent.Dial(addr, r.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dialing agent at %s: %w", addr, err)
	}
	r.clients[addr] = c
	return c, nil
}

// ReplicateChunk reads the chunk from sourceAddr and writes it to destAddr.
func (r *GRPCAddrReplicator) ReplicateChunk(ctx context.Context, chunkID, sourceAddr, destAddr string) error {
	src, err := r.getOrDialClient(sourceAddr)
	if err != nil {
		return fmt.Errorf("connecting to source %s: %w", sourceAddr, err)
	}

	data, checksum, err := src.GetChunk(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("GetChunk %s from %s: %w", chunkID, sourceAddr, err)
	}

	dst, err := r.getOrDialClient(destAddr)
	if err != nil {
		return fmt.Errorf("connecting to dest %s: %w", destAddr, err)
	}

	if err := dst.PutChunk(ctx, chunkID, data, checksum); err != nil {
		return fmt.Errorf("PutChunk %s to %s: %w", chunkID, destAddr, err)
	}

	logging.L.Debug("replicated chunk",
		zap.String("chunkID", chunkID),
		zap.String("from", sourceAddr),
		zap.String("to", destAddr),
		zap.Int("bytes", len(data)))

	return nil
}

// Close closes all cached gRPC connections.
func (r *GRPCAddrReplicator) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for addr, c := range r.clients {
		if err := c.Close(); err != nil {
			logging.L.Warn("failed to close replicator connection",
				zap.String("addr", addr),
				zap.Error(err))
		}
	}
	r.clients = make(map[string]*agent.Client)
}
