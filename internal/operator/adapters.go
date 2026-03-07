// Package operator provides recovery and replication operators for NovaStor.
// This file implements adapters that bridge the operator package with
// the metadata and placement services.
package operator

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
)

// MetadataPlacementAdapter implements PlacementLookup by querying the
// metadata service via its gRPC client. It uses ListPlacementMaps to
// find which chunks are on a given node and GetPlacementMap to look up
// replica information for individual chunks.
type MetadataPlacementAdapter struct {
	client *metadata.GRPCClient
}

// NewMetadataPlacementAdapter creates a PlacementLookup backed by the
// given metadata gRPC client.
func NewMetadataPlacementAdapter(client *metadata.GRPCClient) *MetadataPlacementAdapter {
	return &MetadataPlacementAdapter{client: client}
}

// ChunksOnNode returns all chunk IDs that have a replica on the given node.
// It fetches all placement maps from metadata and filters by node membership.
func (a *MetadataPlacementAdapter) ChunksOnNode(ctx context.Context, nodeID string) ([]string, error) {
	pms, err := a.client.ListPlacementMaps(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing placement maps: %w", err)
	}
	var chunks []string
	for _, pm := range pms {
		for _, n := range pm.Nodes {
			if n == nodeID {
				chunks = append(chunks, pm.ChunkID)
				break
			}
		}
	}
	return chunks, nil
}

// ReplicaNodes returns the set of nodes that hold replicas of the given chunk.
func (a *MetadataPlacementAdapter) ReplicaNodes(ctx context.Context, chunkID string) ([]string, error) {
	pm, err := a.client.GetPlacementMap(ctx, chunkID)
	if err != nil {
		return nil, fmt.Errorf("getting placement map for chunk %s: %w", chunkID, err)
	}
	return pm.Nodes, nil
}

// UpdatePlacement replaces oldNode with newNode in the placement map for the
// given chunk and persists the updated map.
func (a *MetadataPlacementAdapter) UpdatePlacement(ctx context.Context, chunkID string, oldNode, newNode string) error {
	pm, err := a.client.GetPlacementMap(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("getting placement map for chunk %s: %w", chunkID, err)
	}
	updated := false
	for i, n := range pm.Nodes {
		if n == oldNode {
			pm.Nodes[i] = newNode
			updated = true
			break
		}
	}
	if !updated {
		return fmt.Errorf("node %s not found in placement map for chunk %s", oldNode, chunkID)
	}
	if err := a.client.PutPlacementMap(ctx, pm); err != nil {
		return fmt.Errorf("updating placement map for chunk %s: %w", chunkID, err)
	}
	return nil
}

// GRPCChunkReplicator implements ChunkReplicator by reading a chunk from
// the source agent node and writing it to the destination agent node
// using the agent gRPC client.
type GRPCChunkReplicator struct {
	mu         sync.RWMutex
	clients    map[string]*agent.Client
	metaClient *metadata.GRPCClient
}

// NewGRPCChunkReplicator creates a ChunkReplicator that resolves node
// addresses via the metadata service and connects to agent gRPC endpoints.
func NewGRPCChunkReplicator(metaClient *metadata.GRPCClient) *GRPCChunkReplicator {
	return &GRPCChunkReplicator{
		clients:    make(map[string]*agent.Client),
		metaClient: metaClient,
	}
}

// getOrDialClient returns a cached agent client for the given node or
// dials a new one by resolving the node address from metadata.
func (r *GRPCChunkReplicator) getOrDialClient(ctx context.Context, nodeID string) (*agent.Client, error) {
	r.mu.RLock()
	c, ok := r.clients[nodeID]
	r.mu.RUnlock()
	if ok {
		return c, nil
	}

	nodeMeta, err := r.metaClient.GetNodeMeta(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("resolving address for node %s: %w", nodeID, err)
	}

	c, err = agent.Dial(nodeMeta.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialing agent on node %s at %s: %w", nodeID, nodeMeta.Address, err)
	}

	r.mu.Lock()
	// Check again in case another goroutine dialed concurrently.
	if existing, ok := r.clients[nodeID]; ok {
		r.mu.Unlock()
		c.Close()
		return existing, nil
	}
	r.clients[nodeID] = c
	r.mu.Unlock()
	return c, nil
}

// ReplicateChunk reads a chunk from sourceNode and writes it to destNode.
func (r *GRPCChunkReplicator) ReplicateChunk(ctx context.Context, chunkID, sourceNode, destNode string) error {
	src, err := r.getOrDialClient(ctx, sourceNode)
	if err != nil {
		return fmt.Errorf("connecting to source node %s: %w", sourceNode, err)
	}

	data, checksum, err := src.GetChunk(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("reading chunk %s from node %s: %w", chunkID, sourceNode, err)
	}

	dst, err := r.getOrDialClient(ctx, destNode)
	if err != nil {
		return fmt.Errorf("connecting to dest node %s: %w", destNode, err)
	}

	// If the source returned a zero checksum, compute it ourselves.
	if checksum == 0 && len(data) > 0 {
		checksum = crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	}

	if err := dst.PutChunk(ctx, chunkID, data, checksum); err != nil {
		return fmt.Errorf("writing chunk %s to node %s: %w", chunkID, destNode, err)
	}

	logging.L.Info("replicated chunk",
		zap.String("chunkID", chunkID),
		zap.String("source", sourceNode),
		zap.String("dest", destNode),
		zap.Int("bytes", len(data)),
	)
	return nil
}

// Close shuts down all cached agent connections.
func (r *GRPCChunkReplicator) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, c := range r.clients {
		c.Close()
		delete(r.clients, id)
	}
}

// GRPCHealthChecker implements NodeHealthChecker by attempting a
// HasChunk RPC against each agent node. If the agent responds (even
// with "chunk not found"), the node is considered healthy.
type GRPCHealthChecker struct {
	mu         sync.RWMutex
	clients    map[string]*agent.Client
	metaClient *metadata.GRPCClient
	timeout    time.Duration
}

// NewGRPCHealthChecker creates a NodeHealthChecker that pings agent
// nodes via gRPC. The timeout controls how long each health check
// waits before declaring the node unhealthy.
func NewGRPCHealthChecker(metaClient *metadata.GRPCClient, timeout time.Duration) *GRPCHealthChecker {
	return &GRPCHealthChecker{
		clients:    make(map[string]*agent.Client),
		metaClient: metaClient,
		timeout:    timeout,
	}
}

// getOrDialClient returns a cached agent client or dials a new one.
func (h *GRPCHealthChecker) getOrDialClient(ctx context.Context, nodeID string) (*agent.Client, error) {
	h.mu.RLock()
	c, ok := h.clients[nodeID]
	h.mu.RUnlock()
	if ok {
		return c, nil
	}

	nodeMeta, err := h.metaClient.GetNodeMeta(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("resolving address for node %s: %w", nodeID, err)
	}

	c, err = agent.Dial(nodeMeta.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialing agent on node %s at %s: %w", nodeID, nodeMeta.Address, err)
	}

	h.mu.Lock()
	if existing, ok := h.clients[nodeID]; ok {
		h.mu.Unlock()
		c.Close()
		return existing, nil
	}
	h.clients[nodeID] = c
	h.mu.Unlock()
	return c, nil
}

// IsNodeHealthy checks if the agent on the given node responds to a
// HasChunk probe within the configured timeout. A response (even an
// error indicating the chunk doesn't exist) means the node is alive.
func (h *GRPCHealthChecker) IsNodeHealthy(ctx context.Context, nodeID string) bool {
	checkCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	c, err := h.getOrDialClient(checkCtx, nodeID)
	if err != nil {
		logging.L.Debug("health check failed: unable to connect",
			zap.String("nodeID", nodeID),
			zap.Error(err),
		)
		return false
	}

	// Use HasChunk with a dummy ID as a liveness probe. The agent will
	// respond with exists=false, which is still a valid response proving
	// the node is alive.
	_, err = c.HasChunk(checkCtx, "__health_probe__")
	if err != nil {
		logging.L.Debug("health check failed: HasChunk probe error",
			zap.String("nodeID", nodeID),
			zap.Error(err),
		)
		return false
	}
	return true
}

// Close shuts down all cached agent connections.
func (h *GRPCHealthChecker) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, c := range h.clients {
		c.Close()
		delete(h.clients, id)
	}
}
