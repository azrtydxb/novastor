package placement

import "sync"

// Placer selects storage nodes for chunk placement.
//
// NOTE: When NVMe-oF integration is enabled, node IDs returned by Place and
// PlaceKey must be network addresses (host:port format) that can be used for
// gRPC communication with the agent's NVMeTargetService. If logical node IDs
// are used instead, the CSI controller will fail to reach agents when creating
// NVMe-oF targets. To support separate logical IDs and network addresses, a
// nodeID→address resolution mechanism would need to be added to the controller.
type Placer interface {
	// PlaceKey returns a deterministic set of node IDs for the given key.
	// The same key always produces the same placement (assuming the node set
	// has not changed). Implementations that do not support keyed placement
	// may fall back to Place.
	PlaceKey(key string, count int) []string

	// Place returns node IDs using a non-deterministic or round-robin strategy.
	Place(count int) []string

	AddNode(nodeID string)
	RemoveNode(nodeID string)
}

// RoundRobin implements the Placer interface using round-robin node selection.
type RoundRobin struct {
	mu    sync.Mutex
	nodes []string
	index int
}

// NewRoundRobin creates a new round-robin placer with the given initial nodes.
func NewRoundRobin(nodes []string) *RoundRobin {
	cp := make([]string, len(nodes))
	copy(cp, nodes)
	return &RoundRobin{nodes: cp}
}

// PlaceKey falls back to Place for RoundRobin (no key-based placement).
func (rr *RoundRobin) PlaceKey(_ string, count int) []string {
	return rr.Place(count)
}

// Place selects the next count nodes in round-robin order.
func (rr *RoundRobin) Place(count int) []string {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if len(rr.nodes) == 0 || count <= 0 {
		return nil
	}
	if count > len(rr.nodes) {
		count = len(rr.nodes)
	}
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = rr.nodes[(rr.index+i)%len(rr.nodes)]
	}
	rr.index = (rr.index + count) % len(rr.nodes)
	return result
}

// AddNode adds a node to the round-robin pool if not already present.
func (rr *RoundRobin) AddNode(nodeID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for _, n := range rr.nodes {
		if n == nodeID {
			return
		}
	}
	rr.nodes = append(rr.nodes, nodeID)
}

// RemoveNode removes a node from the round-robin pool.
func (rr *RoundRobin) RemoveNode(nodeID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for i, n := range rr.nodes {
		if n == nodeID {
			rr.nodes = append(rr.nodes[:i], rr.nodes[i+1:]...)
			if rr.index >= len(rr.nodes) && len(rr.nodes) > 0 {
				rr.index = rr.index % len(rr.nodes)
			}
			return
		}
	}
}
