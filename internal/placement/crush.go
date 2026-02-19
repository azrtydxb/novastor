// Package placement provides chunk placement algorithms for NovaStor.
// This file implements a CRUSH-style placement algorithm that distributes
// chunks across storage nodes based on failure domains and weights.
package placement

import (
	"math"
	"sort"
	"sync"
)

// Node represents a storage node in the CRUSH topology.
type Node struct {
	ID     string
	Weight float64 // Relative capacity weight (e.g. 1.0 = standard, 2.0 = double capacity).
	Zone   string  // Failure domain: zone (e.g. "us-east-1a").
	Rack   string  // Failure domain: rack (e.g. "rack-A").
}

// FailureDomain controls the granularity of replica separation.
type FailureDomain string

const (
	// DomainNode places replicas on distinct nodes (no topology awareness).
	DomainNode FailureDomain = "node"
	// DomainRack places replicas on nodes in distinct racks.
	DomainRack FailureDomain = "rack"
	// DomainZone places replicas on nodes in distinct zones.
	DomainZone FailureDomain = "zone"
)

// CRUSHPlacer implements the Placer interface using a straw2-like CRUSH
// algorithm with failure-domain awareness and weighted node selection.
type CRUSHPlacer struct {
	mu            sync.RWMutex
	nodes         []Node
	failureDomain FailureDomain
}

// NewCRUSHPlacer creates a CRUSHPlacer from the given node list. The default
// failure domain is DomainNode (no topology constraint).
func NewCRUSHPlacer(nodes []Node) *CRUSHPlacer {
	cp := make([]Node, len(nodes))
	copy(cp, nodes)
	return &CRUSHPlacer{
		nodes:         cp,
		failureDomain: DomainNode,
	}
}

// SetFailureDomain configures the failure domain used during placement.
// Valid values are "zone", "rack", or "node".
func (c *CRUSHPlacer) SetFailureDomain(domain string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch FailureDomain(domain) {
	case DomainZone, DomainRack, DomainNode:
		c.failureDomain = FailureDomain(domain)
	default:
		c.failureDomain = DomainNode
	}
}

// AddNode adds a node with default weight 1.0, empty zone and rack.
// It satisfies the Placer interface.
func (c *CRUSHPlacer) AddNode(nodeID string) {
	c.AddWeightedNode(Node{ID: nodeID, Weight: 1.0})
}

// AddWeightedNode adds a fully specified node to the CRUSH map.
// If a node with the same ID already exists it is replaced.
func (c *CRUSHPlacer) AddWeightedNode(node Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, n := range c.nodes {
		if n.ID == node.ID {
			c.nodes[i] = node
			return
		}
	}
	c.nodes = append(c.nodes, node)
}

// RemoveNode removes the node with the given ID.
func (c *CRUSHPlacer) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, n := range c.nodes {
		if n.ID == nodeID {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			return
		}
	}
}

// Place provides legacy round-robin-compatible placement. It delegates to
// PlaceKey with a fixed key, so successive calls return the same result.
func (c *CRUSHPlacer) Place(count int) []string {
	return c.PlaceKey("__legacy__", count)
}

// PlaceKey returns a deterministic set of node IDs for the given key using a
// straw2-like weighted hash selection with failure-domain awareness.
//
// The algorithm:
//  1. Group nodes by the active failure domain.
//  2. For each node, compute a straw2 draw: -hash(key, nodeID) / weight.
//     The node with the lowest draw (longest straw) wins in each domain group.
//  3. Sort domain groups by their best node's draw (ascending = best first).
//  4. Return the top `count` nodes, one per domain group. If there are fewer
//     domain groups than `count`, fill remaining slots from already-used groups
//     (best unused node within each group, round-robin across groups).
func (c *CRUSHPlacer) PlaceKey(key string, count int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.nodes) == 0 || count <= 0 {
		return nil
	}
	if count > len(c.nodes) {
		count = len(c.nodes)
	}

	// Build per-domain groups with straw2 draws.
	type scored struct {
		node Node
		draw float64 // lower = better (node is more likely to be chosen)
	}
	groups := make(map[string][]scored)
	for _, n := range c.nodes {
		domainKey := c.domainKey(n)
		d := straw2Draw(key, n.ID, n.Weight)
		groups[domainKey] = append(groups[domainKey], scored{node: n, draw: d})
	}

	// Sort nodes within each group by draw ascending (best first).
	for dk := range groups {
		sort.Slice(groups[dk], func(i, j int) bool {
			return groups[dk][i].draw < groups[dk][j].draw
		})
	}

	// Sort domain groups by their best (first) node's draw ascending.
	domainKeys := make([]string, 0, len(groups))
	for dk := range groups {
		domainKeys = append(domainKeys, dk)
	}
	sort.Slice(domainKeys, func(i, j int) bool {
		return groups[domainKeys[i]][0].draw < groups[domainKeys[j]][0].draw
	})

	// Pick nodes: one per domain first, then fill from remaining.
	selected := make([]string, 0, count)
	usedNodes := make(map[string]bool)
	domainIdx := make(map[string]int, len(domainKeys))

	// Phase 1: one node per distinct failure domain.
	for _, dk := range domainKeys {
		if len(selected) >= count {
			break
		}
		n := groups[dk][0]
		selected = append(selected, n.node.ID)
		usedNodes[n.node.ID] = true
		domainIdx[dk] = 1
	}

	// Phase 2: if we still need more, cycle through domains picking next-best.
	for len(selected) < count {
		added := false
		for _, dk := range domainKeys {
			if len(selected) >= count {
				break
			}
			idx := domainIdx[dk]
			g := groups[dk]
			for idx < len(g) && usedNodes[g[idx].node.ID] {
				idx++
			}
			if idx < len(g) {
				selected = append(selected, g[idx].node.ID)
				usedNodes[g[idx].node.ID] = true
				domainIdx[dk] = idx + 1
				added = true
			}
		}
		if !added {
			break
		}
	}

	return selected
}

// domainKey returns the failure-domain bucket key for a node.
func (c *CRUSHPlacer) domainKey(n Node) string {
	switch c.failureDomain {
	case DomainZone:
		return "zone:" + n.Zone
	case DomainRack:
		return "rack:" + n.Rack
	default:
		return "node:" + n.ID
	}
}

// straw2Draw computes the straw2 draw value for a (key, nodeID) pair weighted
// by the node's weight. This is based on the Ceph CRUSH straw2 algorithm:
//
//	draw = -ln(hash(key, nodeID) / maxHash) / weight
//
// A lower draw means the node is more likely to be selected. Nodes with higher
// weight produce proportionally lower draws on average.
//
// The key property for stability: each node's draw depends only on the key and
// that node's ID/weight. Adding or removing other nodes does not change any
// existing node's draw, so only the keys assigned to the changed node move.
func straw2Draw(key, nodeID string, weight float64) float64 {
	if weight <= 0 {
		return math.MaxFloat64
	}

	h := crushHash(key, nodeID)

	// Map hash to (0, 1] -- avoid 0 to prevent -ln(0) = +inf.
	u := float64(h|1) / float64(math.MaxUint64)

	// Exponential draw divided by weight.
	return -math.Log(u) / weight
}

// crushHash produces a well-distributed 64-bit hash for (key, nodeID).
// Uses a variant of the FNV-1a / xxHash-style mixing to ensure that similar
// nodeIDs produce uncorrelated outputs (critical for placement stability).
func crushHash(key, nodeID string) uint64 {
	// Start with FNV offset basis.
	var h uint64 = 14695981039346656037

	// Mix in the key bytes.
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211 // FNV prime
	}

	// Add a separator to avoid collisions between key="ab",node="c" and key="a",node="bc".
	h ^= 0xff
	h *= 1099511628211

	// Mix in the nodeID bytes.
	for i := 0; i < len(nodeID); i++ {
		h ^= uint64(nodeID[i])
		h *= 1099511628211
	}

	// Final avalanche mixing (murmurhash3 finalizer) to improve distribution.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33

	return h
}
