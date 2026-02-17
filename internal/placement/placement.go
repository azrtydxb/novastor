package placement

import "sync"

type Placer interface {
	Place(count int) []string
	AddNode(nodeID string)
	RemoveNode(nodeID string)
}

type RoundRobin struct {
	mu    sync.Mutex
	nodes []string
	index int
}

func NewRoundRobin(nodes []string) *RoundRobin {
	cp := make([]string, len(nodes))
	copy(cp, nodes)
	return &RoundRobin{nodes: cp}
}

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
