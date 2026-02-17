package placement

import "testing"

func TestRoundRobin_PlaceReplicated(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2", "node-3"}
	rr := NewRoundRobin(nodes)
	targets := rr.Place(3)
	if len(targets) != 3 {
		t.Fatalf("Place(3) = %d nodes, want 3", len(targets))
	}
	seen := make(map[string]bool)
	for _, n := range targets {
		if seen[n] {
			t.Errorf("duplicate node: %s", n)
		}
		seen[n] = true
	}
}

func TestRoundRobin_PlaceDistributes(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2"}
	rr := NewRoundRobin(nodes)
	counts := make(map[string]int)
	for i := 0; i < 30; i++ {
		targets := rr.Place(1)
		counts[targets[0]]++
	}
	for _, node := range nodes {
		if counts[node] != 10 {
			t.Errorf("node %s got %d placements, want 10", node, counts[node])
		}
	}
}

func TestRoundRobin_PlaceMoreThanAvailable(t *testing.T) {
	nodes := []string{"node-0", "node-1"}
	rr := NewRoundRobin(nodes)
	targets := rr.Place(5)
	if len(targets) != 2 {
		t.Errorf("Place(5) with 2 nodes = %d, want 2 (capped)", len(targets))
	}
}

func TestRoundRobin_PlaceZero(t *testing.T) {
	nodes := []string{"node-0"}
	rr := NewRoundRobin(nodes)
	targets := rr.Place(0)
	if len(targets) != 0 {
		t.Errorf("Place(0) = %d, want 0", len(targets))
	}
}

func TestRoundRobin_NoNodes(t *testing.T) {
	rr := NewRoundRobin(nil)
	targets := rr.Place(3)
	if len(targets) != 0 {
		t.Errorf("Place with no nodes = %d, want 0", len(targets))
	}
}

func TestRoundRobin_AddRemoveNode(t *testing.T) {
	rr := NewRoundRobin([]string{"node-0", "node-1"})
	rr.AddNode("node-2")
	targets := rr.Place(3)
	if len(targets) != 3 {
		t.Fatalf("Place(3) after add = %d, want 3", len(targets))
	}
	rr.RemoveNode("node-1")
	targets = rr.Place(3)
	if len(targets) != 2 {
		t.Errorf("Place(3) after remove = %d, want 2", len(targets))
	}
}
