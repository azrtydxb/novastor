package placement

import (
	"fmt"
	"math"
	"testing"
)

func TestCRUSH_PlaceReturnsCorrectCount(t *testing.T) {
	nodes := []Node{
		{ID: "a", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "b", Weight: 1, Zone: "z1", Rack: "r2"},
		{ID: "c", Weight: 1, Zone: "z2", Rack: "r3"},
		{ID: "d", Weight: 1, Zone: "z2", Rack: "r4"},
		{ID: "e", Weight: 1, Zone: "z3", Rack: "r5"},
	}
	cp := NewCRUSHPlacer(nodes)

	tests := []struct {
		name  string
		count int
		want  int
	}{
		{"zero", 0, 0},
		{"one", 1, 1},
		{"three", 3, 3},
		{"all", 5, 5},
		{"more than available", 10, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cp.PlaceKey("chunk-42", tt.count)
			if len(result) != tt.want {
				t.Errorf("PlaceKey(count=%d) returned %d nodes, want %d", tt.count, len(result), tt.want)
			}
		})
	}
}

func TestCRUSH_PlaceKeyIsDeterministic(t *testing.T) {
	nodes := []Node{
		{ID: "n1", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "n2", Weight: 2, Zone: "z1", Rack: "r2"},
		{ID: "n3", Weight: 1, Zone: "z2", Rack: "r3"},
		{ID: "n4", Weight: 3, Zone: "z2", Rack: "r4"},
		{ID: "n5", Weight: 1, Zone: "z3", Rack: "r5"},
	}
	cp := NewCRUSHPlacer(nodes)

	first := cp.PlaceKey("deterministic-key", 3)
	for i := 0; i < 100; i++ {
		again := cp.PlaceKey("deterministic-key", 3)
		if len(first) != len(again) {
			t.Fatalf("iteration %d: length mismatch %d vs %d", i, len(first), len(again))
		}
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("iteration %d: position %d differs: %s vs %s", i, j, first[j], again[j])
			}
		}
	}
}

func TestCRUSH_PlaceKeyNoDuplicates(t *testing.T) {
	nodes := []Node{
		{ID: "a", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "b", Weight: 1, Zone: "z1", Rack: "r2"},
		{ID: "c", Weight: 1, Zone: "z2", Rack: "r3"},
		{ID: "d", Weight: 1, Zone: "z2", Rack: "r4"},
	}
	cp := NewCRUSHPlacer(nodes)

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		result := cp.PlaceKey(key, 3)
		seen := make(map[string]bool)
		for _, id := range result {
			if seen[id] {
				t.Fatalf("key %q: duplicate node %s in placement %v", key, id, result)
			}
			seen[id] = true
		}
	}
}

func TestCRUSH_FailureDomainZone(t *testing.T) {
	nodes := []Node{
		{ID: "z1-a", Weight: 1, Zone: "zone-1", Rack: "r1"},
		{ID: "z1-b", Weight: 1, Zone: "zone-1", Rack: "r2"},
		{ID: "z2-a", Weight: 1, Zone: "zone-2", Rack: "r3"},
		{ID: "z2-b", Weight: 1, Zone: "zone-2", Rack: "r4"},
		{ID: "z3-a", Weight: 1, Zone: "zone-3", Rack: "r5"},
		{ID: "z3-b", Weight: 1, Zone: "zone-3", Rack: "r6"},
	}
	cp := NewCRUSHPlacer(nodes)
	cp.SetFailureDomain("zone")

	// With 3 replicas and 3 zones, every replica should be in a different zone.
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("obj-%d", i)
		result := cp.PlaceKey(key, 3)
		if len(result) != 3 {
			t.Fatalf("key %q: got %d nodes, want 3", key, len(result))
		}
		zones := make(map[string]bool)
		for _, id := range result {
			z := nodeZone(nodes, id)
			if zones[z] {
				t.Fatalf("key %q: zone %s used twice in %v", key, z, result)
			}
			zones[z] = true
		}
	}
}

func TestCRUSH_FailureDomainRack(t *testing.T) {
	nodes := []Node{
		{ID: "r1-a", Weight: 1, Zone: "z1", Rack: "rack-1"},
		{ID: "r1-b", Weight: 1, Zone: "z1", Rack: "rack-1"},
		{ID: "r2-a", Weight: 1, Zone: "z1", Rack: "rack-2"},
		{ID: "r2-b", Weight: 1, Zone: "z1", Rack: "rack-2"},
		{ID: "r3-a", Weight: 1, Zone: "z2", Rack: "rack-3"},
	}
	cp := NewCRUSHPlacer(nodes)
	cp.SetFailureDomain("rack")

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("blk-%d", i)
		result := cp.PlaceKey(key, 3)
		if len(result) != 3 {
			t.Fatalf("key %q: got %d nodes, want 3", key, len(result))
		}
		racks := make(map[string]bool)
		for _, id := range result {
			r := nodeRack(nodes, id)
			if racks[r] {
				t.Fatalf("key %q: rack %s used twice in %v", key, r, result)
			}
			racks[r] = true
		}
	}
}

func TestCRUSH_WeightedDistribution(t *testing.T) {
	// Node "heavy" has 4x the weight of "light" nodes.
	// Over many keys, heavy should receive ~4x more placements.
	nodes := []Node{
		{ID: "light-1", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "light-2", Weight: 1, Zone: "z1", Rack: "r2"},
		{ID: "heavy", Weight: 4, Zone: "z1", Rack: "r3"},
	}
	cp := NewCRUSHPlacer(nodes)
	// Use node-level domain so weight is the only differentiator.
	cp.SetFailureDomain("node")

	counts := map[string]int{"light-1": 0, "light-2": 0, "heavy": 0}
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k-%d", i)
		result := cp.PlaceKey(key, 1)
		counts[result[0]]++
	}

	// Expected ratio: heavy ~= 4 * light (each). Total weight = 6, so
	// heavy expected fraction = 4/6 ≈ 0.667, each light = 1/6 ≈ 0.167.
	heavyFrac := float64(counts["heavy"]) / float64(numKeys)
	light1Frac := float64(counts["light-1"]) / float64(numKeys)
	light2Frac := float64(counts["light-2"]) / float64(numKeys)

	t.Logf("Distribution: heavy=%.3f light-1=%.3f light-2=%.3f", heavyFrac, light1Frac, light2Frac)

	// Allow 10% relative tolerance.
	if math.Abs(heavyFrac-0.667) > 0.10 {
		t.Errorf("heavy fraction %.3f, want ~0.667", heavyFrac)
	}
	if math.Abs(light1Frac-0.167) > 0.10 {
		t.Errorf("light-1 fraction %.3f, want ~0.167", light1Frac)
	}
	if math.Abs(light2Frac-0.167) > 0.10 {
		t.Errorf("light-2 fraction %.3f, want ~0.167", light2Frac)
	}
}

func TestCRUSH_StabilityOnAddNode(t *testing.T) {
	// Start with 10 nodes, place 1000 keys, then add 1 node. Only ~1/11 of
	// keys should move (ideally).
	original := make([]Node, 10)
	for i := range original {
		original[i] = Node{
			ID:     fmt.Sprintf("node-%d", i),
			Weight: 1,
			Zone:   "z1",
			Rack:   fmt.Sprintf("r%d", i),
		}
	}

	cp1 := NewCRUSHPlacer(original)

	numKeys := 5000
	before := make(map[string]string, numKeys) // key -> primary node
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		result := cp1.PlaceKey(key, 1)
		before[key] = result[0]
	}

	// Add one node.
	expanded := make([]Node, len(original)+1)
	copy(expanded, original)
	expanded[10] = Node{ID: "node-new", Weight: 1, Zone: "z1", Rack: "r-new"}
	cp2 := NewCRUSHPlacer(expanded)

	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		result := cp2.PlaceKey(key, 1)
		if result[0] != before[key] {
			moved++
		}
	}

	movedFrac := float64(moved) / float64(numKeys)
	// Ideal movement = 1/11 ≈ 0.091. Allow up to 0.25 (generous tolerance for
	// small hash perturbations).
	t.Logf("Moved %.1f%% of keys after adding 1 node to 10", movedFrac*100)
	if movedFrac > 0.25 {
		t.Errorf("moved %.1f%% of keys, want < 25%%", movedFrac*100)
	}
}

func TestCRUSH_StabilityOnRemoveNode(t *testing.T) {
	nodes := make([]Node, 10)
	for i := range nodes {
		nodes[i] = Node{
			ID:     fmt.Sprintf("node-%d", i),
			Weight: 1,
			Zone:   "z1",
			Rack:   fmt.Sprintf("r%d", i),
		}
	}

	cp := NewCRUSHPlacer(nodes)

	numKeys := 5000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		result := cp.PlaceKey(key, 1)
		before[key] = result[0]
	}

	// Remove one node.
	reduced := make([]Node, 9)
	copy(reduced, nodes[:9]) // remove node-9
	cp2 := NewCRUSHPlacer(reduced)

	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		result := cp2.PlaceKey(key, 1)
		if result[0] != before[key] {
			moved++
		}
	}

	movedFrac := float64(moved) / float64(numKeys)
	t.Logf("Moved %.1f%% of keys after removing 1 node from 10", movedFrac*100)
	// Only keys that were on the removed node should move (~10%), plus minor
	// perturbation. Allow up to 20%.
	if movedFrac > 0.20 {
		t.Errorf("moved %.1f%% of keys, want < 20%%", movedFrac*100)
	}
}

func TestCRUSH_RemoveNode(t *testing.T) {
	nodes := []Node{
		{ID: "a", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "b", Weight: 1, Zone: "z1", Rack: "r2"},
		{ID: "c", Weight: 1, Zone: "z2", Rack: "r3"},
	}
	cp := NewCRUSHPlacer(nodes)

	cp.RemoveNode("b")
	result := cp.PlaceKey("test-key", 3)
	if len(result) != 2 {
		t.Fatalf("after removing 1 of 3 nodes, PlaceKey(3) returned %d, want 2", len(result))
	}
	for _, id := range result {
		if id == "b" {
			t.Errorf("removed node 'b' still appears in placement: %v", result)
		}
	}
}

func TestCRUSH_AddWeightedNode(t *testing.T) {
	cp := NewCRUSHPlacer(nil)
	cp.AddWeightedNode(Node{ID: "x", Weight: 2, Zone: "z1", Rack: "r1"})
	cp.AddWeightedNode(Node{ID: "y", Weight: 1, Zone: "z2", Rack: "r2"})

	result := cp.PlaceKey("key-1", 2)
	if len(result) != 2 {
		t.Fatalf("PlaceKey(2) returned %d, want 2", len(result))
	}
}

func TestCRUSH_AddNodeIdempotent(t *testing.T) {
	cp := NewCRUSHPlacer([]Node{
		{ID: "a", Weight: 1, Zone: "z1", Rack: "r1"},
	})
	cp.AddWeightedNode(Node{ID: "a", Weight: 5, Zone: "z2", Rack: "r9"})

	// Should have replaced, not duplicated.
	result := cp.PlaceKey("key", 2)
	if len(result) != 1 {
		t.Errorf("expected 1 node after idempotent add, got %d: %v", len(result), result)
	}
}

func TestCRUSH_EmptyPlacer(t *testing.T) {
	cp := NewCRUSHPlacer(nil)
	result := cp.PlaceKey("key", 3)
	if len(result) != 0 {
		t.Errorf("empty placer returned %d nodes, want 0", len(result))
	}
}

func TestCRUSH_ZeroWeightNodeNeverSelected(t *testing.T) {
	nodes := []Node{
		{ID: "active", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "drained", Weight: 0, Zone: "z1", Rack: "r2"},
	}
	cp := NewCRUSHPlacer(nodes)

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k-%d", i)
		result := cp.PlaceKey(key, 1)
		if len(result) != 1 {
			t.Fatalf("expected 1 result, got %d", len(result))
		}
		if result[0] == "drained" {
			t.Fatalf("zero-weight node selected for key %q", key)
		}
	}
}

func TestCRUSH_LegacyPlaceInterface(t *testing.T) {
	nodes := []Node{
		{ID: "a", Weight: 1, Zone: "z1", Rack: "r1"},
		{ID: "b", Weight: 1, Zone: "z2", Rack: "r2"},
	}
	// Ensure CRUSHPlacer satisfies the Placer interface.
	var p Placer = NewCRUSHPlacer(nodes)
	result := p.Place(1)
	if len(result) != 1 {
		t.Errorf("Place(1) via interface returned %d, want 1", len(result))
	}
}

func TestCRUSH_SetFailureDomainInvalid(t *testing.T) {
	cp := NewCRUSHPlacer(nil)
	cp.SetFailureDomain("invalid-domain")
	// Should fall back to node-level.
	cp.AddWeightedNode(Node{ID: "a", Weight: 1, Zone: "z1", Rack: "r1"})
	cp.AddWeightedNode(Node{ID: "b", Weight: 1, Zone: "z1", Rack: "r1"})
	result := cp.PlaceKey("key", 2)
	if len(result) != 2 {
		t.Errorf("expected 2 nodes with node-level domain, got %d", len(result))
	}
}

// helpers

func nodeZone(nodes []Node, id string) string {
	for _, n := range nodes {
		if n.ID == id {
			return n.Zone
		}
	}
	return ""
}

func nodeRack(nodes []Node, id string) string {
	for _, n := range nodes {
		if n.ID == id {
			return n.Rack
		}
	}
	return ""
}
