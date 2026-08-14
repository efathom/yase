package routing

import (
	"fmt"
	"math"
	"testing"
)

func TestHashRingGetNode(t *testing.T) {
	hr := NewHashRing(256)
	hr.AddNode("node-a")
	hr.AddNode("node-b")
	hr.AddNode("node-c")

	// Same key always maps to same node
	n1 := hr.GetNode("my-document-123")
	n2 := hr.GetNode("my-document-123")
	if n1 != n2 {
		t.Errorf("same key returned different nodes: %q vs %q", n1, n2)
	}

	// Node should be one of the added nodes
	valid := map[string]bool{"node-a": true, "node-b": true, "node-c": true}
	if !valid[n1] {
		t.Errorf("unexpected node: %q", n1)
	}
}

func TestHashRingEmptyRing(t *testing.T) {
	hr := NewHashRing(256)
	if n := hr.GetNode("key"); n != "" {
		t.Errorf("empty ring: got %q, want empty", n)
	}
	if nodes := hr.GetNodes("key", 3); len(nodes) != 0 {
		t.Errorf("empty ring: got %v, want nil", nodes)
	}
}

func TestHashRingAddRemove(t *testing.T) {
	hr := NewHashRing(256)
	hr.AddNode("a")
	hr.AddNode("b")

	if hr.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes, got %d", hr.NodeCount())
	}

	hr.RemoveNode("a")
	if hr.NodeCount() != 1 {
		t.Fatalf("expected 1 node after remove, got %d", hr.NodeCount())
	}

	// All keys should now map to "b"
	for i := 0; i < 100; i++ {
		n := hr.GetNode(fmt.Sprintf("key-%d", i))
		if n != "b" {
			t.Fatalf("key-%d: got %q, want b", i, n)
		}
	}
}

func TestHashRingDuplicateAdd(t *testing.T) {
	hr := NewHashRing(256)
	hr.AddNode("a")
	hr.AddNode("a") // duplicate
	if hr.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", hr.NodeCount())
	}
}

func TestHashRingGetNodes(t *testing.T) {
	hr := NewHashRing(256)
	hr.AddNode("a")
	hr.AddNode("b")
	hr.AddNode("c")

	nodes := hr.GetNodes("key", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	// Should be distinct
	if nodes[0] == nodes[1] {
		t.Error("nodes should be distinct")
	}

	// Requesting more nodes than available
	nodes = hr.GetNodes("key", 5)
	if len(nodes) != 3 {
		t.Errorf("expected 3 (all nodes), got %d", len(nodes))
	}
}

func TestHashRingBalance(t *testing.T) {
	hr := NewHashRing(256)
	numNodes := 10
	for i := 0; i < numNodes; i++ {
		hr.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Distribute 10,000 keys and count per-node assignment
	counts := make(map[string]int)
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		n := hr.GetNode(fmt.Sprintf("doc-%d", i))
		counts[n]++
	}

	// With 256 vnodes per node and 10 nodes, expect ~1000 per node
	expected := float64(numKeys) / float64(numNodes)
	var minCount, maxCount int
	minCount = numKeys
	for _, c := range counts {
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}

	ratio := float64(maxCount) / float64(minCount)
	t.Logf("Balance: min=%d, max=%d, expected=%.0f, ratio=%.3f", minCount, maxCount, expected, ratio)

	// max/min ratio should be < 3.0 with 256 vnodes and FNV-1a
	if ratio > 3.0 {
		t.Errorf("balance ratio %.3f exceeds 3.0", ratio)
	}
}

func TestHashRingMinimalDisruption(t *testing.T) {
	hr := NewHashRing(256)
	numNodes := 10
	for i := 0; i < numNodes; i++ {
		hr.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Record initial assignments for 10,000 keys
	numKeys := 10000
	before := make(map[string]string) // key → node
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("doc-%d", i)
		before[key] = hr.GetNode(key)
	}

	// Remove one node
	hr.RemoveNode("node-5")

	// Count reassigned keys
	reassigned := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("doc-%d", i)
		after := hr.GetNode(key)
		if after != before[key] {
			reassigned++
		}
	}

	pct := float64(reassigned) / float64(numKeys) * 100
	t.Logf("Removing 1 of %d nodes: %.1f%% keys reassigned (%d/%d)", numNodes, pct, reassigned, numKeys)

	// Expected: ~10% (1/N). Allow up to 15%.
	if pct > 15.0 {
		t.Errorf("too many reassignments: %.1f%% > 15%%", pct)
	}
}

func TestHashRingAddNodeMinimalDisruption(t *testing.T) {
	hr := NewHashRing(256)
	for i := 0; i < 10; i++ {
		hr.AddNode(fmt.Sprintf("node-%d", i))
	}

	numKeys := 10000
	before := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("doc-%d", i)
		before[key] = hr.GetNode(key)
	}

	// Add one node
	hr.AddNode("node-new")

	reassigned := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("doc-%d", i)
		if hr.GetNode(key) != before[key] {
			reassigned++
		}
	}

	pct := float64(reassigned) / float64(numKeys) * 100
	t.Logf("Adding 1 node to %d: %.1f%% keys reassigned", 10, pct)

	// Expected: ~1/(N+1) ≈ 9%. Allow up to 15%.
	if pct > 15.0 {
		t.Errorf("too many reassignments: %.1f%% > 15%%", pct)
	}
}

func TestHashRingStandardDeviation(t *testing.T) {
	hr := NewHashRing(256)
	numNodes := 10
	for i := 0; i < numNodes; i++ {
		hr.AddNode(fmt.Sprintf("node-%d", i))
	}

	counts := make(map[string]float64)
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		counts[hr.GetNode(fmt.Sprintf("k-%d", i))]++
	}

	mean := float64(numKeys) / float64(numNodes)
	var variance float64
	for _, c := range counts {
		d := c - mean
		variance += d * d
	}
	variance /= float64(numNodes)
	stddev := math.Sqrt(variance)
	cv := stddev / mean // coefficient of variation

	t.Logf("StdDev=%.1f, Mean=%.1f, CV=%.4f", stddev, mean, cv)

	// CV should be < 0.25 with 256 vnodes and FNV-1a
	if cv > 0.25 {
		t.Errorf("coefficient of variation %.4f exceeds 0.25", cv)
	}
}
