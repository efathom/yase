package hnsw

import (
	"sync/atomic"
)

// Node represents an HNSW vertex. VectorOffset points into the OffHeapArena
// (byte offset), keeping the Node itself free of GC-traced vector data.
// Each layer has its own atomic edge list for lock-free concurrent updates.
type Node struct {
	ID                 uint32
	Level              int
	VectorOffset       uint64
	BinaryVectorOffset uint64 // offset to binary-quantized vector in arena (0 if BBQ disabled)
	BinaryVectorLen    int    // number of bytes for the binary vector
	Edges              []atomic.Pointer[[]uint32] // per-layer neighbor lists
}

// NewNode creates a node with empty edge lists for layers [0..level].
func NewNode(id uint32, level int, vectorOffset uint64) *Node {
	n := &Node{
		ID:           id,
		Level:        level,
		VectorOffset: vectorOffset,
		Edges:        make([]atomic.Pointer[[]uint32], level+1),
	}
	for i := 0; i <= level; i++ {
		empty := make([]uint32, 0)
		n.Edges[i].Store(&empty)
	}
	return n
}

// GetEdges returns the current neighbor list for the given layer (wait-free read).
func (n *Node) GetEdges(layer int) []uint32 {
	if layer > n.Level {
		return nil
	}
	ptr := n.Edges[layer].Load()
	if ptr == nil {
		return nil
	}
	return *ptr
}

// addEdgeCAS performs a lock-free append of targetID to the node's edge list
// at the given layer. If the list exceeds maxEdges, it is pruned using the
// provided pruner function. Returns true if the edge was added (not a duplicate).
func (n *Node) addEdgeCAS(layer int, targetID uint32, maxEdges int, pruner func(nodeID uint32, candidates []uint32, max int) []uint32) bool {
	for {
		oldPtr := n.Edges[layer].Load()
		oldEdges := *oldPtr

		// Check for duplicate
		for _, id := range oldEdges {
			if id == targetID {
				return false
			}
		}

		// Copy-on-write append
		newEdges := make([]uint32, len(oldEdges)+1)
		copy(newEdges, oldEdges)
		newEdges[len(oldEdges)] = targetID

		// Prune if over capacity
		if len(newEdges) > maxEdges && pruner != nil {
			newEdges = pruner(n.ID, newEdges, maxEdges)
		}

		if n.Edges[layer].CompareAndSwap(oldPtr, &newEdges) {
			return true
		}
		// CAS failed — another goroutine modified edges; retry
	}
}
