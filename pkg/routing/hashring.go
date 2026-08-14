package routing

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// HashRing implements consistent hashing with virtual nodes for
// shard-to-node placement. Uses FNV-1a for deterministic hashing.
type HashRing struct {
	mu           sync.RWMutex
	ring         []ringEntry          // sorted by hash
	vnodeCount   int                  // virtual nodes per physical node
	nodeToVnodes map[string][]uint32  // nodeID → vnode hashes (for removal)
}

type ringEntry struct {
	Hash   uint32
	NodeID string
}

// NewHashRing creates an empty consistent hash ring.
// vnodeCount controls load balance — higher means more even distribution
// at the cost of more memory. 256 is a good default.
func NewHashRing(vnodeCount int) *HashRing {
	if vnodeCount <= 0 {
		vnodeCount = 256
	}
	return &HashRing{
		vnodeCount:   vnodeCount,
		nodeToVnodes: make(map[string][]uint32),
	}
}

// AddNode projects vnodeCount virtual positions onto the ring.
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodeToVnodes[nodeID]; exists {
		return // already added
	}

	hashes := make([]uint32, hr.vnodeCount)
	for i := 0; i < hr.vnodeCount; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", nodeID, i))
		hr.ring = append(hr.ring, ringEntry{Hash: h, NodeID: nodeID})
		hashes[i] = h
	}
	hr.nodeToVnodes[nodeID] = hashes

	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i].Hash < hr.ring[j].Hash
	})
}

// RemoveNode removes all virtual nodes for the given physical node.
// Only adjacent keys are reassigned — O(vnodes) not O(total keys).
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodeToVnodes[nodeID]; !exists {
		return
	}

	filtered := make([]ringEntry, 0, len(hr.ring)-hr.vnodeCount)
	for _, e := range hr.ring {
		if e.NodeID != nodeID {
			filtered = append(filtered, e)
		}
	}
	hr.ring = filtered
	delete(hr.nodeToVnodes, nodeID)
}

// GetNode returns the node responsible for the given key.
// Walks clockwise from hash(key) to the first ring entry.
// Returns empty string if the ring is empty.
func (hr *HashRing) GetNode(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return ""
	}

	h := hashKey(key)
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i].Hash >= h
	})
	if idx >= len(hr.ring) {
		idx = 0 // wrap around
	}
	return hr.ring[idx].NodeID
}

// GetNodes returns up to n distinct physical nodes for replication.
// Walks clockwise from hash(key), skipping duplicate physical nodes.
func (hr *HashRing) GetNodes(key string, n int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i].Hash >= h
	})
	if idx >= len(hr.ring) {
		idx = 0
	}

	seen := make(map[string]bool)
	var result []string
	ringLen := len(hr.ring)

	for i := 0; i < ringLen && len(result) < n; i++ {
		entry := hr.ring[(idx+i)%ringLen]
		if !seen[entry.NodeID] {
			seen[entry.NodeID] = true
			result = append(result, entry.NodeID)
		}
	}
	return result
}

// NodeCount returns the number of physical nodes on the ring.
func (hr *HashRing) NodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodeToVnodes)
}

// Nodes returns all physical node IDs on the ring.
func (hr *HashRing) Nodes() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	nodes := make([]string, 0, len(hr.nodeToVnodes))
	for id := range hr.nodeToVnodes {
		nodes = append(nodes, id)
	}
	return nodes
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
