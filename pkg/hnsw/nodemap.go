package hnsw

import (
	"sync"
	"sync/atomic"
)

const nodeShards = 64

// nodeMap is a sharded ID → *Node map. Distance computations look a node up
// on every visit, so a single RWMutex over the whole map becomes a cache-line
// hotspot under concurrent queries; sharding spreads that contention.
type nodeMap struct {
	shards [nodeShards]struct {
		mu sync.RWMutex
		m  map[uint32]*Node
	}
	count atomic.Int64
}

func newNodeMap() *nodeMap {
	nm := &nodeMap{}
	for i := range nm.shards {
		nm.shards[i].m = make(map[uint32]*Node)
	}
	return nm
}

func (nm *nodeMap) get(id uint32) *Node {
	s := &nm.shards[id%nodeShards]
	s.mu.RLock()
	n := s.m[id]
	s.mu.RUnlock()
	return n
}

// set stores n and reports whether it replaced an existing node.
func (nm *nodeMap) set(id uint32, n *Node) bool {
	s := &nm.shards[id%nodeShards]
	s.mu.Lock()
	_, existed := s.m[id]
	s.m[id] = n
	s.mu.Unlock()
	if !existed {
		nm.count.Add(1)
	}
	return existed
}

func (nm *nodeMap) len() int { return int(nm.count.Load()) }

// ids returns a snapshot of all node IDs.
func (nm *nodeMap) ids() []uint32 {
	out := make([]uint32, 0, nm.len())
	for i := range nm.shards {
		s := &nm.shards[i]
		s.mu.RLock()
		for id := range s.m {
			out = append(out, id)
		}
		s.mu.RUnlock()
	}
	return out
}

// forEach calls fn for every node (snapshot per shard).
func (nm *nodeMap) forEach(fn func(id uint32, n *Node)) {
	for _, id := range nm.ids() {
		if n := nm.get(id); n != nil {
			fn(id, n)
		}
	}
}

// visitedPool recycles visited sets across SearchLayer calls to avoid a map
// allocation per layer per query.
var visitedPool = sync.Pool{New: func() any { return make(map[uint32]struct{}, 256) }}

func getVisited() map[uint32]struct{} { return visitedPool.Get().(map[uint32]struct{}) }

func putVisited(m map[uint32]struct{}) {
	if len(m) > 1<<16 {
		return // don't pin very large maps in the pool
	}
	clear(m)
	visitedPool.Put(m)
}
