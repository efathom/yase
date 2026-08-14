package hnsw

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/efathom/yase/pkg/memory"
	"github.com/efathom/yase/pkg/vector"
)

// uint64ToBytes converts []uint64 to []byte in little-endian format.
func uint64ToBytes(vals []uint64) []byte {
	b := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:(i+1)*8], v)
	}
	return b
}

// Config holds HNSW hyperparameters.
type Config struct {
	M              int             // max edges per node per layer (default 16)
	Mmax0          int             // max edges at layer 0 (default 2*M = 32)
	EfConstruction int             // search width during construction (default 200)
	EfSearch       int             // search width during queries (default 50)
	VecDim         int             // vector dimensionality
	DistFunc       vector.DistFunc // distance function (default CosineDistanceFast)
	BBQEnabled     bool            // store binary-quantized vectors for fast Hamming traversal
}

// DefaultConfig returns HNSW parameters from the paper defaults.
func DefaultConfig(vecDim int) Config {
	return Config{
		M:              16,
		Mmax0:          32,
		EfConstruction: 200,
		EfSearch:       50,
		VecDim:         vecDim,
		DistFunc:       vector.DistCosine,
	}
}

// Graph is the main HNSW index structure. The nodes map is protected by a
// RWMutex, but individual edge lists use lock-free CAS for concurrent updates.
type Graph struct {
	mu         sync.RWMutex
	nodes      map[uint32]*Node
	entryPoint atomic.Pointer[Node]
	maxLevel   int
	arena      memory.Arena
	cfg        Config
	ml         float64 // level normalization: 1/ln(M)
	rng        *rand.Rand
	rngMu      sync.Mutex
}

// NewGraph creates an empty HNSW graph backed by the given off-heap arena.
func NewGraph(arena memory.Arena, cfg Config) *Graph {
	if cfg.DistFunc == nil {
		cfg.DistFunc = vector.DistCosine
	}
	return &Graph{
		nodes: make(map[uint32]*Node),
		arena: arena,
		cfg:   cfg,
		ml:    1.0 / math.Log(float64(cfg.M)),
		rng:   rand.New(rand.NewSource(42)),
	}
}

// randomLevel generates a random level from the exponential distribution:
// floor(-ln(uniform(0,1)) * ml)
func (g *Graph) randomLevel() int {
	g.rngMu.Lock()
	r := g.rng.Float64()
	g.rngMu.Unlock()
	if r == 0 {
		r = 1e-10
	}
	return int(math.Floor(-math.Log(r) * g.ml))
}

// getNode safely retrieves a node by ID.
func (g *Graph) getNode(id uint32) *Node {
	g.mu.RLock()
	n := g.nodes[id]
	g.mu.RUnlock()
	return n
}

// distBetween computes distance between two nodes using arena vectors.
func (g *Graph) distBetween(a, b uint32) float32 {
	na := g.getNode(a)
	nb := g.getNode(b)
	if na == nil || nb == nil {
		return math.MaxFloat32
	}
	va := g.arena.GetFloat32(na.VectorOffset, g.cfg.VecDim)
	vb := g.arena.GetFloat32(nb.VectorOffset, g.cfg.VecDim)
	return g.cfg.DistFunc(va, vb)
}

// distToQuery returns a closure that computes distance from a query vector to a node.
func (g *Graph) distToQuery(queryVec []float32) func(id uint32) float32 {
	dist := g.cfg.DistFunc
	return func(id uint32) float32 {
		n := g.getNode(id)
		if n == nil {
			return math.MaxFloat32
		}
		nv := g.arena.GetFloat32(n.VectorOffset, g.cfg.VecDim)
		return dist(queryVec, nv)
	}
}

// getEdges returns the edge list accessor for SearchLayer.
func (g *Graph) getEdges(id uint32, layer int) []uint32 {
	n := g.getNode(id)
	if n == nil {
		return nil
	}
	return n.GetEdges(layer)
}

// Len returns the number of nodes in the graph.
func (g *Graph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes)
}

// EfSearch returns the configured search width (ef) for queries.
func (g *Graph) EfSearch() int {
	return g.cfg.EfSearch
}

// MaxLevel returns the current maximum layer in the graph.
func (g *Graph) MaxLevel() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.maxLevel
}

// Insert adds a vector to the HNSW graph following Algorithm 1 from the paper.
// It allocates the vector in the off-heap arena, creates a node, then performs
// greedy descent + bidirectional linking at each layer.
func (g *Graph) Insert(id uint32, vec []float32) error {
	if len(vec) != g.cfg.VecDim {
		return fmt.Errorf("vector dimension mismatch: got %d, want %d", len(vec), g.cfg.VecDim)
	}

	// Allocate vector in arena
	offset, err := g.arena.AllocFloat32(vec)
	if err != nil {
		return err
	}

	return g.insertAt(id, vec, offset)
}

// InsertAt inserts a vector whose bytes are already allocated at offset in the
// arena (via AllocFloat32). The caller owns the allocation and must keep vec
// and offset consistent. This avoids a second arena allocation for centroids.
func (g *Graph) InsertAt(id uint32, vec []float32, offset uint64) error {
	if len(vec) != g.cfg.VecDim {
		return fmt.Errorf("vector dimension mismatch: got %d, want %d", len(vec), g.cfg.VecDim)
	}
	return g.insertAt(id, vec, offset)
}

func (g *Graph) insertAt(id uint32, vec []float32, offset uint64) error {
	level := g.randomLevel()
	node := NewNode(id, level, offset)

	// BBQ: store binary-quantized vector alongside float32
	if g.cfg.BBQEnabled {
		binVec := vector.BinaryQuantize(vec)
		binBytes := uint64ToBytes(binVec)
		binOffset, err := g.arena.AllocBytes(binBytes)
		if err != nil {
			return fmt.Errorf("alloc binary vector: %w", err)
		}
		node.BinaryVectorOffset = binOffset
		node.BinaryVectorLen = len(binBytes)
	}

	// Add to nodes map
	g.mu.Lock()
	g.nodes[id] = node
	ep := g.entryPoint.Load()

	// First node: set as entry point and return
	if ep == nil {
		g.entryPoint.Store(node)
		g.maxLevel = level
		g.mu.Unlock()
		return nil
	}
	currentMaxLevel := g.maxLevel
	g.mu.Unlock()

	// Re-read entry point atomically — another insert may have changed it
	// between our unlock and here.
	ep = g.entryPoint.Load()

	distFunc := g.distToQuery(vec)

	// Phase 1: Greedy descent from top layers down to node's level + 1
	currentEP := ep.ID
	for l := currentMaxLevel; l > level; l-- {
		results := SearchLayer(currentEP, 1, l, g.getEdges, distFunc)
		if len(results) > 0 {
			currentEP = results[0].ID
		}
	}

	// Phase 2: Insert with neighbors at layers min(level, maxLevel)..0
	topLayer := level
	if topLayer > currentMaxLevel {
		topLayer = currentMaxLevel
	}
	for l := topLayer; l >= 0; l-- {
		results := SearchLayer(currentEP, g.cfg.EfConstruction, l, g.getEdges, distFunc)

		maxEdges := g.cfg.M
		if l == 0 {
			maxEdges = g.cfg.Mmax0
		}

		// Select best neighbors using diversity heuristic
		selected := SelectNeighborsHeuristic(id, results, maxEdges, g.distBetween)

		pruner := func(nodeID uint32, candidateIDs []uint32, max int) []uint32 {
			candidates := make([]Candidate, len(candidateIDs))
			nNode := g.getNode(nodeID)
			if nNode == nil {
				return candidateIDs[:max]
			}
			nVec := g.arena.GetFloat32(nNode.VectorOffset, g.cfg.VecDim)
			for i, cid := range candidateIDs {
				cn := g.getNode(cid)
				if cn == nil {
					candidates[i] = Candidate{ID: cid, Distance: math.MaxFloat32}
					continue
				}
				cv := g.arena.GetFloat32(cn.VectorOffset, g.cfg.VecDim)
				candidates[i] = Candidate{ID: cid, Distance: g.cfg.DistFunc(nVec, cv)}
			}
			pruned := SelectNeighborsHeuristic(nodeID, candidates, max, g.distBetween)
			result := make([]uint32, len(pruned))
			for i, p := range pruned {
				result[i] = p.ID
			}
			return result
		}

		// Build the node's own outgoing edges via CAS to avoid lost updates
		// from concurrent backlink insertions targeting this node.
		for _, s := range selected {
			node.addEdgeCAS(l, s.ID, maxEdges, pruner)
		}

		// Bidirectional linking: add edge from each neighbor back to new node
		for _, s := range selected {
			neighbor := g.getNode(s.ID)
			if neighbor != nil && l <= neighbor.Level {
				neighbor.addEdgeCAS(l, id, maxEdges, pruner)
			}
		}

		// Use closest result as entry point for next layer down
		if len(results) > 0 {
			currentEP = results[0].ID
		}
	}

	// Update entry point if new node has higher level
	if level > currentMaxLevel {
		g.mu.Lock()
		if level > g.maxLevel {
			g.maxLevel = level
			g.entryPoint.Store(node)
		}
		g.mu.Unlock()
	}

	return nil
}

// Search finds the k nearest neighbors of the query vector using the HNSW
// search procedure: greedy descent through upper layers, then ef-bounded
// search at layer 0.
func (g *Graph) Search(queryVec []float32, k int, ef int) []Candidate {
	ep := g.entryPoint.Load()
	if ep == nil {
		return nil
	}

	distFunc := g.distToQuery(queryVec)

	// Greedy descent through upper layers
	currentEP := ep.ID
	g.mu.RLock()
	maxLvl := g.maxLevel
	g.mu.RUnlock()

	for l := maxLvl; l > 0; l-- {
		results := SearchLayer(currentEP, 1, l, g.getEdges, distFunc)
		if len(results) > 0 {
			currentEP = results[0].ID
		}
	}

	// Bounded search at layer 0
	results := SearchLayer(currentEP, ef, 0, g.getEdges, distFunc)

	if len(results) > k {
		results = results[:k]
	}
	return results
}
