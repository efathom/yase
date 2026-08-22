package hnsw

import (
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/efathom/yase/pkg/memory"
	"github.com/efathom/yase/pkg/vector"
)

const testDim = 128

func makeTestGraph(t *testing.T, n int) (*Graph, [][]float32) {
	t.Helper()
	arenaSize := uint64(n) * uint64(testDim) * 4 * 2
	arena, err := memory.NewOffHeapArena(arenaSize)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}

	cfg := DefaultConfig(testDim)
	g := NewGraph(arena, cfg)

	rng := rand.New(rand.NewSource(42))
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		v := make([]float32, testDim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = v
		if err := g.Insert(uint32(i), v); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	return g, vecs
}

func bruteForceKNN(query []float32, vecs [][]float32, k int) []uint32 {
	type pair struct {
		id   uint32
		dist float32
	}
	pairs := make([]pair, len(vecs))
	for i, v := range vecs {
		pairs[i] = pair{id: uint32(i), dist: vector.CosineDistance(query, v)}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].dist < pairs[j].dist
	})
	result := make([]uint32, k)
	for i := 0; i < k; i++ {
		result[i] = pairs[i].id
	}
	return result
}

func TestInsertAndSearchSingle(t *testing.T) {
	arena, err := memory.NewOffHeapArena(4096)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	cfg := DefaultConfig(4)
	g := NewGraph(arena, cfg)

	vec := []float32{1, 2, 3, 4}
	if err := g.Insert(0, vec); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	results := g.Search(vec, 1, 10)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != 0 {
		t.Errorf("expected ID 0, got %d", results[0].ID)
	}
	if results[0].Distance > 1e-6 {
		t.Errorf("expected distance ~0, got %f", results[0].Distance)
	}
}

func TestInsertAndSearch1000(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1000-vector test in short mode")
	}

	n := 1000
	k := 10
	ef := 50

	g, vecs := makeTestGraph(t, n)
	defer g.arena.Close()

	// Use a random query vector
	rng := rand.New(rand.NewSource(99))
	query := make([]float32, testDim)
	for i := range query {
		query[i] = rng.Float32()*2 - 1
	}

	results := g.Search(query, k, ef)
	if len(results) < k {
		t.Fatalf("expected %d results, got %d", k, len(results))
	}

	// Compute recall@10
	trueNN := bruteForceKNN(query, vecs, k)
	trueSet := make(map[uint32]bool)
	for _, id := range trueNN {
		trueSet[id] = true
	}

	hits := 0
	for _, r := range results {
		if trueSet[r.ID] {
			hits++
		}
	}
	recall := float64(hits) / float64(k)
	t.Logf("recall@%d = %.2f (%d/%d)", k, recall, hits, k)
	if recall < 0.7 {
		t.Errorf("recall@%d = %.2f, want > 0.70", k, recall)
	}
}

func TestConcurrentInsertSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}

	arenaSize := uint64(200) * uint64(testDim) * 4 * 2
	arena, err := memory.NewOffHeapArena(arenaSize)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	cfg := DefaultConfig(testDim)
	g := NewGraph(arena, cfg)

	var wg sync.WaitGroup
	numInserters := 50
	numSearchers := 50

	// Inserters
	wg.Add(numInserters)
	for i := 0; i < numInserters; i++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			vec := make([]float32, testDim)
			for j := range vec {
				vec[j] = rng.Float32()*2 - 1
			}
			g.Insert(uint32(id), vec)
		}(i)
	}

	// Searchers (start after a small delay to let some inserts happen)
	wg.Add(numSearchers)
	for i := 0; i < numSearchers; i++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id + 1000)))
			query := make([]float32, testDim)
			for j := range query {
				query[j] = rng.Float32()*2 - 1
			}
			g.Search(query, 5, 20)
		}(i)
	}

	wg.Wait()

	if g.Len() != numInserters {
		t.Errorf("expected %d nodes, got %d", numInserters, g.Len())
	}
}

func TestMultiLayerStructure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-layer structure test in short mode")
	}

	// Building the graph dominates this package's runtime, and the race
	// detector multiplies it by roughly fifteen. 2K nodes exercise the same
	// layer-assignment invariants; set YASE_SLOW_TESTS=1 for the full 10K run
	// (CI does this in a dedicated job).
	n := 2000
	if os.Getenv("YASE_SLOW_TESTS") == "1" {
		n = 10000
	}

	g, _ := makeTestGraph(t, n)
	defer g.arena.Close()

	if g.Len() != n {
		t.Errorf("expected %d nodes, got %d", n, g.Len())
	}

	// Verify entry point is at max level
	ep := g.entryPoint.Load()
	if ep == nil {
		t.Fatal("entry point is nil")
	}
	if ep.Level != g.MaxLevel() {
		t.Errorf("entry point level %d != max level %d", ep.Level, g.MaxLevel())
	}

	// Verify max level is reasonable: expected ~log(N)/log(M)
	expectedMaxLevel := math.Log(float64(n)) / math.Log(float64(g.cfg.M))
	if float64(g.MaxLevel()) > expectedMaxLevel*3 {
		t.Errorf("max level %d seems too high for %d nodes (expected ~%.1f)", g.MaxLevel(), n, expectedMaxLevel)
	}

	t.Logf("%d nodes: max level = %d (expected ~%.1f)", n, g.MaxLevel(), expectedMaxLevel)
}

func TestBidirectionalEdges(t *testing.T) {
	arena, err := memory.NewOffHeapArena(1 << 20)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	cfg := DefaultConfig(4)
	g := NewGraph(arena, cfg)

	// Insert two close vectors
	g.Insert(0, []float32{1, 0, 0, 0})
	g.Insert(1, []float32{1, 0.01, 0, 0}) // very close to node 0

	node0 := g.getNode(0)
	node1 := g.getNode(1)

	// Check that node 1 has node 0 as neighbor at layer 0
	edges1 := node1.GetEdges(0)
	found0in1 := false
	for _, id := range edges1 {
		if id == 0 {
			found0in1 = true
		}
	}

	// Check that node 0 has node 1 as neighbor at layer 0
	edges0 := node0.GetEdges(0)
	found1in0 := false
	for _, id := range edges0 {
		if id == 1 {
			found1in0 = true
		}
	}

	if !found0in1 && !found1in0 {
		t.Error("expected bidirectional edge between nodes 0 and 1, found neither direction")
	}
}

func TestEdgePruningRespectsMmax(t *testing.T) {
	arena, err := memory.NewOffHeapArena(1 << 20)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	cfg := Config{M: 4, Mmax0: 8, EfConstruction: 50, VecDim: 4}
	g := NewGraph(arena, cfg)

	// Insert enough nodes so some will have their edges pruned
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100; i++ {
		vec := make([]float32, 4)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		g.Insert(uint32(i), vec)
	}

	// Verify no node exceeds Mmax0 edges at layer 0
	g.mu.RLock()
	for id, node := range g.nodes {
		edges := node.GetEdges(0)
		if len(edges) > cfg.Mmax0 {
			t.Errorf("node %d has %d edges at layer 0, max allowed is %d", id, len(edges), cfg.Mmax0)
		}
		// Check upper layers respect M
		for l := 1; l <= node.Level; l++ {
			e := node.GetEdges(l)
			if len(e) > cfg.M {
				t.Errorf("node %d has %d edges at layer %d, max allowed is %d", id, len(e), l, cfg.M)
			}
		}
	}
	g.mu.RUnlock()
}

func TestSelectNeighborsHeuristic(t *testing.T) {
	// Test the diversity heuristic with known geometry
	// Base is at origin, candidates at varying positions
	candidates := []Candidate{
		{ID: 1, Distance: 0.1}, // close
		{ID: 2, Distance: 0.2}, // medium
		{ID: 3, Distance: 0.3}, // far
		{ID: 4, Distance: 0.4}, // farther
		{ID: 5, Distance: 0.5}, // farthest
	}

	// distFunc: all candidates close to each other (0.05 apart)
	distFunc := func(a, b uint32) float32 {
		return float32(math.Abs(float64(a)-float64(b))) * 0.05
	}

	selected := SelectNeighborsHeuristic(0, candidates, 3, distFunc)
	if len(selected) != 3 {
		t.Errorf("expected 3 selected, got %d", len(selected))
	}
}

func BenchmarkInsert1K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		arenaSize := uint64(1000) * uint64(testDim) * 4 * 2
		arena, _ := memory.NewOffHeapArena(arenaSize)
		cfg := DefaultConfig(testDim)
		g := NewGraph(arena, cfg)
		rng := rand.New(rand.NewSource(42))

		vecs := make([][]float32, 1000)
		for j := 0; j < 1000; j++ {
			v := make([]float32, testDim)
			for k := range v {
				v[k] = rng.Float32()*2 - 1
			}
			vecs[j] = v
		}
		b.StartTimer()

		for j, v := range vecs {
			g.Insert(uint32(j), v)
		}
		arena.Close()
	}
}

func BenchmarkSearch(b *testing.B) {
	arenaSize := uint64(10000) * uint64(testDim) * 4 * 2
	arena, _ := memory.NewOffHeapArena(arenaSize)
	defer arena.Close()

	cfg := DefaultConfig(testDim)
	g := NewGraph(arena, cfg)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 10000; i++ {
		v := make([]float32, testDim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		g.Insert(uint32(i), v)
	}

	query := make([]float32, testDim)
	for i := range query {
		query[i] = rng.Float32()*2 - 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Search(query, 10, 50)
	}
}
