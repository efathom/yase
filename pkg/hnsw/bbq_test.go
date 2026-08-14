package hnsw

import (
	"math/rand"
	"testing"

	"github.com/efathom/yase/pkg/memory"
)

func TestSearchBBQRecall(t *testing.T) {
	const dim = 64
	const numVectors = 1000
	arenaSize := uint64(numVectors * (dim*4 + 128) * 2) // float32 + binary + headroom

	arena, err := memory.NewOffHeapArena(arenaSize)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	cfg := DefaultConfig(dim)
	cfg.BBQEnabled = true
	g := NewGraph(arena, cfg)

	rng := rand.New(rand.NewSource(42))
	vecs := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		vecs[i] = vec
		if err := g.Insert(uint32(i), vec); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// Run exact search for ground truth
	queryVec := vecs[0]
	exactResults := g.Search(queryVec, 10, 50)

	// Run BBQ search with various oversampling factors
	for _, oversample := range []int{1, 3, 5} {
		bbqResults := g.SearchBBQ(queryVec, 10, 50, oversample)

		// Calculate recall@10
		exactIDs := make(map[uint32]bool)
		for _, r := range exactResults {
			exactIDs[r.ID] = true
		}
		hits := 0
		for _, r := range bbqResults {
			if exactIDs[r.ID] {
				hits++
			}
		}
		recall := float64(hits) / float64(len(exactResults))
		t.Logf("BBQ oversample=%d: recall@10=%.1f%% (%d/%d)", oversample, recall*100, hits, len(exactResults))

		// Binary quantization at dim=64 has limited precision; at production
		// dimensions (768+), recall is significantly higher.
		if oversample >= 3 && recall < 0.5 {
			t.Errorf("expected recall >= 50%% with oversample=%d, got %.1f%%", oversample, recall*100)
		}
	}
}

func TestSearchBBQEmptyGraph(t *testing.T) {
	arena, _ := memory.NewOffHeapArena(4096)
	defer arena.Close()

	cfg := DefaultConfig(32)
	cfg.BBQEnabled = true
	g := NewGraph(arena, cfg)

	results := g.SearchBBQ(make([]float32, 32), 10, 50, 3)
	if len(results) != 0 {
		t.Errorf("expected empty results for empty graph, got %d", len(results))
	}
}

func TestBBQDisabledFallback(t *testing.T) {
	const dim = 32
	arena, _ := memory.NewOffHeapArena(1 << 20)
	defer arena.Close()

	// BBQ disabled — nodes should have BinaryVectorLen == 0
	cfg := DefaultConfig(dim)
	cfg.BBQEnabled = false
	g := NewGraph(arena, cfg)

	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i)
	}
	g.Insert(0, vec)

	node := g.getNode(0)
	if node.BinaryVectorLen != 0 {
		t.Errorf("expected BinaryVectorLen=0 when BBQ disabled, got %d", node.BinaryVectorLen)
	}
}

func BenchmarkSearchBBQ(b *testing.B) {
	const dim = 128
	const numVectors = 5000
	arenaSize := uint64(numVectors * (dim*4 + 128) * 2)

	arena, _ := memory.NewOffHeapArena(arenaSize)
	defer arena.Close()

	cfg := DefaultConfig(dim)
	cfg.BBQEnabled = true
	g := NewGraph(arena, cfg)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		g.Insert(uint32(i), vec)
	}

	queryVec := make([]float32, dim)
	for i := range queryVec {
		queryVec[i] = rng.Float32()*2 - 1
	}

	b.Run("ExactSearch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			g.Search(queryVec, 10, 50)
		}
	})

	b.Run("BBQ_oversample3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			g.SearchBBQ(queryVec, 10, 50, 3)
		}
	})
}
