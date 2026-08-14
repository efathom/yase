package index

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func tempHybridEngine(t *testing.T, vecDim int) (*HybridEngine, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "hybrid-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	indexPath := filepath.Join(dir, "test.bluge")
	arenaSize := uint64(100000 * vecDim * 4 * 2)
	he, err := NewHybridEngine(indexPath, arenaSize, vecDim, 5) // 20% centroids
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewHybridEngine: %v", err)
	}
	return he, func() {
		he.Close()
		os.RemoveAll(dir)
	}
}

func TestHybridIngestAndSearch(t *testing.T) {
	const vecDim = 32
	he, cleanup := tempHybridEngine(t, vecDim)
	defer cleanup()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	// Insert 50 documents — 10 centroids (IDs 0,5,10,...,45) and 40 leaves
	for i := 0; i < 50; i++ {
		vec := make([]float32, vecDim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		doc := Document{
			ID:       uint32(i),
			Text:     "search engine vector database document " + string(rune('A'+i%26)),
			Vector:   vec,
			Metadata: map[string]string{"group": "test"},
		}
		if err := he.Ingest(ctx, doc); err != nil {
			t.Fatalf("Ingest(%d): %v", i, err)
		}
	}

	// Search
	queryVec := make([]float32, vecDim)
	for j := range queryVec {
		queryVec[j] = rng.Float32()*2 - 1
	}

	results, err := he.HybridSearch(ctx, "search engine", queryVec, map[string]string{"group": "test"}, 5)
	if err != nil {
		t.Fatalf("HybridSearch: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected results from hybrid search")
	}
	if len(results) > 5 {
		t.Errorf("expected at most 5 results, got %d", len(results))
	}

	// Verify results are sorted by fused score descending
	for i := 1; i < len(results); i++ {
		if results[i].FusedScore > results[i-1].FusedScore {
			t.Errorf("results not sorted: index %d (%f) > index %d (%f)",
				i, results[i].FusedScore, i-1, results[i-1].FusedScore)
		}
	}

	t.Logf("Hybrid search returned %d results", len(results))
	for _, r := range results {
		t.Logf("  ID=%d BM25=%.4f Semantic=%.4f Fused=%.6f", r.ID, r.BM25Score, r.SemanticScore, r.FusedScore)
	}
}

func TestHybridSearchEmptyIndex(t *testing.T) {
	he, cleanup := tempHybridEngine(t, 16)
	defer cleanup()

	queryVec := make([]float32, 16)
	results, err := he.HybridSearch(context.Background(), "anything", queryVec, nil, 5)
	if err != nil {
		t.Fatalf("HybridSearch: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results on empty index, got %d", len(results))
	}
}

func TestHybridSearchMetadataFilter(t *testing.T) {
	const vecDim = 16
	he, cleanup := tempHybridEngine(t, vecDim)
	defer cleanup()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	// Insert docs with different tenants
	for i := 0; i < 30; i++ {
		vec := make([]float32, vecDim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		tenant := "alpha"
		if i%2 == 0 {
			tenant = "beta"
		}
		doc := Document{
			ID:       uint32(i),
			Text:     "data processing pipeline",
			Vector:   vec,
			Metadata: map[string]string{"tenant": tenant},
		}
		he.Ingest(ctx, doc)
	}

	queryVec := make([]float32, vecDim)
	for j := range queryVec {
		queryVec[j] = rng.Float32()*2 - 1
	}

	// Search only tenant=alpha
	results, err := he.HybridSearch(ctx, "data", queryVec, map[string]string{"tenant": "alpha"}, 10)
	if err != nil {
		t.Fatalf("HybridSearch: %v", err)
	}

	// All results should be from tenant alpha (odd IDs)
	for _, r := range results {
		if r.ID%2 == 0 {
			t.Errorf("result ID %d belongs to tenant beta, expected only alpha", r.ID)
		}
	}
	t.Logf("Metadata-filtered search returned %d results", len(results))
}

func TestHybridCentroidAndLeafCounts(t *testing.T) {
	const vecDim = 8
	he, cleanup := tempHybridEngine(t, vecDim)
	defer cleanup()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 20; i++ {
		vec := make([]float32, vecDim)
		for j := range vec {
			vec[j] = rng.Float32()
		}
		he.Ingest(ctx, Document{
			ID:     uint32(i),
			Text:   "test document",
			Vector: vec,
		})
	}

	// With centroidRate=5, IDs 0,5,10,15 are centroids (4 total)
	if he.Graph.Len() != 4 {
		t.Errorf("expected 4 centroids in HNSW graph, got %d", he.Graph.Len())
	}

	// Verify inverted files have leaves
	_, leafCount := he.InvertedFileStats()
	if leafCount != 16 {
		t.Errorf("expected 16 leaves in inverted files, got %d", leafCount)
	}
}
