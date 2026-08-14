package hnsw

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/efathom/yase/pkg/memory"
)

func TestGraphSnapshotRestore(t *testing.T) {
	const dim = 32
	const numVectors = 500
	arenaSize := uint64(numVectors * dim * 4 * 2)

	arena, err := memory.NewOffHeapArena(arenaSize)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	cfg := DefaultConfig(dim)
	g := NewGraph(arena, cfg)

	// Insert vectors
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

	// Query before snapshot
	queryVec := vecs[0]
	resultsBefore := g.Search(queryVec, 10, 50)
	if len(resultsBefore) == 0 {
		t.Fatal("expected search results before snapshot")
	}

	// Snapshot
	var buf bytes.Buffer
	if err := g.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Logf("Snapshot size: %d bytes for %d nodes", buf.Len(), numVectors)

	// Restore into a new graph backed by the same arena
	g2, err := RestoreGraph(&buf, arena, cfg)
	if err != nil {
		t.Fatalf("RestoreGraph: %v", err)
	}

	// Verify node count
	if g2.Len() != g.Len() {
		t.Errorf("node count: got %d, want %d", g2.Len(), g.Len())
	}

	// Verify max level
	if g2.MaxLevel() != g.MaxLevel() {
		t.Errorf("max level: got %d, want %d", g2.MaxLevel(), g.MaxLevel())
	}

	// Search after restore should return same results
	resultsAfter := g2.Search(queryVec, 10, 50)
	if len(resultsAfter) != len(resultsBefore) {
		t.Fatalf("result count: got %d, want %d", len(resultsAfter), len(resultsBefore))
	}

	for i := range resultsBefore {
		if resultsBefore[i].ID != resultsAfter[i].ID {
			t.Errorf("result[%d]: got ID %d, want %d", i, resultsAfter[i].ID, resultsBefore[i].ID)
		}
	}
}

func TestGraphSnapshotEmpty(t *testing.T) {
	arena, _ := memory.NewOffHeapArena(4096)
	defer arena.Close()

	g := NewGraph(arena, DefaultConfig(32))

	var buf bytes.Buffer
	if err := g.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot empty graph: %v", err)
	}

	g2, err := RestoreGraph(&buf, arena, DefaultConfig(32))
	if err != nil {
		t.Fatalf("RestoreGraph empty: %v", err)
	}
	if g2.Len() != 0 {
		t.Errorf("expected empty graph, got %d nodes", g2.Len())
	}
}

func TestGraphSnapshotInvalidMagic(t *testing.T) {
	buf := bytes.NewBuffer([]byte("BADDxxxxxxxxxxxxxxxxxxxxxxxx"))
	arena, _ := memory.NewOffHeapArena(4096)
	defer arena.Close()

	_, err := RestoreGraph(buf, arena, DefaultConfig(32))
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}
