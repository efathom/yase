package index

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/efathom/yase/pkg/memory"
)

func TestHybridEnginePersistRestore(t *testing.T) {
	dir := t.TempDir()
	const dim = 32
	const numDocs = 100
	const centroidRate = 5

	arenaPath := filepath.Join(dir, "arena.bin")
	indexPath := filepath.Join(dir, "bluge")
	persistDir := filepath.Join(dir, "snapshot")

	// Create with FileArena
	arena, err := memory.NewFileArena(arenaPath, 1<<20)
	if err != nil {
		t.Fatalf("NewFileArena: %v", err)
	}

	engine, err := NewHybridEngineWithArena(indexPath, arena, dim, centroidRate)
	if err != nil {
		t.Fatalf("NewHybridEngineWithArena: %v", err)
	}

	// Ingest documents
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		doc := Document{
			ID:       uint32(i),
			Text:     "test document",
			Vector:   vec,
			Metadata: map[string]string{"idx": "test"},
		}
		if err := engine.Ingest(ctx, doc); err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
	}

	// Verify before snapshot
	centroids, leaves := engine.InvertedFileStats()
	t.Logf("Before: %d centroids, %d leaves, %d graph nodes", centroids, leaves, engine.Graph.Len())

	// Snapshot
	cfg := PersistConfig{Dir: persistDir, ArenaPath: arenaPath}
	if err := engine.SaveSnapshot(cfg); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Close original
	engine.Close()

	// Restore
	engine2, err := LoadHybridEngine(cfg, indexPath, dim, centroidRate)
	if err != nil {
		t.Fatalf("LoadHybridEngine: %v", err)
	}
	defer engine2.Close()

	// Verify after restore
	centroids2, leaves2 := engine2.InvertedFileStats()
	t.Logf("After: %d centroids, %d leaves, %d graph nodes", centroids2, leaves2, engine2.Graph.Len())

	if centroids2 != centroids {
		t.Errorf("centroids: got %d, want %d", centroids2, centroids)
	}
	if leaves2 != leaves {
		t.Errorf("leaves: got %d, want %d", leaves2, leaves)
	}

	// Verify vector offsets round-tripped
	var offsetCount int
	engine2.VectorOffsets.Range(func(_, _ any) bool {
		offsetCount++
		return true
	})
	if offsetCount != numDocs {
		t.Errorf("vector offsets: got %d, want %d", offsetCount, numDocs)
	}
}
