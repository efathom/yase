package builder

import (
	"context"
	"math/rand"
	"testing"

	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/storage"
)

func TestBuildAndLoadManifest(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewFSStore(dir)
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	dim := 32
	docs := generateDocs(rng, 100, dim)

	ctx := context.Background()
	manifest, err := Build(ctx, docs, &BuildConfig{
		NumShards:    3,
		VecDim:       dim,
		CentroidRate: 5,
		ArenaSize:    16 * 1024 * 1024,
		OutputPrefix: "indexes/test-v1",
	}, store)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if manifest.DocCount != 100 {
		t.Errorf("doc count: got %d, want 100", manifest.DocCount)
	}
	if len(manifest.ShardIDs) == 0 {
		t.Fatal("expected at least 1 shard")
	}
	if len(manifest.Paths) != len(manifest.ShardIDs) {
		t.Errorf("paths count %d != shard count %d", len(manifest.Paths), len(manifest.ShardIDs))
	}

	t.Logf("Built %d shards from %d docs (version=%s)", len(manifest.ShardIDs), manifest.DocCount, manifest.Version)
	for _, sid := range manifest.ShardIDs {
		t.Logf("  shard-%d: %s", sid, manifest.Paths[sid])
	}

	// Verify manifest is loadable from storage
	loaded, err := LoadManifest(ctx, store, "indexes/test-v1/manifest.json")
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if loaded.DocCount != 100 {
		t.Errorf("loaded doc count: got %d, want 100", loaded.DocCount)
	}
	if len(loaded.ShardIDs) != len(manifest.ShardIDs) {
		t.Errorf("loaded shard count: got %d, want %d", len(loaded.ShardIDs), len(manifest.ShardIDs))
	}
}

func TestBuildSingleShard(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewFSStore(dir)

	rng := rand.New(rand.NewSource(42))
	docs := generateDocs(rng, 20, 16)

	manifest, err := Build(context.Background(), docs, &BuildConfig{
		NumShards:    1,
		VecDim:       16,
		CentroidRate: 5,
		ArenaSize:    8 * 1024 * 1024,
		OutputPrefix: "indexes/single",
	}, store)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if len(manifest.ShardIDs) != 1 {
		t.Errorf("expected 1 shard, got %d", len(manifest.ShardIDs))
	}
}

func TestBuildShardMetaUploaded(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewFSStore(dir)

	rng := rand.New(rand.NewSource(42))
	docs := generateDocs(rng, 50, 16)

	manifest, err := Build(context.Background(), docs, &BuildConfig{
		NumShards:    2,
		VecDim:       16,
		OutputPrefix: "indexes/meta-test",
	}, store)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Verify meta.json exists for each shard
	for _, sid := range manifest.ShardIDs {
		metaPath := manifest.Paths[sid] + "/meta.json"
		rc, err := store.Download(context.Background(), metaPath)
		if err != nil {
			t.Errorf("shard %d: meta.json not found: %v", sid, err)
			continue
		}
		rc.Close()
	}
}

func TestBuildErrors(t *testing.T) {
	store, _ := storage.NewFSStore(t.TempDir())
	ctx := context.Background()

	t.Run("no docs", func(t *testing.T) {
		_, err := Build(ctx, nil, &BuildConfig{NumShards: 1, VecDim: 16}, store)
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("zero shards", func(t *testing.T) {
		_, err := Build(ctx, []index.Document{{ID: 1}}, &BuildConfig{NumShards: 0, VecDim: 16}, store)
		if err == nil {
			t.Error("expected error")
		}
	})
}

func generateDocs(rng *rand.Rand, n, dim int) []index.Document {
	docs := make([]index.Document, n)
	for i := range docs {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		docs[i] = index.Document{
			ID:       uint32(i + 1),
			Text:     "test document content for search indexing",
			Vector:   vec,
			Metadata: map[string]string{"batch": "test"},
		}
	}
	return docs
}
