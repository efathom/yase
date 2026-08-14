package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/routing"
	"github.com/efathom/yase/pkg/storage"
)

// BuildConfig defines an offline index build job.
type BuildConfig struct {
	NumShards    int    // target shard count
	VecDim       int    // vector dimensionality
	CentroidRate int    // HNSW-IF centroid rate (e.g. 5 = 20%)
	ArenaSize    uint64 // arena size per shard
	OutputPrefix string // storage path prefix (e.g. "indexes/v42")
}

// BuildManifest describes a completed index build.
type BuildManifest struct {
	Version   string            `json:"version"`
	ShardIDs  []uint32          `json:"shard_ids"`
	Paths     map[uint32]string `json:"paths"` // shardID → storage prefix
	DocCount  int               `json:"doc_count"`
	CreatedAt time.Time         `json:"created_at"`
}

// Build runs an offline index build from the given documents:
// 1. Shard documents via consistent hashing
// 2. For each shard: build Bluge + HNSW-IF index
// 3. Upload manifest to store
// 4. Return manifest for alias swap
func Build(ctx context.Context, docs []index.Document, cfg *BuildConfig, store storage.IndexStore) (*BuildManifest, error) {
	if cfg.NumShards <= 0 {
		return nil, fmt.Errorf("builder: NumShards must be > 0")
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("builder: no documents to index")
	}

	// Shard documents via consistent hashing
	ring := routing.NewHashRing(256)
	for i := 0; i < cfg.NumShards; i++ {
		ring.AddNode(fmt.Sprintf("shard-%d", i))
	}

	shardDocs := make(map[uint32][]index.Document)
	for _, doc := range docs {
		node := ring.GetNode(fmt.Sprintf("%d", doc.ID))
		// Parse shard ID from node name
		var shardID uint32
		fmt.Sscanf(node, "shard-%d", &shardID)
		shardDocs[shardID] = append(shardDocs[shardID], doc)
	}

	version := fmt.Sprintf("v%d", time.Now().Unix())
	manifest := &BuildManifest{
		Version:   version,
		ShardIDs:  make([]uint32, 0, cfg.NumShards),
		Paths:     make(map[uint32]string),
		DocCount:  len(docs),
		CreatedAt: time.Now(),
	}

	// Build each shard
	for shardID := uint32(0); shardID < uint32(cfg.NumShards); shardID++ {
		docs := shardDocs[shardID]
		if len(docs) == 0 {
			continue
		}

		shardPrefix := fmt.Sprintf("%s/shard-%d", cfg.OutputPrefix, shardID)

		arenaSize := cfg.ArenaSize
		if arenaSize == 0 {
			arenaSize = 64 * 1024 * 1024 // 64MB default
		}
		centroidRate := cfg.CentroidRate
		if centroidRate == 0 {
			centroidRate = 5
		}

		indexPath := fmt.Sprintf("/tmp/yase-build-%s-shard-%d", version, shardID)
		engine, err := index.NewHybridEngine(indexPath, arenaSize, cfg.VecDim, centroidRate)
		if err != nil {
			return nil, fmt.Errorf("shard %d: create engine: %w", shardID, err)
		}

		for _, doc := range docs {
			if err := engine.Ingest(ctx, doc); err != nil {
				engine.Close()
				return nil, fmt.Errorf("shard %d: ingest doc %d: %w", shardID, doc.ID, err)
			}
		}

		// Serialize shard metadata
		meta := shardMeta{
			ShardID:  shardID,
			DocCount: len(docs),
			VecDim:   cfg.VecDim,
		}
		metaBytes, _ := json.Marshal(meta)

		if err := store.Upload(ctx, shardPrefix+"/meta.json", bytes.NewReader(metaBytes)); err != nil {
			engine.Close()
			return nil, fmt.Errorf("shard %d: upload meta: %w", shardID, err)
		}

		engine.Close()

		manifest.ShardIDs = append(manifest.ShardIDs, shardID)
		manifest.Paths[shardID] = shardPrefix
	}

	// Upload manifest
	manifestBytes, _ := json.Marshal(manifest)
	manifestPath := fmt.Sprintf("%s/manifest.json", cfg.OutputPrefix)
	if err := store.Upload(ctx, manifestPath, bytes.NewReader(manifestBytes)); err != nil {
		return nil, fmt.Errorf("upload manifest: %w", err)
	}

	return manifest, nil
}

// LoadManifest reads a build manifest from storage.
func LoadManifest(ctx context.Context, store storage.IndexStore, path string) (*BuildManifest, error) {
	rc, err := store.Download(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("download manifest: %w", err)
	}
	defer rc.Close()

	var manifest BuildManifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &manifest, nil
}

type shardMeta struct {
	ShardID  uint32 `json:"shard_id"`
	DocCount int    `json:"doc_count"`
	VecDim   int    `json:"vec_dim"`
}
