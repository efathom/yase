package glue

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/efathom/yase/pkg/chunking"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/pipeline"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// PipelineHandler processes Kafka messages through the full indexing pipeline:
// deserialize → chunk → embed → WASM hooks → hybrid index.
//
// It routes each record to the correct collection's engine using _collection_id
// metadata. Records without a collection ID are routed to _default.
type PipelineHandler struct {
	CollectionMgr *collection.Manager
	WasmEngine    *pipeline.WasmEngine
	Threshold     float32 // Semantic chunking similarity threshold

	// Deprecated: kept for backward compatibility with NewPipelineHandler.
	// Prefer using CollectionMgr for engine/embedder resolution.
	Embedder embedder.Embedder
	Engine   *index.HybridEngine
}

// NewPipelineHandler creates a pipeline handler with all dependencies.
// Deprecated: Use NewCollectionPipelineHandler for collection-aware pipelines.
func NewPipelineHandler(emb embedder.Embedder, engine *index.HybridEngine, wasmEngine *pipeline.WasmEngine, threshold float32) *PipelineHandler {
	if threshold <= 0 {
		threshold = 0.75
	}
	return &PipelineHandler{
		Embedder:   emb,
		Engine:     engine,
		WasmEngine: wasmEngine,
		Threshold:  threshold,
	}
}

// NewCollectionPipelineHandler creates a collection-aware pipeline handler.
func NewCollectionPipelineHandler(mgr *collection.Manager, wasmEngine *pipeline.WasmEngine, threshold float32) *PipelineHandler {
	if threshold <= 0 {
		threshold = 0.75
	}
	return &PipelineHandler{
		CollectionMgr: mgr,
		WasmEngine:    wasmEngine,
		Threshold:     threshold,
	}
}

// resolveEngine returns the engine for the given collection ID.
// Falls back to the legacy single-engine field if CollectionMgr is nil.
func (p *PipelineHandler) resolveEngine(collectionID string) (*index.HybridEngine, error) {
	if p.CollectionMgr != nil {
		return p.CollectionMgr.GetEngine(collectionID)
	}
	if p.Engine != nil {
		return p.Engine, nil
	}
	return nil, fmt.Errorf("no engine available")
}

// resolveEmbedder returns the embedder for the given collection ID.
// Falls back to the legacy single-embedder field if CollectionMgr is nil.
func (p *PipelineHandler) resolveEmbedder(collectionID string) (embedder.Embedder, error) {
	if p.CollectionMgr != nil {
		return p.CollectionMgr.GetEmbedder(collectionID)
	}
	if p.Embedder != nil {
		return p.Embedder, nil
	}
	return nil, fmt.Errorf("no embedder available")
}

// Handle processes a single Kafka message through the indexing pipeline.
func (p *PipelineHandler) Handle(ctx context.Context, msg kafka.Message) error {
	// 1. Deserialize CrawlRecord from protobuf
	var record ingestionv1.CrawlRecord
	if err := proto.Unmarshal(msg.Value, &record); err != nil {
		return fmt.Errorf("unmarshal crawl record: %w", err)
	}

	markdown := string(record.RawContent)
	if markdown == "" {
		return nil // Nothing to index
	}

	// 2. Resolve collection: use _collection_id from metadata, fall back to _default
	collectionID := record.Metadata["_collection_id"]
	if collectionID == "" {
		collectionID = collection.DefaultCollectionID
	}

	emb, err := p.resolveEmbedder(collectionID)
	if err != nil {
		return fmt.Errorf("resolve embedder for collection %q: %w", collectionID, err)
	}

	eng, err := p.resolveEngine(collectionID)
	if err != nil {
		return fmt.Errorf("resolve engine for collection %q: %w", collectionID, err)
	}

	// 2b. Delete-before-write: remove any previously indexed chunks for this
	// URL so that re-ingestion with fewer chunks does not leave stale results.
	if _, err := eng.DeleteByFilter(ctx, map[string]string{"url": record.Url}); err != nil {
		slog.Warn("pipeline: delete-before-write failed (continuing)", "url", record.Url, "error", err)
	}

	// 3. Semantic chunking using valley detection
	embedFn := func(text string) ([]float32, error) {
		return emb.Embed(ctx, text)
	}

	chunks, err := chunking.SemanticChunker(markdown, embedFn, p.Threshold)
	if err != nil {
		return fmt.Errorf("semantic chunker: %w", err)
	}

	// 4. Process each chunk through the pipeline
	for i, chunkText := range chunks {
		// WASM pre-embed hook (NER, PII redaction, etc.)
		if p.WasmEngine != nil {
			processed, err := p.WasmEngine.ExecuteHooks(ctx, pipeline.PreEmbed, chunkText)
			if err != nil {
				slog.Warn("pipeline: WASM PreEmbed hook error", "error", err)
			} else {
				chunkText = processed
			}
		}

		// Embed the chunk
		vec, err := emb.Embed(ctx, chunkText)
		if err != nil {
			slog.Error("pipeline: embed error", "chunk", i, "url", record.Url, "error", err)
			continue
		}

		// Deterministic chunk ID: FNV-1a hash of "url#chunk_idx"
		chunkID := hashChunkID(record.Url, i)

		// Build metadata
		metadata := make(map[string]string)
		for k, v := range record.Metadata {
			metadata[k] = v
		}
		metadata["url"] = record.Url
		metadata["chunk_idx"] = fmt.Sprintf("%d", i)

		// Ingest into hybrid engine (Bluge + HNSW-IF)
		doc := index.Document{
			ID:       chunkID,
			Text:     chunkText,
			Vector:   vec,
			Metadata: metadata,
		}
		if err := eng.Ingest(ctx, doc); err != nil {
			slog.Error("pipeline: ingest error", "chunk", i, "url", record.Url, "error", err)
			continue
		}
	}

	return nil
}

// hashChunkID returns a deterministic uint32 chunk ID from url#index.
func hashChunkID(url string, chunkIdx int) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s#%d", url, chunkIdx)))
	return h.Sum32()
}
