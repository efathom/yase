package glue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/chunking"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/metrics"
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

	// Accept, when set, filters records this process should index (e.g. the
	// records whose URL hashes to this node's shard). Rejected records are
	// acknowledged without indexing.
	Accept func(record *ingestionv1.CrawlRecord) bool

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
//
// The message is fully processed — chunked, hooked, embedded — before the
// index is touched, and then all previous chunks of the URL are replaced in a
// single atomic batch. Any failure returns an error so the message is retried
// (and eventually dead-lettered) instead of being committed half-indexed.
func (p *PipelineHandler) Handle(ctx context.Context, msg kafka.Message) error {
	// 1. Deserialize CrawlRecord from protobuf
	var record ingestionv1.CrawlRecord
	if err := proto.Unmarshal(msg.Value, &record); err != nil {
		return Permanent(fmt.Errorf("unmarshal crawl record: %w", err))
	}
	if record.Url == "" {
		return Permanent(fmt.Errorf("crawl record has no url"))
	}
	if p.Accept != nil && !p.Accept(&record) {
		return nil // belongs to another shard
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
		return Permanent(fmt.Errorf("resolve embedder for collection %q: %w", collectionID, err))
	}
	eng, err := p.resolveEngine(collectionID)
	if err != nil {
		return Permanent(fmt.Errorf("resolve engine for collection %q: %w", collectionID, err))
	}

	// 3. Semantic chunking — sentences are embedded in one batch call.
	opts := chunking.DefaultOptions()
	opts.Threshold = p.Threshold
	chunks, err := chunking.SemanticChunkerWithOptions(markdown, func(texts []string) ([][]float32, error) {
		return emb.EmbedBatch(ctx, texts)
	}, opts)
	if err != nil {
		return fmt.Errorf("semantic chunker: %w", err)
	}
	if len(chunks) == 0 {
		return nil
	}

	// 4. WASM pre-embed hooks (NER, PII redaction, ...). Fail closed: indexing
	// text a redaction hook could not process would leak what it guards.
	if p.WasmEngine != nil {
		for i, c := range chunks {
			processed, err := p.WasmEngine.ExecuteHooks(ctx, pipeline.PreEmbed, c)
			if err != nil {
				return fmt.Errorf("WASM PreEmbed hook on chunk %d: %w", i, err)
			}
			chunks[i] = processed
		}
	}

	// 5. Embed all chunks in one batch.
	vecs, err := emb.EmbedBatch(ctx, chunks)
	if err != nil {
		return fmt.Errorf("embed %d chunks: %w", len(chunks), err)
	}
	if len(vecs) != len(chunks) {
		return fmt.Errorf("embedder returned %d vectors for %d chunks", len(vecs), len(chunks))
	}

	// 6. Build documents with engine-assigned IDs (unique within the
	// collection, never reused — no hash collisions).
	docs := make([]index.Document, len(chunks))
	for i, chunkText := range chunks {
		id, err := eng.NextID()
		if err != nil {
			return err
		}
		metadata := make(map[string]string, len(record.Metadata)+2)
		for k, v := range record.Metadata {
			metadata[k] = v
		}
		metadata["url"] = record.Url
		metadata["chunk_idx"] = strconv.Itoa(i)
		docs[i] = index.Document{ID: id, Text: chunkText, Vector: vecs[i], Metadata: metadata}
	}

	// 7. Atomically replace this URL's previous chunks. The replace scope
	// includes the tenant, so one tenant's record can never remove another
	// tenant's documents that happen to share a URL.
	replace := map[string]string{"url": record.Url}
	if t := record.Metadata[auth.TenantField]; t != "" {
		replace[auth.TenantField] = t
	}
	if err := eng.IngestBatch(ctx, replace, docs); err != nil {
		if errors.Is(err, index.ErrDimensionMismatch) {
			return Permanent(err)
		}
		return fmt.Errorf("index %d chunks for %s: %w", len(docs), record.Url, err)
	}
	metrics.RecordsIndexed.WithLabelValues(collectionID).Inc()
	slog.Debug("pipeline: indexed record", "url", record.Url, "collection", collectionID, "chunks", len(docs))
	return nil
}
