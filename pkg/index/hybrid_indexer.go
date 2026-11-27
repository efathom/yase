package index

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/hnsw"
	"github.com/efathom/yase/pkg/memory"
	"github.com/efathom/yase/pkg/reranker"
	"github.com/efathom/yase/pkg/vector"
)

var (
	// ErrEngineClosed is returned when an operation is attempted on a closed engine.
	ErrEngineClosed = errors.New("hybrid engine is closed")
	// ErrDimensionMismatch is returned when a vector's length differs from the engine's.
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
	// ErrIDSpaceExhausted is returned when the engine has assigned every uint32 ID.
	ErrIDSpaceExhausted = errors.New("document ID space exhausted; compact or reindex")
)

// HNswConfig converts app-level HNSW settings into an hnsw.Config.
func HNswConfig(cfg config.HNSWConfig, vecDim int) hnsw.Config {
	c := hnsw.DefaultConfig(vecDim)
	if cfg.M >= 2 {
		c.M = cfg.M
	}
	if cfg.Mmax0 >= c.M {
		c.Mmax0 = cfg.Mmax0
	}
	if cfg.EfConstruction > 0 {
		c.EfConstruction = cfg.EfConstruction
	}
	if cfg.EfSearch > 0 {
		c.EfSearch = cfg.EfSearch
	}
	return c
}

// engineGraphConfig fixes the distance function: the engine stores
// L2-normalized vectors, so cosine distance reduces to 1 - dot.
func engineGraphConfig(c hnsw.Config) hnsw.Config {
	c.DistFunc = vector.DistNormalizedDot
	return c
}

// Document represents a single unit to be indexed in the hybrid engine.
type Document struct {
	ID       uint32
	Text     string
	Vector   []float32
	Metadata map[string]string
}

// HybridEngine co-locates a Bluge inverted index (BM25 + Roaring Bitmaps) with
// an HNSW-IF vector index. Centroids (every CentroidRate-th doc) live in the
// HNSW graph; leaves are stored in per-centroid inverted file posting lists.
//
// Locking (always acquired in this order: writeMu, lifecycleMu, ifMu):
//   - lifecycleMu: RLock for every operation, Lock to Close or to swap in a
//     compacted arena/graph.
//   - writeMu: RLock for Ingest/Delete (which may run concurrently), Lock for
//     a snapshot or compaction, which need a stable cut. Searches never take
//     writeMu, so snapshots do not block queries.
//   - ifMu: guards invertedFiles, leafCentroid and offsets.
type HybridEngine struct {
	BlugeStore         *BlugeStore
	Arena              memory.Arena
	Graph              *hnsw.Graph
	VecDim             int
	CentroidRate       int               // every Nth doc is a centroid (e.g., 5 = 20%)
	EfSearch           int               // HNSW search width (ef)
	Reranker           reranker.Reranker // nil = disabled
	RerankerCandidates int               // how many RRF candidates to feed to reranker
	Probes             int               // centroids probed per query (0 = scale with corpus)

	ifMu          sync.RWMutex
	invertedFiles map[uint32][]uint32 // centroid ID → leaf doc IDs
	leafCentroid  map[uint32]uint32   // leaf doc ID → centroid ID (reverse index)
	offsets       map[uint32]uint64   // doc ID → arena byte offset of its vector

	hnswCfg   hnsw.Config
	arenaCap  uint64
	nextID    atomic.Uint64 // next auto-assigned document ID
	deadBytes atomic.Uint64 // arena bytes held by deleted/replaced vectors
	gen       atomic.Uint64 // bumped on every write; used for cache invalidation

	store *engineStore // non-nil for persistent engines

	writeMu     sync.RWMutex
	lifecycleMu sync.RWMutex
	closed      atomic.Bool
}

// NewHybridEngine creates an in-memory hybrid engine (Bluge persists at
// indexPath; vectors live in anonymous memory).
func NewHybridEngine(indexPath string, arenaSize uint64, vecDim int, centroidRate int) (*HybridEngine, error) {
	return NewHybridEngineWithConfig(indexPath, arenaSize, hnsw.DefaultConfig(vecDim), centroidRate)
}

// NewHybridEngineWithConfig creates an in-memory hybrid engine with custom
// HNSW hyperparameters. Use OpenHybridEngine for a durable engine.
func NewHybridEngineWithConfig(indexPath string, arenaSize uint64, hnswCfg hnsw.Config, centroidRate int) (*HybridEngine, error) {
	arena, err := memory.NewOffHeapArena(arenaSize)
	if err != nil {
		return nil, fmt.Errorf("arena: %w", err)
	}
	he, err := NewHybridEngineWithArenaConfig(indexPath, arena, hnswCfg, centroidRate)
	if err != nil {
		arena.Close()
		return nil, err
	}
	he.arenaCap = arenaSize
	return he, nil
}

// NewHybridEngineWithArena creates a HybridEngine using a provided arena.
func NewHybridEngineWithArena(indexPath string, arena memory.Arena, vecDim int, centroidRate int) (*HybridEngine, error) {
	return NewHybridEngineWithArenaConfig(indexPath, arena, hnsw.DefaultConfig(vecDim), centroidRate)
}

// NewHybridEngineWithArenaConfig creates a HybridEngine using a provided arena
// and custom HNSW hyperparameters.
func NewHybridEngineWithArenaConfig(indexPath string, arena memory.Arena, hnswCfg hnsw.Config, centroidRate int) (*HybridEngine, error) {
	bs, err := NewBlugeStore(indexPath)
	if err != nil {
		return nil, fmt.Errorf("bluge store: %w", err)
	}
	hnswCfg = engineGraphConfig(hnswCfg)
	return newEngine(bs, arena, hnsw.NewGraph(arena, hnswCfg), hnswCfg, centroidRate), nil
}

func newEngine(bs *BlugeStore, arena memory.Arena, g *hnsw.Graph, hnswCfg hnsw.Config, centroidRate int) *HybridEngine {
	return &HybridEngine{
		BlugeStore:    bs,
		Arena:         arena,
		Graph:         g,
		invertedFiles: make(map[uint32][]uint32),
		leafCentroid:  make(map[uint32]uint32),
		offsets:       make(map[uint32]uint64),
		VecDim:        hnswCfg.VecDim,
		CentroidRate:  normalizeCentroidRate(centroidRate),
		EfSearch:      hnswCfg.EfSearch,
		hnswCfg:       hnswCfg,
		arenaCap:      1 << 30,
	}
}

// normalizeCentroidRate ensures the centroid rate is positive (default 5 = 20%).
func normalizeCentroidRate(rate int) int {
	if rate <= 0 {
		return 5
	}
	return rate
}

func (he *HybridEngine) acquire() error {
	he.lifecycleMu.RLock()
	if he.closed.Load() {
		he.lifecycleMu.RUnlock()
		return ErrEngineClosed
	}
	return nil
}

func (he *HybridEngine) release() { he.lifecycleMu.RUnlock() }

// NextID reserves a fresh document ID that no live or previously used
// document in this engine has. IDs are engine-scoped: pair them with the
// collection (or shard) to identify a document globally.
func (he *HybridEngine) NextID() (uint32, error) {
	id := he.nextID.Add(1) - 1
	if id > math.MaxUint32 {
		return 0, ErrIDSpaceExhausted
	}
	return uint32(id), nil
}

// observeID advances the ID allocator past an explicitly supplied ID so that
// NextID never hands out an ID already in use.
func (he *HybridEngine) observeID(id uint32) {
	want := uint64(id) + 1
	for {
		cur := he.nextID.Load()
		if cur >= want || he.nextID.CompareAndSwap(cur, want) {
			return
		}
	}
}

// Generation returns a counter that changes whenever the engine's contents
// change. Caches compare it to detect stale entries.
func (he *HybridEngine) Generation() uint64 { return he.gen.Load() }

// NodeCount returns the number of HNSW nodes (centroids).
func (he *HybridEngine) NodeCount() int {
	if he.acquire() != nil {
		return 0
	}
	defer he.release()
	return he.Graph.Len()
}

// ArenaUsedBytes returns the arena allocation watermark.
func (he *HybridEngine) ArenaUsedBytes() uint64 {
	if he.acquire() != nil {
		return 0
	}
	defer he.release()
	return he.Arena.UsedBytes()
}

// DeadBytes returns arena bytes held by vectors of deleted or replaced documents.
func (he *HybridEngine) DeadBytes() uint64 { return he.deadBytes.Load() }

// DocCount returns the number of documents with a vector.
func (he *HybridEngine) DocCount() int {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	return len(he.offsets)
}

// Ingest adds or replaces a single document in both indexes.
func (he *HybridEngine) Ingest(ctx context.Context, doc Document) error {
	return he.IngestBatch(ctx, nil, []Document{doc})
}

type preparedDoc struct {
	doc        Document
	vec        []float32 // normalized
	offset     uint64
	isCentroid bool
}

// IngestBatch atomically replaces every document matching replace (if
// non-empty) with docs. The text-index side is a single Bluge batch, so a
// reader sees either the old version or the new one, never a mix.
func (he *HybridEngine) IngestBatch(ctx context.Context, replace map[string]string, docs []Document) error {
	he.writeMu.RLock()
	defer he.writeMu.RUnlock()
	if err := he.acquire(); err != nil {
		return err
	}
	defer he.release()

	for _, d := range docs {
		if len(d.Vector) != he.VecDim {
			return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(d.Vector), he.VecDim)
		}
	}

	var stale []uint32
	if len(replace) > 0 {
		ids, _, err := he.BlugeStore.FilteredIDs(ctx, replace, -1)
		if err != nil {
			return fmt.Errorf("resolve replaced documents: %w", err)
		}
		stale = ids
	}

	// 1. Allocate vectors first so an exhausted arena fails before anything
	// is visible.
	prepared := make([]preparedDoc, len(docs))
	vecBytes := uint64(he.VecDim * 4)
	for i, d := range docs {
		nv := vector.Normalized(d.Vector)
		off, err := he.Arena.AllocFloat32(nv)
		if err != nil {
			he.deadBytes.Add(uint64(i) * vecBytes) // earlier allocations are now garbage
			return fmt.Errorf("arena alloc: %w", err)
		}
		prepared[i] = preparedDoc{doc: d, vec: nv, offset: off}
	}

	// 2. Text index: delete stale + upsert new in one batch.
	newIDs := make(map[uint32]struct{}, len(docs))
	bdocs := make([]batchDoc, len(prepared))
	// The first document of an empty engine becomes a centroid regardless of
	// its ID so that leaves are never orphaned.
	needFirstCentroid := he.Graph.Len() == 0
	for i := range prepared {
		p := &prepared[i]
		newIDs[p.doc.ID] = struct{}{}
		he.observeID(p.doc.ID)
		p.isCentroid = p.doc.ID%uint32(he.CentroidRate) == 0 || (needFirstCentroid && i == 0)
		meta := make(map[string]string, len(p.doc.Metadata)+1)
		for k, v := range p.doc.Metadata {
			meta[k] = v
		}
		if p.isCentroid {
			meta["_type"] = "centroid"
		} else {
			meta["_type"] = "leaf"
		}
		bdocs[i] = batchDoc{id: p.doc.ID, content: p.doc.Text, metadata: meta}
	}
	var deletes []uint32
	for _, id := range stale {
		if _, kept := newIDs[id]; !kept {
			deletes = append(deletes, id)
		}
	}
	if err := he.BlugeStore.ApplyBatch(deletes, bdocs); err != nil {
		he.deadBytes.Add(uint64(len(prepared)) * vecBytes)
		return fmt.Errorf("bluge batch: %w", err)
	}

	// 3. Vector index.
	he.removeVectors(deletes)
	for _, p := range prepared {
		if err := he.insertVector(p.doc.ID, p.vec, p.offset, p.isCentroid); err != nil {
			return err
		}
	}
	he.gen.Add(1)
	return nil
}

// insertVector places a normalized vector (already in the arena) into the
// HNSW-IF structure, replacing any previous vector for id.
func (he *HybridEngine) insertVector(id uint32, vec []float32, offset uint64, isCentroid bool) error {
	he.ifMu.Lock()
	if _, existed := he.offsets[id]; existed {
		he.deadBytes.Add(uint64(he.VecDim * 4))
		he.unlinkLeafLocked(id)
	}
	he.offsets[id] = offset
	he.ifMu.Unlock()

	if isCentroid {
		if err := he.Graph.InsertAt(id, vec, offset); err != nil {
			return fmt.Errorf("hnsw insert centroid: %w", err)
		}
		return nil
	}
	if centroidID, ok := he.findNearestCentroid(vec); ok {
		he.ifMu.Lock()
		he.invertedFiles[centroidID] = append(he.invertedFiles[centroidID], id)
		he.leafCentroid[id] = centroidID
		he.ifMu.Unlock()
	}
	return nil
}

// unlinkLeafLocked removes id from its centroid's posting list. ifMu must be held.
func (he *HybridEngine) unlinkLeafLocked(id uint32) {
	c, ok := he.leafCentroid[id]
	if !ok {
		return
	}
	list := he.invertedFiles[c]
	for i, leaf := range list {
		if leaf == id {
			list[i] = list[len(list)-1]
			list = list[:len(list)-1]
			break
		}
	}
	he.invertedFiles[c] = list
	delete(he.leafCentroid, id)
}

// removeVectors drops ids from the vector index. Centroid graph nodes stay as
// routing waypoints (their vectors are dead) until the next compaction.
func (he *HybridEngine) removeVectors(ids []uint32) int {
	if len(ids) == 0 {
		return 0
	}
	removed := 0
	he.ifMu.Lock()
	for _, id := range ids {
		if _, ok := he.offsets[id]; ok {
			delete(he.offsets, id)
			he.unlinkLeafLocked(id)
			he.deadBytes.Add(uint64(he.VecDim * 4))
			removed++
		}
	}
	he.ifMu.Unlock()
	return removed
}

// findNearestCentroid performs a full HNSW greedy descent to find the closest
// centroid for a leaf document.
func (he *HybridEngine) findNearestCentroid(vec []float32) (uint32, bool) {
	if he.Graph.Len() == 0 {
		return 0, false
	}
	results := he.Graph.Search(vec, 1, 10)
	if len(results) == 0 {
		return 0, false
	}
	return results[0].ID, true
}

// GetInvertedFile returns a copy of the posting list for a centroid.
func (he *HybridEngine) GetInvertedFile(centroidID uint32) []uint32 {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	list := he.invertedFiles[centroidID]
	if len(list) == 0 {
		return nil
	}
	return append([]uint32(nil), list...)
}

// InvertedFileStats returns centroid count and total leaf count.
func (he *HybridEngine) InvertedFileStats() (centroids int, leaves int) {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	centroids = len(he.invertedFiles)
	for _, list := range he.invertedFiles {
		leaves += len(list)
	}
	return
}

// VectorOffset returns the arena offset of a document's vector.
func (he *HybridEngine) VectorOffset(id uint32) (uint64, bool) {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	off, ok := he.offsets[id]
	return off, ok
}

// Delete removes documents by ID from both indexes.
//
// When filters is non-empty, an ID is only removed if the stored document also
// matches those filters. This keeps tenant scoping enforceable on the
// delete-by-ID path: knowing another tenant's document IDs is not sufficient to
// delete them. Pass nil for an unconstrained delete.
func (he *HybridEngine) Delete(ctx context.Context, docIDs []uint32, filters map[string]string) (int, error) {
	he.writeMu.RLock()
	defer he.writeMu.RUnlock()
	if err := he.acquire(); err != nil {
		return 0, err
	}
	defer he.release()

	if len(docIDs) == 0 {
		return 0, nil
	}
	// Resolve which IDs exist (and pass the filters) — cost is O(len(docIDs)).
	present, err := he.BlugeStore.MatchIDs(ctx, docIDs, filters)
	if err != nil {
		return 0, fmt.Errorf("resolve delete: %w", err)
	}
	ids := make([]uint32, 0, len(present))
	for _, id := range docIDs {
		if present[id] {
			ids = append(ids, id)
			delete(present, id) // de-duplicate repeated IDs
		}
	}
	return he.deleteLocked(ids)
}

// deleteLocked removes ids. Callers hold lifecycleMu and writeMu for read.
func (he *HybridEngine) deleteLocked(ids []uint32) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if err := he.BlugeStore.ApplyBatch(ids, nil); err != nil {
		return 0, fmt.Errorf("bluge delete: %w", err)
	}
	he.removeVectors(ids)
	he.gen.Add(1)
	return len(ids), nil
}

// DeleteByFilter removes every document matching the given metadata filters.
// An empty filter is rejected rather than deleting the whole index.
func (he *HybridEngine) DeleteByFilter(ctx context.Context, filters map[string]string) (int, error) {
	if len(filters) == 0 {
		return 0, errors.New("delete by filter requires at least one filter")
	}
	he.writeMu.RLock()
	defer he.writeMu.RUnlock()
	if err := he.acquire(); err != nil {
		return 0, err
	}
	defer he.release()

	ids, _, err := he.BlugeStore.FilteredIDs(ctx, filters, -1)
	if err != nil {
		return 0, fmt.Errorf("search for delete: %w", err)
	}
	return he.deleteLocked(ids)
}

// Close releases all resources. Persistent engines write a final snapshot
// first. It waits for in-flight operations; later operations return
// ErrEngineClosed.
func (he *HybridEngine) Close() error {
	var snapErr error
	if he.store != nil && !he.closed.Load() {
		snapErr = he.Checkpoint()
	}

	he.lifecycleMu.Lock()
	defer he.lifecycleMu.Unlock()
	if he.closed.Swap(true) {
		return nil
	}
	blugeErr := he.BlugeStore.Close()
	arenaErr := he.Arena.Close()
	return errors.Join(snapErr, blugeErr, arenaErr)
}
