package index

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/hnsw"
	"github.com/efathom/yase/pkg/memory"
	"github.com/efathom/yase/pkg/reranker"
)

// ErrEngineClosed is returned when an operation is attempted on a closed engine.
var ErrEngineClosed = errors.New("hybrid engine is closed")

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

// Document represents a single unit to be indexed in the hybrid engine.
type Document struct {
	ID       uint32
	Text     string
	Vector   []float32
	Metadata map[string]string
}

// HybridEngine co-locates a Bluge inverted index (BM25 + Roaring Bitmaps) with
// an HNSW-IF vector index. Centroids (20%) live in the HNSW graph in RAM;
// leaves (80%) are stored in per-centroid inverted file posting lists.
type HybridEngine struct {
	BlugeStore         *BlugeStore
	Arena              memory.Arena
	Graph              *hnsw.Graph
	ifMu               sync.RWMutex
	invertedFiles      map[uint32][]uint32 // centroid ID → leaf doc IDs
	VectorOffsets      sync.Map            // map[uint32]uint64 — doc ID → arena byte offset
	VecDim             int
	CentroidRate       int               // every Nth doc is a centroid (e.g., 5 = 20%)
	EfSearch           int               // HNSW search width (ef)
	Reranker           reranker.Reranker // nil = disabled
	RerankerCandidates int               // how many RRF candidates to feed to reranker

	// lifecycleMu coordinates Close() with in-flight Search/Ingest/Delete.
	// Readers take RLock for the duration of an operation; Close takes Lock.
	lifecycleMu sync.RWMutex
	closed      atomic.Bool
}

// NewHybridEngine creates a hybrid search engine with co-located indexes.
func NewHybridEngine(indexPath string, arenaSize uint64, vecDim int, centroidRate int) (*HybridEngine, error) {
	return NewHybridEngineWithConfig(indexPath, arenaSize, hnsw.DefaultConfig(vecDim), centroidRate)
}

// NewHybridEngineWithConfig creates a hybrid search engine with custom HNSW
// hyperparameters.
func NewHybridEngineWithConfig(indexPath string, arenaSize uint64, hnswCfg hnsw.Config, centroidRate int) (*HybridEngine, error) {
	bs, err := NewBlugeStore(indexPath)
	if err != nil {
		return nil, fmt.Errorf("bluge store: %w", err)
	}

	arena, err := memory.NewOffHeapArena(arenaSize)
	if err != nil {
		bs.Close()
		return nil, fmt.Errorf("arena: %w", err)
	}

	graph := hnsw.NewGraph(arena, hnswCfg)

	return &HybridEngine{
		BlugeStore:    bs,
		Arena:         arena,
		Graph:         graph,
		invertedFiles: make(map[uint32][]uint32),
		VecDim:        hnswCfg.VecDim,
		CentroidRate:  normalizeCentroidRate(centroidRate),
		EfSearch:      hnswCfg.EfSearch,
	}, nil
}

// normalizeCentroidRate ensures the centroid rate is positive (default 5 = 20%).
func normalizeCentroidRate(rate int) int {
	if rate <= 0 {
		return 5
	}
	return rate
}

// Ingest adds a document to both the Bluge inverted index and the HNSW-IF
// vector index. The routing decision (centroid vs leaf) is based on doc ID.
func (he *HybridEngine) Ingest(ctx context.Context, doc Document) error {
	he.lifecycleMu.RLock()
	defer he.lifecycleMu.RUnlock()
	if he.closed.Load() {
		return ErrEngineClosed
	}

	if he.CentroidRate <= 0 {
		he.CentroidRate = normalizeCentroidRate(he.CentroidRate)
	}

	// Validate vector dimensionality to prevent arena OOB reads on search.
	if len(doc.Vector) != he.VecDim {
		return fmt.Errorf("vector dimension mismatch: got %d, want %d", len(doc.Vector), he.VecDim)
	}

	// 1. Allocate vector in off-heap arena
	offset, err := he.Arena.AllocFloat32(doc.Vector)
	if err != nil {
		return fmt.Errorf("arena alloc: %w", err)
	}
	he.VectorOffsets.Store(doc.ID, offset)

	// 2. HNSW-IF routing
	// The first document becomes a centroid regardless of its ID so that early
	// leaves are never orphaned before a centroid exists.
	isCentroid := doc.ID%uint32(he.CentroidRate) == 0 || he.Graph.Len() == 0

	// Prepare metadata for Bluge
	meta := doc.Metadata
	if meta == nil {
		meta = make(map[string]string)
	}

	if isCentroid {
		meta["_type"] = "centroid"
		// Insert using the already-allocated arena offset (no double allocation).
		if err := he.Graph.InsertAt(doc.ID, doc.Vector, offset); err != nil {
			return fmt.Errorf("hnsw insert centroid: %w", err)
		}
	} else {
		meta["_type"] = "leaf"
		// Find nearest centroid via HNSW greedy descent
		centroidID := he.findNearestCentroid(doc.Vector)
		if centroidID != 0 || he.Graph.Len() > 0 {
			he.appendToInvertedFile(centroidID, doc.ID)
		}
	}

	// 3. Index in Bluge for BM25 + metadata filtering
	docIDStr := strconv.FormatUint(uint64(doc.ID), 10)
	if err := he.BlugeStore.IndexDocument(docIDStr, doc.Text, meta); err != nil {
		return fmt.Errorf("bluge index: %w", err)
	}

	return nil
}

// findNearestCentroid performs a full HNSW greedy descent to find the closest
// centroid for a leaf document.
func (he *HybridEngine) findNearestCentroid(vec []float32) uint32 {
	if he.Graph.Len() == 0 {
		return 0
	}
	results := he.Graph.Search(vec, 1, 10)
	if len(results) > 0 {
		return results[0].ID
	}
	return 0
}

// appendToInvertedFile appends a leaf doc ID to a centroid's posting list.
func (he *HybridEngine) appendToInvertedFile(centroidID, leafID uint32) {
	he.ifMu.Lock()
	he.invertedFiles[centroidID] = append(he.invertedFiles[centroidID], leafID)
	he.ifMu.Unlock()
}

// GetInvertedFile returns a copy of the posting list for a centroid
// (used by searcher). The copy avoids a data race with concurrent append.
func (he *HybridEngine) GetInvertedFile(centroidID uint32) []uint32 {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	list := he.invertedFiles[centroidID]
	if len(list) == 0 {
		return nil
	}
	return append([]uint32(nil), list...)
}

// InvertedFileStats returns centroid count and total leaf count (for testing).
func (he *HybridEngine) InvertedFileStats() (centroids int, leaves int) {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	centroids = len(he.invertedFiles)
	for _, list := range he.invertedFiles {
		leaves += len(list)
	}
	return
}

// Delete removes documents from the Bluge index and inverted files by their IDs.
// Note: HNSW graph nodes are not physically removed (tombstoned) — they are excluded
// from results because the Bluge pre-filter stage will no longer return them.
func (he *HybridEngine) Delete(ctx context.Context, docIDs []uint32) (int, error) {
	he.lifecycleMu.RLock()
	defer he.lifecycleMu.RUnlock()
	if he.closed.Load() {
		return 0, ErrEngineClosed
	}

	deleted := 0

	// Delete from Bluge
	for _, id := range docIDs {
		idStr := strconv.FormatUint(uint64(id), 10)
		if err := he.BlugeStore.DeleteDocument(idStr); err != nil {
			continue
		}
		deleted++
	}

	// Remove from inverted files
	idSet := make(map[uint32]struct{}, len(docIDs))
	for _, id := range docIDs {
		idSet[id] = struct{}{}
	}

	he.ifMu.Lock()
	for centroidID, leaves := range he.invertedFiles {
		filtered := leaves[:0]
		for _, leafID := range leaves {
			if _, del := idSet[leafID]; !del {
				filtered = append(filtered, leafID)
			}
		}
		he.invertedFiles[centroidID] = filtered
	}
	he.ifMu.Unlock()

	// Remove from vector offsets
	for _, id := range docIDs {
		he.VectorOffsets.Delete(id)
	}

	return deleted, nil
}

// DeleteByFilter removes documents matching the given metadata filters.
func (he *HybridEngine) DeleteByFilter(ctx context.Context, filters map[string]string) (int, error) {
	he.lifecycleMu.RLock()
	defer he.lifecycleMu.RUnlock()
	if he.closed.Load() {
		return 0, ErrEngineClosed
	}

	// Find matching doc IDs via Bluge search
	scores, err := he.BlugeStore.SearchWithScores(ctx, "", filters, 100000)
	if err != nil {
		return 0, fmt.Errorf("search for delete: %w", err)
	}

	docIDs := make([]uint32, 0, len(scores))
	for id := range scores {
		docIDs = append(docIDs, id)
	}

	if len(docIDs) == 0 {
		return 0, nil
	}

	return he.Delete(ctx, docIDs)
}

// Close releases all resources. It waits for in-flight operations to complete
// before unmapping the arena to avoid use-after-close crashes. Subsequent
// operations return ErrEngineClosed.
func (he *HybridEngine) Close() error {
	he.lifecycleMu.Lock()
	defer he.lifecycleMu.Unlock()

	he.closed.Store(true)
	blugeErr := he.BlugeStore.Close()
	arenaErr := he.Arena.Close()
	return errors.Join(blugeErr, arenaErr)
}
