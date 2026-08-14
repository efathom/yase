package collection

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
)

// Searcher executes single-collection and cross-collection searches.
type Searcher struct {
	Manager *Manager
}

// NewSearcher creates a collection-aware searcher.
func NewSearcher(mgr *Manager) *Searcher {
	return &Searcher{Manager: mgr}
}

// SearchResult wraps an index.ScoredResult with the collection it came from.
type SearchResult struct {
	index.ScoredResult
	CollectionID string `json:"collection_id"`
}

// Search executes a hybrid search across one or more collections.
// If collectionIDs is empty, all collections are searched.
// Results are merged using Reciprocal Rank Fusion.
func (s *Searcher) Search(
	ctx context.Context,
	collectionIDs []string,
	textQuery string,
	queryVec []float32,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	engines := s.resolveEngines(collectionIDs)
	if len(engines) == 0 {
		return nil, fmt.Errorf("no collections found")
	}

	// Single collection: no fan-out needed
	if len(engines) == 1 {
		for colID, eng := range engines {
			results, err := eng.HybridSearch(ctx, textQuery, queryVec, filters, topK)
			if err != nil {
				return nil, fmt.Errorf("search collection %q: %w", colID, err)
			}
			return tagResults(results, colID), nil
		}
	}

	// Multi-collection: fan-out in parallel, merge via RRF
	type colResult struct {
		colID   string
		results []index.ScoredResult
		err     error
	}

	var wg sync.WaitGroup
	ch := make(chan colResult, len(engines))

	for colID, eng := range engines {
		wg.Add(1)
		go func(id string, e *index.HybridEngine) {
			defer wg.Done()
			results, err := e.HybridSearch(ctx, textQuery, queryVec, filters, topK)
			ch <- colResult{colID: id, results: results, err: err}
		}(colID, eng)
	}

	wg.Wait()
	close(ch)

	// Collect all results with collection tags
	var allResults []SearchResult
	for cr := range ch {
		if cr.err != nil {
			// Log but don't fail the entire search for one collection error
			continue
		}
		allResults = append(allResults, tagResults(cr.results, cr.colID)...)
	}

	if len(allResults) == 0 {
		return nil, nil
	}

	// Cross-collection RRF: re-fuse all candidates
	return crossCollectionRRF(allResults, topK), nil
}

// SearchSingle searches a single collection by ID.
func (s *Searcher) SearchSingle(
	ctx context.Context,
	collectionID string,
	textQuery string,
	queryVec []float32,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	return s.Search(ctx, []string{collectionID}, textQuery, queryVec, filters, topK)
}

// SearchDefault searches the _default collection.
func (s *Searcher) SearchDefault(
	ctx context.Context,
	textQuery string,
	queryVec []float32,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	return s.Search(ctx, []string{DefaultCollectionID}, textQuery, queryVec, filters, topK)
}

// resolveEngines returns the engines for the given collection IDs.
// If the list is empty, returns all engines.
func (s *Searcher) resolveEngines(collectionIDs []string) map[string]*index.HybridEngine {
	if len(collectionIDs) == 0 {
		return s.Manager.AllEngines()
	}

	result := make(map[string]*index.HybridEngine, len(collectionIDs))
	for _, id := range collectionIDs {
		if eng, err := s.Manager.GetEngine(id); err == nil {
			result[id] = eng
		}
	}
	return result
}

// GetEmbedder returns the appropriate embedder for a collection search.
// For cross-collection searches, returns the _default collection's embedder.
func (s *Searcher) GetEmbedder(collectionID string) (embedder.Embedder, error) {
	if collectionID == "" {
		collectionID = DefaultCollectionID
	}
	return s.Manager.GetEmbedder(collectionID)
}

func tagResults(results []index.ScoredResult, collectionID string) []SearchResult {
	tagged := make([]SearchResult, len(results))
	for i, r := range results {
		tagged[i] = SearchResult{
			ScoredResult: r,
			CollectionID: collectionID,
		}
	}
	return tagged
}

// crossCollectionRRF applies Reciprocal Rank Fusion across results from
// multiple collections. Each collection's results are already RRF-fused
// internally; this second pass fuses across collections by their FusedScore rank.
func crossCollectionRRF(results []SearchResult, topK int) []SearchResult {
	const k = 60.0

	// Rank by FusedScore (descending) — this is each result's intra-collection rank
	sort.Slice(results, func(i, j int) bool {
		return results[i].FusedScore > results[j].FusedScore
	})

	// Assign cross-collection RRF score based on position in merged list
	for i := range results {
		results[i].FusedScore = 1.0 / (k + float64(i+1))
	}

	// Re-sort by new RRF score (already sorted since rank-based, but stable sort for ties)
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].FusedScore > results[j].FusedScore
	})

	if len(results) > topK {
		results = results[:topK]
	}
	return results
}
