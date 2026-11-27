package collection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// QueryEmbedder returns the query vector to use for a collection, given that
// collection's embedder. Implementations should memoize per embedder so a
// cross-collection search embeds the query once per distinct model.
type QueryEmbedder func(ctx context.Context, collectionID string, emb embedder.Embedder) ([]float32, error)

// Search executes a hybrid search across one or more collections using the
// same query vector for all of them. Prefer SearchEmbedded when collections
// may use different embedders.
func (s *Searcher) Search(
	ctx context.Context,
	callerTenant string,
	collectionIDs []string,
	textQuery string,
	queryVec []float32,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	return s.SearchEmbedded(ctx, callerTenant, collectionIDs, textQuery,
		func(context.Context, string, embedder.Embedder) ([]float32, error) { return queryVec, nil },
		filters, topK)
}

// SearchEmbedded executes a hybrid search across one or more collections
// (all the caller may access when collectionIDs is empty). Each collection is
// queried with a vector from its own embedder, so collections with different
// models or dimensions can be searched together.
func (s *Searcher) SearchEmbedded(
	ctx context.Context,
	callerTenant string,
	collectionIDs []string,
	textQuery string,
	embed QueryEmbedder,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	engines := s.resolveEngines(callerTenant, collectionIDs)
	if len(engines) == 0 {
		return nil, fmt.Errorf("%w: no accessible collections", ErrNotFound)
	}

	searchOne := func(colID string, eng *index.HybridEngine) ([]index.ScoredResult, error) {
		emb, err := s.Manager.GetEmbedder(colID)
		if err != nil {
			return nil, err
		}
		vec, err := embed(ctx, colID, emb)
		if err != nil {
			return nil, fmt.Errorf("embed query: %w", err)
		}
		return eng.HybridSearch(ctx, textQuery, vec, filters, topK)
	}

	if len(engines) == 1 {
		for colID, eng := range engines {
			results, err := searchOne(colID, eng)
			if err != nil {
				return nil, fmt.Errorf("search collection %q: %w", colID, err)
			}
			return tagResults(results, colID), nil
		}
	}

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
			results, err := searchOne(id, e)
			ch <- colResult{colID: id, results: results, err: err}
		}(colID, eng)
	}
	wg.Wait()
	close(ch)

	var allResults []SearchResult
	var errs []error
	for cr := range ch {
		if cr.err != nil {
			// One failing collection degrades, rather than fails, the search.
			slog.Warn("cross-collection search: collection failed", "collection", cr.colID, "error", cr.err)
			errs = append(errs, fmt.Errorf("%s: %w", cr.colID, cr.err))
			continue
		}
		allResults = append(allResults, tagResults(cr.results, cr.colID)...)
	}
	if len(allResults) == 0 {
		if len(errs) > 0 {
			return nil, fmt.Errorf("all collections failed: %w", errors.Join(errs...))
		}
		return nil, nil
	}

	ids := make([]string, 0, len(engines))
	for id := range engines {
		ids = append(ids, id)
	}
	if s.Manager.SameEmbedder(ids) {
		return pooledFusion(allResults, topK), nil
	}
	return crossCollectionRRF(allResults, topK), nil
}

// pooledFusion ranks results from collections that share an embedder as one
// index (their cosine similarities are directly comparable).
func pooledFusion(results []SearchResult, topK int) []SearchResult {
	scored := make([]index.ScoredResult, len(results))
	for i, r := range results {
		scored[i] = r.ScoredResult
	}
	order := index.FusePooled(scored, topK)
	out := make([]SearchResult, len(order))
	for i, o := range order {
		out[i] = results[o.Index]
		out[i].FusedScore = o.Score
	}
	return out
}

// SearchSingle searches a single collection by ID.
func (s *Searcher) SearchSingle(
	ctx context.Context,
	callerTenant string,
	collectionID string,
	textQuery string,
	queryVec []float32,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	return s.Search(ctx, callerTenant, []string{collectionID}, textQuery, queryVec, filters, topK)
}

// SearchDefault searches the _default collection.
func (s *Searcher) SearchDefault(
	ctx context.Context,
	callerTenant string,
	textQuery string,
	queryVec []float32,
	filters map[string]string,
	topK int,
) ([]SearchResult, error) {
	return s.Search(ctx, callerTenant, []string{DefaultCollectionID}, textQuery, queryVec, filters, topK)
}

// resolveEngines returns the engines the caller's tenant may search.
// If the list is empty, returns every engine that tenant owns.
func (s *Searcher) resolveEngines(callerTenant string, collectionIDs []string) map[string]*index.HybridEngine {
	return s.Manager.EnginesForTenant(callerTenant, collectionIDs)
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

// crossCollectionRRF merges results from collections whose scores are not
// comparable (different embedders): each result scores 1/(k + its rank within
// its own collection), with the collection-local fused score breaking ties.
func crossCollectionRRF(results []SearchResult, topK int) []SearchResult {
	const k = 60.0
	byCol := make(map[string][]int)
	for i, r := range results {
		byCol[r.CollectionID] = append(byCol[r.CollectionID], i)
	}
	local := make([]float64, len(results))
	for _, idx := range byCol {
		sort.SliceStable(idx, func(a, b int) bool { return results[idx[a]].FusedScore > results[idx[b]].FusedScore })
		for rank, i := range idx {
			local[i] = results[i].FusedScore
			results[i].FusedScore = 1.0 / (k + float64(rank+1))
		}
	}
	order := make([]int, len(results))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		ra, rb := results[order[a]].FusedScore, results[order[b]].FusedScore
		if ra != rb {
			return ra > rb
		}
		return local[order[a]] > local[order[b]]
	})
	if len(order) > topK {
		order = order[:topK]
	}
	out := make([]SearchResult, len(order))
	for i, j := range order {
		out[i] = results[j]
	}
	return out
}

// NewQueryEmbedder returns a QueryEmbedder that embeds query once per
// distinct embedder. If given is non-empty it is used for every collection
// whose embedder produces vectors of that length.
func NewQueryEmbedder(query string, given []float32) QueryEmbedder {
	var mu sync.Mutex
	cache := make(map[embedder.Embedder][]float32)
	return func(ctx context.Context, _ string, emb embedder.Embedder) ([]float32, error) {
		if len(given) > 0 && (emb.Dimension() == 0 || emb.Dimension() == len(given)) {
			return given, nil
		}
		mu.Lock()
		defer mu.Unlock()
		if v, ok := cache[emb]; ok {
			return v, nil
		}
		v, err := emb.Embed(ctx, query)
		if err != nil {
			return nil, err
		}
		cache[emb] = v
		return v, nil
	}
}
