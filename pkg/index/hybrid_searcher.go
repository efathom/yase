package index

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/vector"
)

// FlatSearchThreshold is the largest filter-match count for which the
// pre-filter stage switches from ANN traversal to an exact scan of every
// matching document. Restrictive filters (a small tenant, a single source)
// would otherwise starve ANN of eligible candidates; broad ones are cheaper
// to search approximately and post-check.
var FlatSearchThreshold = 20000

// probesFor returns how many centroids to probe. Recall depends on the
// fraction of centroids probed, so the default scales with the centroid
// count (a fixed cap of 100 probes fell to recall@10 ≈ 0.64 at 100k
// documents). Measured on clustered data: centroids/16 keeps recall@10
// ≥ 0.95 from 5k to 100k documents.
func probesFor(numCentroids, topK, override int) int {
	if override > 0 {
		return override
	}
	p := numCentroids / 16
	if p < topK*20 {
		p = topK * 20
	}
	if p < 64 {
		p = 64
	}
	if p > 4096 {
		p = 4096
	}
	if p > numCentroids {
		p = numCentroids
	}
	return p
}

// lexicalCandidates is how many top BM25 hits are injected into the candidate
// pool regardless of their vector distance, so strong keyword matches are
// never lost just because they are semantically far from the query.
func lexicalCandidates(topK, rerank int) int {
	n := topK * 10
	if n < 100 {
		n = 100
	}
	if rerank*2 > n {
		n = rerank * 2
	}
	return n
}

// HybridSearch executes the 5-stage hybrid search pipeline:
//  1. Pre-Filter: restrictive filters resolve to an exact allowed set
//     (flat scan); broad filters are applied as an O(candidates) post-check.
//  2. Candidate generation: HNSW centroid traversal + inverted-file leaves,
//     unioned with the top BM25 hits.
//  3. Exact rescoring: cosine similarity from the off-heap arena.
//  4. Reciprocal Rank Fusion of the lexical and semantic rankings.
//  5. Optional cross-encoder reranking.
func (he *HybridEngine) HybridSearch(
	ctx context.Context,
	textQuery string,
	queryVec []float32,
	metadataFilters map[string]string,
	topK int,
) ([]ScoredResult, error) {
	if err := he.acquire(); err != nil {
		return nil, err
	}
	defer he.release()

	if topK <= 0 {
		return nil, nil
	}
	if len(queryVec) != he.VecDim {
		return nil, fmt.Errorf("%w: query has %d dims, collection has %d", ErrDimensionMismatch, len(queryVec), he.VecDim)
	}
	q := vector.Normalized(queryVec)

	metrics.HNSWNodeCount.Set(float64(he.Graph.Len()))
	metrics.ArenaUsedBytes.Set(float64(he.Arena.UsedBytes()))
	stageStart := time.Now()
	observeStage := func(stage string) {
		metrics.SearchStageLatency.WithLabelValues(stage).Observe(time.Since(stageStart).Seconds())
		stageStart = time.Now()
	}

	// ── Stage 1: pre-filter ──
	filtered := len(metadataFilters) > 0
	var flatIDs []uint32
	useFlat := false
	if filtered {
		ids, overflow, err := he.BlugeStore.FilteredIDs(ctx, metadataFilters, FlatSearchThreshold)
		if err != nil {
			metrics.SearchErrorsTotal.WithLabelValues("search", "prefilter").Inc()
			return nil, err
		}
		if !overflow {
			if len(ids) == 0 {
				return nil, nil
			}
			flatIDs, useFlat = ids, true
		}
	}

	bm25Scores, err := he.BlugeStore.SearchWithScores(ctx, textQuery, metadataFilters,
		lexicalCandidates(topK, he.RerankerCandidates))
	if err != nil {
		metrics.SearchErrorsTotal.WithLabelValues("search", "bm25").Inc()
		return nil, err
	}
	observeStage("bm25_prefilter")

	// ── Stage 2: candidate generation ──
	cand := make(map[uint32]struct{}, len(bm25Scores)+256)
	if useFlat {
		for _, id := range flatIDs {
			cand[id] = struct{}{}
		}
	} else {
		he.annCandidates(q, topK, filtered, cand)
		if filtered {
			// Broad filter: keep only ANN candidates that satisfy it. BM25
			// hits were produced under the filter and need no check.
			ids := make([]uint32, 0, len(cand))
			for id := range cand {
				if _, lex := bm25Scores[id]; !lex {
					ids = append(ids, id)
				}
			}
			ok, err := he.BlugeStore.MatchIDs(ctx, ids, metadataFilters)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				if !ok[id] {
					delete(cand, id)
				}
			}
		}
	}
	for id := range bm25Scores {
		cand[id] = struct{}{}
	}
	observeStage("hnsw_traversal")

	// ── Stage 3: exact rescoring ──
	candidates := make([]ScoredResult, 0, len(cand))
	he.ifMu.RLock()
	for id := range cand {
		off, ok := he.offsets[id]
		if !ok {
			continue // deleted, or vector not yet rebuilt
		}
		v := he.Arena.GetFloat32(off, he.VecDim)
		candidates = append(candidates, ScoredResult{
			ID:            id,
			BM25Score:     bm25Scores[id],
			SemanticScore: vector.DotProductUnrolled(q, v),
		})
	}
	he.ifMu.RUnlock()
	if len(candidates) == 0 {
		return nil, nil
	}
	observeStage("exact_rescore")

	// ── Stage 4: Reciprocal Rank Fusion ──
	rrfTopN := topK
	if he.Reranker != nil && he.RerankerCandidates > topK {
		rrfTopN = he.RerankerCandidates
	}
	fused := ReciprocalRankFusion(candidates, rrfTopN)
	observeStage("rrf_fusion")

	// ── Stage 5: optional cross-encoder reranking ──
	if he.Reranker != nil && len(fused) > 0 {
		fused = he.rerank(ctx, textQuery, fused)
	}
	if len(fused) > topK {
		fused = fused[:topK]
	}
	observeStage("rerank")
	return fused, nil
}

// annCandidates adds centroid and inverted-file members near q to cand.
func (he *HybridEngine) annCandidates(q []float32, topK int, filtered bool, cand map[uint32]struct{}) {
	numCentroids := he.Graph.Len()
	if numCentroids == 0 {
		return
	}
	target := probesFor(numCentroids, topK, he.Probes)
	if filtered {
		target *= 2 // post-filtering discards some candidates; probe wider
	}
	ef := he.EfSearch
	if ef <= 0 {
		ef = 50
	}
	if ef < target {
		ef = target
	}
	nearest := he.Graph.Search(q, target, ef)

	he.ifMu.RLock()
	defer he.ifMu.RUnlock()
	for _, c := range nearest {
		cand[c.ID] = struct{}{}
		for _, leaf := range he.invertedFiles[c.ID] {
			cand[leaf] = struct{}{}
		}
	}
}

func (he *HybridEngine) rerank(ctx context.Context, query string, fused []ScoredResult) []ScoredResult {
	ids := make([]uint32, len(fused))
	for i, r := range fused {
		ids[i] = r.ID
	}
	docs, err := he.BlugeStore.GetDocumentsByIDs(ctx, ids)
	if err != nil {
		slog.Error("failed to fetch docs for reranking", "error", err)
		return fused
	}
	texts := make([]string, len(fused))
	for i, r := range fused {
		texts[i] = docs[r.ID].Content
	}
	reranked, err := he.Reranker.Rerank(ctx, query, texts)
	if err != nil {
		slog.Warn("reranker error, falling back to RRF", "error", err)
		return fused
	}
	out := make([]ScoredResult, 0, len(reranked))
	for _, rr := range reranked {
		if rr.Index >= 0 && rr.Index < len(fused) {
			out = append(out, fused[rr.Index])
		}
	}
	if len(out) == 0 {
		return fused
	}
	return out
}
