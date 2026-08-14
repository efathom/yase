package index

import (
	"context"
	"log/slog"
	"time"

	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/vector"
)

// HybridSearch executes the full 5-stage hybrid search pipeline:
//  1. Pre-Filter: Bluge Roaring Bitmap metadata filtering + BM25 scoring
//  2. ANN Centroid Traversal: HNSW greedy descent to find top centroids
//  3. Targeted Exact Rescoring: load vectors from arena, compute cosine similarity
//  4. Reciprocal Rank Fusion: merge BM25 and semantic ranks
//  5. Cross-Encoder Reranking: optional TEI reranker for final precision (Stage 5)
func (he *HybridEngine) HybridSearch(
	ctx context.Context,
	textQuery string,
	queryVec []float32,
	metadataFilters map[string]string,
	topK int,
) ([]ScoredResult, error) {
	he.lifecycleMu.RLock()
	defer he.lifecycleMu.RUnlock()
	if he.closed.Load() {
		return nil, ErrEngineClosed
	}

	if topK <= 0 {
		return nil, nil
	}

	// Observability: export index/arena sizes and time the pipeline stages.
	metrics.HNSWNodeCount.Set(float64(he.Graph.Len()))
	metrics.ArenaUsedBytes.Set(float64(he.Arena.UsedBytes()))
	stageStart := time.Now()
	observeStage := func(stage string) {
		metrics.SearchStageLatency.WithLabelValues(stage).Observe(time.Since(stageStart).Seconds())
		stageStart = time.Now()
	}

	// ── Stage 1: Pre-Filter (Bluge Roaring Bitmaps) ──
	// Build the "allowed" set with a filter-only query so BM25 ranking cannot
	// drop semantically relevant documents. BM25 scores are computed separately
	// for ranking (RRF), with 0 for documents the text query did not score.
	numCentroids := he.Graph.Len()
	if numCentroids == 0 {
		return nil, nil
	}

	// Search more centroids on larger corpora, bounded for latency.
	targetCentroids := numCentroids / 10
	if targetCentroids < 5 {
		targetCentroids = 5
	}
	if targetCentroids > 100 {
		targetCentroids = 100
	}

	preFilterLimit := targetCentroids * he.CentroidRate * 100
	if preFilterLimit < 10000 {
		preFilterLimit = 10000
	}
	if preFilterLimit > 1000000 {
		preFilterLimit = 1000000
	}

	allowed, err := he.BlugeStore.SearchFiltered(ctx, metadataFilters, preFilterLimit)
	if err != nil {
		return nil, err
	}
	if len(allowed) == 0 {
		return nil, nil // short-circuit: no documents passed filters
	}

	bm25Scores, err := he.BlugeStore.SearchWithScores(ctx, textQuery, metadataFilters, preFilterLimit)
	if err != nil {
		metrics.SearchErrorsTotal.WithLabelValues("search", "bm25").Inc()
		return nil, err
	}
	observeStage("bm25_prefilter")

	// ── Stage 2: ANN Centroid Traversal ──
	// Full HNSW greedy descent returning top closest centroids.
	efSearch := he.EfSearch
	if efSearch <= 0 {
		efSearch = 50
	}
	nearestCentroids := he.Graph.Search(queryVec, targetCentroids, efSearch)
	observeStage("hnsw_traversal")

	// ── Stage 3: Targeted Exact Rescoring ──
	// For each centroid cluster, rescore only the allowed leaf documents.
	var candidates []ScoredResult
	seen := make(map[uint32]struct{}, len(nearestCentroids)*100)

	// Also check the centroids themselves
	for _, centroid := range nearestCentroids {
		if allowed[centroid.ID] {
			if _, dup := seen[centroid.ID]; dup {
				continue
			}
			seen[centroid.ID] = struct{}{}
			offsetVal, ok := he.VectorOffsets.Load(centroid.ID)
			if ok {
				cv := he.Arena.GetFloat32(offsetVal.(uint64), he.VecDim)
				semScore := vector.CosineSimilarity(queryVec, cv)
				candidates = append(candidates, ScoredResult{
					ID:            centroid.ID,
					BM25Score:     bm25Scores[centroid.ID],
					SemanticScore: semScore,
				})
			}
		}

		// Iterate the centroid's inverted file posting list
		clusterMembers := he.GetInvertedFile(centroid.ID)
		if len(clusterMembers) == 0 {
			continue
		}

		for _, leafID := range clusterMembers {
			if _, dup := seen[leafID]; dup {
				continue
			}
			seen[leafID] = struct{}{}
			// O(1) pre-filter check — skip unauthorized documents
			if !allowed[leafID] {
				continue
			}

			// Load raw vector from off-heap arena
			offsetVal, ok := he.VectorOffsets.Load(leafID)
			if !ok {
				continue
			}
			rawVec := he.Arena.GetFloat32(offsetVal.(uint64), he.VecDim)

			// Compute cosine similarity
			semScore := vector.CosineSimilarity(queryVec, rawVec)

			candidates = append(candidates, ScoredResult{
				ID:            leafID,
				BM25Score:     bm25Scores[leafID],
				SemanticScore: semScore,
			})
		}
	}

	if len(candidates) == 0 {
		return nil, nil
	}
	observeStage("exact_rescore")

	// ── Stage 4: Reciprocal Rank Fusion ──
	// When reranker is active, retrieve more candidates from RRF for reranking to select from.
	rrfTopN := topK
	if he.Reranker != nil && he.RerankerCandidates > topK {
		rrfTopN = he.RerankerCandidates
	}
	fused := ReciprocalRankFusion(candidates, rrfTopN)
	observeStage("rrf_fusion")

	// ── Stage 5: Optional Cross-Encoder Reranking ──
	if he.Reranker != nil && len(fused) > 0 {
		ids := make([]uint32, len(fused))
		for i, r := range fused {
			ids[i] = r.ID
		}
		docs, err := he.BlugeStore.GetDocumentsByIDs(ctx, ids)
		if err != nil {
			slog.Error("failed to fetch docs for reranking", "error", err)
		} else {
			texts := make([]string, len(fused))
			for i, r := range fused {
				if hit, ok := docs[r.ID]; ok {
					texts[i] = hit.Content
				}
			}

			reranked, err := he.Reranker.Rerank(ctx, textQuery, texts)
			if err != nil {
				slog.Warn("reranker error, falling back to RRF", "error", err)
			} else {
				reordered := make([]ScoredResult, len(reranked))
				for i, rr := range reranked {
					reordered[i] = fused[rr.Index]
				}
				fused = reordered
			}
		}
	}

	if len(fused) > topK {
		fused = fused[:topK]
	}
	observeStage("rerank")
	return fused, nil
}
