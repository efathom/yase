package index

import "sort"

// ScoredResult holds both BM25 and semantic scores for a document,
// plus the fused RRF score after rank fusion.
type ScoredResult struct {
	ID            uint32
	BM25Score     float64
	SemanticScore float32
	FusedScore    float64
}

// ReciprocalRankFusion merges unbounded BM25 scores with bounded cosine
// similarity scores using rank-based fusion:
//
//	FusedScore(d) = 1/(k + BM25_rank) + 1/(k + Semantic_rank)
//
// k=60 is the standard hyperparameter that prevents low-rank documents from
// dominating. RRF is scale-independent — it merges by rank order, not raw scores.
func ReciprocalRankFusion(candidates []ScoredResult, topK int) []ScoredResult {
	if len(candidates) == 0 {
		return nil
	}

	const kRRF = 60.0

	// 1. Rank by BM25 score descending → assign lexical ranks (1-indexed)
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].BM25Score > candidates[j].BM25Score
	})
	bm25Ranks := make(map[uint32]int, len(candidates))
	for i, c := range candidates {
		bm25Ranks[c.ID] = i + 1
	}

	// 2. Rank by semantic score descending → assign semantic ranks (1-indexed)
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].SemanticScore > candidates[j].SemanticScore
	})

	// 3. Compute fused score per candidate
	for i, c := range candidates {
		semanticRank := i + 1
		lexicalRank := bm25Ranks[c.ID]
		candidates[i].FusedScore = 1.0/(kRRF+float64(lexicalRank)) + 1.0/(kRRF+float64(semanticRank))
	}

	// 4. Sort by fused score descending
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].FusedScore > candidates[j].FusedScore
	})

	if len(candidates) > topK {
		return candidates[:topK]
	}
	return candidates
}
