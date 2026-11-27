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
// Only documents that actually matched the text query (BM25Score > 0) get a
// lexical rank; the rest receive no lexical contribution rather than an
// arbitrary rank among ties. k=60 is the standard hyperparameter. RRF is
// scale-independent — it merges by rank order, not raw scores.
func ReciprocalRankFusion(candidates []ScoredResult, topK int) []ScoredResult {
	if len(candidates) == 0 {
		return nil
	}

	const kRRF = 60.0

	// 1. Lexical ranks for documents that matched the text query.
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].BM25Score > candidates[j].BM25Score
	})
	bm25Ranks := make(map[uint32]int, len(candidates))
	for i, c := range candidates {
		if c.BM25Score <= 0 {
			break
		}
		bm25Ranks[c.ID] = i + 1
	}

	// 2. Semantic ranks.
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].SemanticScore > candidates[j].SemanticScore
	})

	// 3. Fused score.
	for i, c := range candidates {
		score := 1.0 / (kRRF + float64(i+1))
		if r, ok := bm25Ranks[c.ID]; ok {
			score += 1.0 / (kRRF + float64(r))
		}
		candidates[i].FusedScore = score
	}

	// 4. Sort by fused score descending.
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].FusedScore > candidates[j].FusedScore
	})

	if topK > 0 && len(candidates) > topK {
		return candidates[:topK]
	}
	return candidates
}
