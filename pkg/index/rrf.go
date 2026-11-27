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

// PooledRank is one entry of FusePooled's output.
type PooledRank struct {
	Index int     // position in the input slice
	Score float64 // fused score
}

// FusePooled re-ranks results gathered from several disjoint sources (shards
// or collections searched with the same embedder) as if they came from one
// index: documents are ranked globally by BM25 (only those that matched the
// text) and by semantic similarity, and the ranks are fused with RRF.
//
// Unlike re-fusing each source's own rank positions — which interleaves the
// sources so every source's #1 ties regardless of quality — this compares the
// underlying scores directly. Entries are identified by position, so equal
// IDs from different sources never collide. It returns at most topK entries
// (all if topK <= 0), best first.
func FusePooled(results []ScoredResult, topK int) []PooledRank {
	const kRRF = 60.0
	n := len(results)
	if n == 0 {
		return nil
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	scores := make([]float64, n)

	sort.SliceStable(idx, func(a, b int) bool { return results[idx[a]].BM25Score > results[idx[b]].BM25Score })
	for rank, i := range idx {
		if results[i].BM25Score <= 0 {
			break
		}
		scores[i] += 1.0 / (kRRF + float64(rank+1))
	}
	sort.SliceStable(idx, func(a, b int) bool { return results[idx[a]].SemanticScore > results[idx[b]].SemanticScore })
	for rank, i := range idx {
		scores[i] += 1.0 / (kRRF + float64(rank+1))
	}
	sort.SliceStable(idx, func(a, b int) bool { return scores[idx[a]] > scores[idx[b]] })
	if topK > 0 && len(idx) > topK {
		idx = idx[:topK]
	}
	out := make([]PooledRank, len(idx))
	for i, j := range idx {
		out[i] = PooledRank{Index: j, Score: scores[j]}
	}
	return out
}
