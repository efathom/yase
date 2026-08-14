package query

import (
	"sort"

	"github.com/efathom/yase/pkg/index"
)

const rrfK = 60.0

// MergeRRF fuses multiple shard-local ranked lists using Reciprocal Rank Fusion.
// Each shard returns results ranked by local RRF score. The merger re-ranks
// globally: a document appearing high in multiple shards is boosted.
func MergeRRF(shardResults [][]index.ScoredResult, topK int) []index.ScoredResult {
	if len(shardResults) == 0 {
		return nil
	}

	scores := make(map[uint32]float64)
	meta := make(map[uint32]index.ScoredResult) // keep best BM25/semantic scores

	for _, results := range shardResults {
		for rank, r := range results {
			scores[r.ID] += 1.0 / (rrfK + float64(rank+1))
			// Keep the highest individual scores across shards
			if existing, ok := meta[r.ID]; !ok || r.BM25Score > existing.BM25Score {
				meta[r.ID] = r
			}
		}
	}

	type scored struct {
		id    uint32
		score float64
	}
	all := make([]scored, 0, len(scores))
	for id, score := range scores {
		all = append(all, scored{id, score})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].score > all[j].score })

	if topK > 0 && len(all) > topK {
		all = all[:topK]
	}

	results := make([]index.ScoredResult, len(all))
	for i, s := range all {
		r := meta[s.id]
		results[i] = index.ScoredResult{
			ID:            s.id,
			FusedScore:    s.score,
			BM25Score:     r.BM25Score,
			SemanticScore: r.SemanticScore,
		}
	}
	return results
}
