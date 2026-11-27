package query

import (
	"github.com/efathom/yase/pkg/index"
)

// ShardResults is one shard's ranked results.
type ShardResults struct {
	ShardID uint32
	Results []index.ScoredResult
}

// Result is a merged result tagged with the shard that holds the document.
// Document IDs are shard-local, so (ShardID, ID) identifies a document.
type Result struct {
	index.ScoredResult
	ShardID uint32 `json:"shard_id"`
}

// MergeShards fuses shard-local results into one global ranking.
//
// Shards hold disjoint documents embedded with the same model, so their raw
// cosine similarities are directly comparable and BM25 is approximately so.
// Results are therefore pooled and ranked globally on those scores (RRF over
// the pooled BM25 and semantic rankings) rather than by each shard's local
// rank — which would place every shard's #1 on equal footing regardless of
// how good it is.
func MergeShards(shards []ShardResults, topK int) []Result {
	var pooled []Result
	for _, s := range shards {
		for _, r := range s.Results {
			pooled = append(pooled, Result{ScoredResult: r, ShardID: s.ShardID})
		}
	}
	if len(pooled) == 0 {
		return nil
	}
	scored := make([]index.ScoredResult, len(pooled))
	for i, p := range pooled {
		scored[i] = p.ScoredResult
	}
	order := index.FusePooled(scored, topK)
	out := make([]Result, len(order))
	for i, o := range order {
		out[i] = pooled[o.Index]
		out[i].FusedScore = o.Score
	}
	return out
}
