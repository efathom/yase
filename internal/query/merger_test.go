package query

import (
	"testing"

	"github.com/efathom/yase/pkg/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeShards(t *testing.T) {
	tests := []struct {
		name   string
		shards []ShardResults
		topK   int
		want   []Result // ShardID + ID in order
	}{
		{"empty", nil, 10, nil},
		{
			name: "single shard keeps its order",
			shards: []ShardResults{{ShardID: 0, Results: []index.ScoredResult{
				{ID: 1, BM25Score: 10, SemanticScore: 0.9}, {ID: 2, BM25Score: 8, SemanticScore: 0.7}, {ID: 3, BM25Score: 5, SemanticScore: 0.5}}}},
			topK: 2,
			want: []Result{{ShardID: 0, ScoredResult: index.ScoredResult{ID: 1}}, {ShardID: 0, ScoredResult: index.ScoredResult{ID: 2}}},
		},
		{
			// Shard 1's best is weak; shard 0's second is strong. Local-rank
			// fusion would rank shard 1's #1 second; global scores do not.
			name: "global scores beat local rank",
			shards: []ShardResults{
				{ShardID: 0, Results: []index.ScoredResult{{ID: 1, BM25Score: 9, SemanticScore: 0.95}, {ID: 2, BM25Score: 8, SemanticScore: 0.9}}},
				{ShardID: 1, Results: []index.ScoredResult{{ID: 7, SemanticScore: 0.1}}},
			},
			topK: 3,
			want: []Result{{ShardID: 0, ScoredResult: index.ScoredResult{ID: 1}}, {ShardID: 0, ScoredResult: index.ScoredResult{ID: 2}}, {ShardID: 1, ScoredResult: index.ScoredResult{ID: 7}}},
		},
		{
			name: "same ID on different shards are different documents",
			shards: []ShardResults{
				{ShardID: 0, Results: []index.ScoredResult{{ID: 1, SemanticScore: 0.9}}},
				{ShardID: 1, Results: []index.ScoredResult{{ID: 1, SemanticScore: 0.8}}},
			},
			topK: 10,
			want: []Result{{ShardID: 0, ScoredResult: index.ScoredResult{ID: 1}}, {ShardID: 1, ScoredResult: index.ScoredResult{ID: 1}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeShards(tt.shards, tt.topK)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				assert.Equal(t, tt.want[i].ShardID, got[i].ShardID, "rank %d shard", i)
				assert.Equal(t, tt.want[i].ID, got[i].ID, "rank %d id", i)
			}
			for i := 1; i < len(got); i++ {
				assert.GreaterOrEqual(t, got[i-1].FusedScore, got[i].FusedScore)
			}
		})
	}
}
