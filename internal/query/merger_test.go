package query

import (
	"testing"

	"github.com/efathom/yase/pkg/index"
)

func TestMergeRRFSingleShard(t *testing.T) {
	results := [][]index.ScoredResult{
		{
			{ID: 1, BM25Score: 10, SemanticScore: 0.9, FusedScore: 0.5},
			{ID: 2, BM25Score: 8, SemanticScore: 0.7, FusedScore: 0.4},
			{ID: 3, BM25Score: 5, SemanticScore: 0.5, FusedScore: 0.3},
		},
	}

	merged := MergeRRF(results, 2)
	if len(merged) != 2 {
		t.Fatalf("expected 2 results, got %d", len(merged))
	}
	// First result should be ID=1 (rank 1)
	if merged[0].ID != 1 {
		t.Errorf("first result: got ID=%d, want 1", merged[0].ID)
	}
	if merged[1].ID != 2 {
		t.Errorf("second result: got ID=%d, want 2", merged[1].ID)
	}
}

func TestMergeRRFMultiShardBoost(t *testing.T) {
	// Doc 5 appears in both shards → should be boosted
	shard1 := []index.ScoredResult{
		{ID: 1, BM25Score: 10},
		{ID: 5, BM25Score: 8},
		{ID: 3, BM25Score: 5},
	}
	shard2 := []index.ScoredResult{
		{ID: 5, BM25Score: 9}, // same doc, high rank in both
		{ID: 7, BM25Score: 6},
		{ID: 8, BM25Score: 4},
	}

	merged := MergeRRF([][]index.ScoredResult{shard1, shard2}, 5)

	// Doc 5 should be #1 (appears rank 2 in shard1 + rank 1 in shard2)
	if merged[0].ID != 5 {
		t.Errorf("expected doc 5 to be #1 (multi-shard boost), got ID=%d", merged[0].ID)
	}

	t.Logf("Merged ranking:")
	for i, r := range merged {
		t.Logf("  #%d: ID=%d, fused=%.6f", i+1, r.ID, r.FusedScore)
	}
}

func TestMergeRRFEmpty(t *testing.T) {
	merged := MergeRRF(nil, 10)
	if merged != nil {
		t.Errorf("expected nil, got %v", merged)
	}

	merged = MergeRRF([][]index.ScoredResult{}, 10)
	if merged != nil {
		t.Errorf("expected nil, got %v", merged)
	}
}

func TestMergeRRFTopKTruncation(t *testing.T) {
	results := [][]index.ScoredResult{
		{
			{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5},
		},
	}

	merged := MergeRRF(results, 3)
	if len(merged) != 3 {
		t.Errorf("expected 3 results, got %d", len(merged))
	}
}

func TestMergeRRFPreservesBestScores(t *testing.T) {
	shard1 := []index.ScoredResult{
		{ID: 1, BM25Score: 10, SemanticScore: 0.8},
	}
	shard2 := []index.ScoredResult{
		{ID: 1, BM25Score: 15, SemanticScore: 0.6},
	}

	merged := MergeRRF([][]index.ScoredResult{shard1, shard2}, 1)
	if len(merged) != 1 {
		t.Fatalf("expected 1 result, got %d", len(merged))
	}
	// Should keep the higher BM25 score
	if merged[0].BM25Score != 15 {
		t.Errorf("BM25Score: got %f, want 15", merged[0].BM25Score)
	}
}
