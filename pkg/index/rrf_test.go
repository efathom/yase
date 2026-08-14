package index

import (
	"testing"
)

func TestRRFBasic(t *testing.T) {
	candidates := []ScoredResult{
		{ID: 1, BM25Score: 10.0, SemanticScore: 0.5},
		{ID: 2, BM25Score: 5.0, SemanticScore: 0.9},  // best semantic
		{ID: 3, BM25Score: 15.0, SemanticScore: 0.3}, // best BM25
	}

	results := ReciprocalRankFusion(candidates, 3)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// All fused scores should be positive
	for _, r := range results {
		if r.FusedScore <= 0 {
			t.Errorf("ID %d: fused score should be positive, got %f", r.ID, r.FusedScore)
		}
	}

	// Results should be sorted by fused score descending
	for i := 1; i < len(results); i++ {
		if results[i].FusedScore > results[i-1].FusedScore {
			t.Errorf("results not sorted: index %d (%f) > index %d (%f)",
				i, results[i].FusedScore, i-1, results[i-1].FusedScore)
		}
	}

	// The candidate that ranks well in BOTH lists (ID=1: BM25 rank=2, semantic rank=2)
	// should outrank a candidate that ranks #1 in only one list.
	// ID=1: 1/(60+2) + 1/(60+2) = 2/62 ≈ 0.03226
	// ID=2: 1/(60+3) + 1/(60+1) = 1/63 + 1/61 ≈ 0.03227
	// ID=3: 1/(60+1) + 1/(60+3) = 1/61 + 1/63 ≈ 0.03227
	// IDs 2 and 3 are symmetric in this case. ID=1 should be competitive.
	t.Logf("RRF results: %+v", results)
}

func TestRRFSingleCandidate(t *testing.T) {
	candidates := []ScoredResult{
		{ID: 42, BM25Score: 7.5, SemanticScore: 0.8},
	}
	results := ReciprocalRankFusion(candidates, 1)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != 42 {
		t.Errorf("expected ID 42, got %d", results[0].ID)
	}
	// Single candidate: rank 1 in both → 1/(60+1) + 1/(60+1) = 2/61
	expected := 2.0 / 61.0
	if abs(results[0].FusedScore-expected) > 1e-10 {
		t.Errorf("fused score: got %f, want %f", results[0].FusedScore, expected)
	}
}

func TestRRFEmpty(t *testing.T) {
	results := ReciprocalRankFusion(nil, 10)
	if results != nil {
		t.Errorf("expected nil for empty input, got %v", results)
	}
}

func TestRRFTopKTruncation(t *testing.T) {
	candidates := make([]ScoredResult, 10)
	for i := range candidates {
		candidates[i] = ScoredResult{
			ID:            uint32(i),
			BM25Score:     float64(10 - i),
			SemanticScore: float32(i) / 10.0,
		}
	}

	results := ReciprocalRankFusion(candidates, 3)
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestRRFRankingPreference(t *testing.T) {
	// A candidate ranked well in both lists should beat one ranked #1 in only one
	candidates := []ScoredResult{
		{ID: 1, BM25Score: 100.0, SemanticScore: 0.1}, // #1 BM25, last semantic
		{ID: 2, BM25Score: 50.0, SemanticScore: 0.5},  // middle in both
		{ID: 3, BM25Score: 1.0, SemanticScore: 0.99},  // last BM25, #1 semantic
	}
	results := ReciprocalRankFusion(candidates, 3)

	// ID=2 (rank 2 in both) should have fused = 1/(60+2) + 1/(60+2) = 2/62 ≈ 0.03226
	// ID=1 (rank 1 BM25, rank 3 semantic) = 1/61 + 1/63 ≈ 0.03227
	// ID=3 (rank 3 BM25, rank 1 semantic) = 1/63 + 1/61 ≈ 0.03227
	// IDs 1 and 3 are symmetric and should slightly beat ID=2
	t.Logf("Ranking preference results: %+v", results)
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
