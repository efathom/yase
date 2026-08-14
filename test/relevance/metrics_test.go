package relevance

import (
	"math"
	"testing"
)

func TestRecallAtK(t *testing.T) {
	results := []Result{
		{DocID: 1, Relevant: true},
		{DocID: 2, Relevant: false},
		{DocID: 3, Relevant: true},
		{DocID: 4, Relevant: false},
		{DocID: 5, Relevant: true},
	}

	recall := RecallAtK(results, 3, 4) // 2 of 4 relevant found in top 3
	if math.Abs(recall-0.5) > 0.01 {
		t.Errorf("Recall@3: got %.2f, want 0.50", recall)
	}

	recall5 := RecallAtK(results, 5, 4) // 3 of 4 relevant found in top 5
	if math.Abs(recall5-0.75) > 0.01 {
		t.Errorf("Recall@5: got %.2f, want 0.75", recall5)
	}
}

func TestPrecisionAtK(t *testing.T) {
	results := []Result{
		{DocID: 1, Relevant: true},
		{DocID: 2, Relevant: true},
		{DocID: 3, Relevant: false},
	}

	prec := PrecisionAtK(results, 3)
	if math.Abs(prec-0.6667) > 0.01 {
		t.Errorf("P@3: got %.4f, want 0.6667", prec)
	}
}

func TestMRR(t *testing.T) {
	tests := []struct {
		name    string
		results []Result
		want    float64
	}{
		{"first is relevant", []Result{{Relevant: true}, {Relevant: false}}, 1.0},
		{"second is relevant", []Result{{Relevant: false}, {Relevant: true}}, 0.5},
		{"third is relevant", []Result{{Relevant: false}, {Relevant: false}, {Relevant: true}}, 1.0 / 3},
		{"none relevant", []Result{{Relevant: false}, {Relevant: false}}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MRR(tt.results)
			if math.Abs(got-tt.want) > 0.01 {
				t.Errorf("MRR: got %.4f, want %.4f", got, tt.want)
			}
		})
	}
}

func TestNDCG(t *testing.T) {
	// Perfect ranking: grades 3, 2, 1, 0
	perfect := []Result{
		{GradeInt: 3}, {GradeInt: 2}, {GradeInt: 1}, {GradeInt: 0},
	}
	ndcg := NDCG(perfect, 4)
	if math.Abs(ndcg-1.0) > 0.01 {
		t.Errorf("perfect NDCG@4: got %.4f, want 1.0", ndcg)
	}

	// Reversed ranking
	reversed := []Result{
		{GradeInt: 0}, {GradeInt: 1}, {GradeInt: 2}, {GradeInt: 3},
	}
	ndcgReversed := NDCG(reversed, 4)
	if ndcgReversed >= 1.0 {
		t.Errorf("reversed NDCG should be < 1.0, got %.4f", ndcgReversed)
	}
	t.Logf("Reversed NDCG@4: %.4f", ndcgReversed)
}

func TestEvaluate(t *testing.T) {
	q1 := []Result{{Relevant: true}, {Relevant: false}, {Relevant: true}}
	q2 := []Result{{Relevant: false}, {Relevant: true}, {Relevant: false}}

	report := Evaluate([][]Result{q1, q2}, []int{3, 2})

	t.Logf("Report: Recall@10=%.2f P@10=%.2f MRR=%.2f nDCG@10=%.2f MAP=%.2f",
		report.MeanRecallAt10, report.MeanPrecisionAt10, report.MeanMRR,
		report.MeanNDCGAt10, report.MAP)

	if report.NumQueries != 2 {
		t.Errorf("num_queries: got %d", report.NumQueries)
	}
	if report.MeanMRR <= 0 {
		t.Error("MRR should be > 0")
	}
}
