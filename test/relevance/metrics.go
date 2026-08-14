// Package relevance provides search quality evaluation metrics.
// Use these to benchmark YASE's search pipeline against standard IR metrics.
package relevance

import (
	"math"
	"sort"
)

// Result represents a search result with its relevance label.
type Result struct {
	DocID    uint32
	Score    float64
	Relevant bool // ground truth: is this document relevant?
	GradeInt int  // graded relevance (0=irrelevant, 1=marginal, 2=relevant, 3=highly relevant)
}

// RecallAtK computes Recall@K: fraction of relevant documents found in top K results.
func RecallAtK(results []Result, k int, totalRelevant int) float64 {
	if totalRelevant == 0 || k <= 0 {
		return 0
	}

	if k > len(results) {
		k = len(results)
	}

	found := 0
	for i := 0; i < k; i++ {
		if results[i].Relevant {
			found++
		}
	}
	return float64(found) / float64(totalRelevant)
}

// PrecisionAtK computes Precision@K: fraction of top K results that are relevant.
func PrecisionAtK(results []Result, k int) float64 {
	if k <= 0 {
		return 0
	}

	if k > len(results) {
		k = len(results)
	}

	relevant := 0
	for i := 0; i < k; i++ {
		if results[i].Relevant {
			relevant++
		}
	}
	return float64(relevant) / float64(k)
}

// MRR computes Mean Reciprocal Rank: 1/rank of first relevant result.
func MRR(results []Result) float64 {
	for i, r := range results {
		if r.Relevant {
			return 1.0 / float64(i+1)
		}
	}
	return 0
}

// NDCG computes Normalized Discounted Cumulative Gain at K.
// Uses graded relevance (GradeInt field).
func NDCG(results []Result, k int) float64 {
	if k <= 0 || len(results) == 0 {
		return 0
	}
	if k > len(results) {
		k = len(results)
	}

	dcg := computeDCG(results, k)

	// Compute ideal DCG (sort by grade descending)
	ideal := make([]Result, len(results))
	copy(ideal, results)
	sort.Slice(ideal, func(i, j int) bool {
		return ideal[i].GradeInt > ideal[j].GradeInt
	})
	idcg := computeDCG(ideal, k)

	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

func computeDCG(results []Result, k int) float64 {
	var dcg float64
	for i := 0; i < k && i < len(results); i++ {
		gain := math.Pow(2, float64(results[i].GradeInt)) - 1
		discount := math.Log2(float64(i + 2)) // log2(i+2) since i is 0-indexed
		dcg += gain / discount
	}
	return dcg
}

// AveragePrecision computes AP for a single query (used to compute MAP).
func AveragePrecision(results []Result, totalRelevant int) float64 {
	if totalRelevant == 0 {
		return 0
	}

	var sum float64
	relevant := 0
	for i, r := range results {
		if r.Relevant {
			relevant++
			sum += float64(relevant) / float64(i+1)
		}
	}
	return sum / float64(totalRelevant)
}

// MAP computes Mean Average Precision across multiple queries.
func MAP(queryResults [][]Result, totalRelevants []int) float64 {
	if len(queryResults) == 0 {
		return 0
	}
	var sum float64
	for i, results := range queryResults {
		sum += AveragePrecision(results, totalRelevants[i])
	}
	return sum / float64(len(queryResults))
}

// EvalReport summarizes search quality metrics for a query set.
type EvalReport struct {
	NumQueries        int     `json:"num_queries"`
	MeanRecallAt10    float64 `json:"mean_recall_at_10"`
	MeanPrecisionAt10 float64 `json:"mean_precision_at_10"`
	MeanMRR           float64 `json:"mean_mrr"`
	MeanNDCGAt10      float64 `json:"mean_ndcg_at_10"`
	MAP               float64 `json:"map"`
}

// Evaluate runs all metrics on a set of query results and returns a report.
func Evaluate(queryResults [][]Result, totalRelevants []int) EvalReport {
	n := len(queryResults)
	if n == 0 {
		return EvalReport{}
	}

	var sumRecall, sumPrecision, sumMRR, sumNDCG float64
	for i, results := range queryResults {
		sumRecall += RecallAtK(results, 10, totalRelevants[i])
		sumPrecision += PrecisionAtK(results, 10)
		sumMRR += MRR(results)
		sumNDCG += NDCG(results, 10)
	}

	return EvalReport{
		NumQueries:        n,
		MeanRecallAt10:    sumRecall / float64(n),
		MeanPrecisionAt10: sumPrecision / float64(n),
		MeanMRR:           sumMRR / float64(n),
		MeanNDCGAt10:      sumNDCG / float64(n),
		MAP:               MAP(queryResults, totalRelevants),
	}
}
