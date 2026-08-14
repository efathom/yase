package vector

import "math"

// CosineSimilarity computes the cosine similarity between two float32 vectors.
// Returns 0 if either vector has zero norm. Uses the shorter length to avoid
// index-out-of-range panics on mismatched dimensions.
func CosineSimilarity(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 0
	}
	var dot, normA, normB float32
	for i := 0; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// CosineDistance returns 1 - CosineSimilarity, useful as a distance metric
// where 0 means identical and 2 means opposite.
func CosineDistance(a, b []float32) float32 {
	return 1 - CosineSimilarity(a, b)
}

// DotProduct computes the dot product of two float32 vectors.
func DotProduct(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}
