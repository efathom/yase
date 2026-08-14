package vector

import "math"

// DotProductUnrolled computes the dot product with 8-way loop unrolling.
// The Go compiler can vectorize the inner loop on supported architectures.
func DotProductUnrolled(a, b []float32) float32 {
	n := len(a)
	var s0, s1, s2, s3, s4, s5, s6, s7 float32

	i := 0
	for ; i+7 < n; i += 8 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
		s4 += a[i+4] * b[i+4]
		s5 += a[i+5] * b[i+5]
		s6 += a[i+6] * b[i+6]
		s7 += a[i+7] * b[i+7]
	}
	sum := (s0 + s1) + (s2 + s3) + (s4 + s5) + (s6 + s7)
	for ; i < n; i++ {
		sum += a[i] * b[i]
	}
	return sum
}

// L2SquaredUnrolled computes the squared L2 (Euclidean) distance with
// 8-way loop unrolling. Useful as an alternative distance metric.
func L2SquaredUnrolled(a, b []float32) float32 {
	n := len(a)
	var s0, s1, s2, s3, s4, s5, s6, s7 float32

	i := 0
	for ; i+7 < n; i += 8 {
		d0, d1 := a[i]-b[i], a[i+1]-b[i+1]
		d2, d3 := a[i+2]-b[i+2], a[i+3]-b[i+3]
		d4, d5 := a[i+4]-b[i+4], a[i+5]-b[i+5]
		d6, d7 := a[i+6]-b[i+6], a[i+7]-b[i+7]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
		s4 += d4 * d4
		s5 += d5 * d5
		s6 += d6 * d6
		s7 += d7 * d7
	}
	sum := (s0 + s1) + (s2 + s3) + (s4 + s5) + (s6 + s7)
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// NormSquaredUnrolled computes the squared L2 norm with 8-way unrolling.
func NormSquaredUnrolled(a []float32) float32 {
	return DotProductUnrolled(a, a)
}

// CosineSimilarityFast computes cosine similarity using unrolled dot products.
// Uses three unrolled passes: dot(a,b), norm(a), norm(b).
func CosineSimilarityFast(a, b []float32) float32 {
	dot := DotProductUnrolled(a, b)
	normA := NormSquaredUnrolled(a)
	normB := NormSquaredUnrolled(b)
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA))*math.Sqrt(float64(normB)))
}

// CosineDistanceFast returns 1 - CosineSimilarityFast.
func CosineDistanceFast(a, b []float32) float32 {
	return 1 - CosineSimilarityFast(a, b)
}

// CosineSimilaritySinglePass computes cosine similarity in one pass over both
// vectors (better cache locality for large dimensions). Accumulates dot, normA,
// normB simultaneously with 4-way unrolling.
func CosineSimilaritySinglePass(a, b []float32) float32 {
	n := len(a)
	var dot0, dot1, dot2, dot3 float32
	var na0, na1, na2, na3 float32
	var nb0, nb1, nb2, nb3 float32

	i := 0
	for ; i+3 < n; i += 4 {
		a0, a1, a2, a3 := a[i], a[i+1], a[i+2], a[i+3]
		b0, b1, b2, b3 := b[i], b[i+1], b[i+2], b[i+3]
		dot0 += a0 * b0
		dot1 += a1 * b1
		dot2 += a2 * b2
		dot3 += a3 * b3
		na0 += a0 * a0
		na1 += a1 * a1
		na2 += a2 * a2
		na3 += a3 * a3
		nb0 += b0 * b0
		nb1 += b1 * b1
		nb2 += b2 * b2
		nb3 += b3 * b3
	}
	dot := (dot0 + dot1) + (dot2 + dot3)
	normA := (na0 + na1) + (na2 + na3)
	normB := (nb0 + nb1) + (nb2 + nb3)

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA))*math.Sqrt(float64(normB)))
}
