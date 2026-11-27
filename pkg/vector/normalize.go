package vector

import "math"

// Normalized returns an L2-normalized copy of v. A zero vector is returned
// unchanged (as a copy), so its similarity to everything stays 0.
func Normalized(v []float32) []float32 {
	out := make([]float32, len(v))
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		copy(out, v)
		return out
	}
	inv := float32(1 / math.Sqrt(sum))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}
