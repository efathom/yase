package embedder

import (
	"context"
	"hash/fnv"
	"math"
	"math/rand"
)

// MockEmbedder returns deterministic vectors based on FNV-1a text hashing.
// Used in all unit tests to avoid external API calls.
type MockEmbedder struct {
	dim int
}

// NewMockEmbedder creates a deterministic mock embedder with the given dimension.
func NewMockEmbedder(dim int) *MockEmbedder {
	return &MockEmbedder{dim: dim}
}

// Embed returns a deterministic, normalized vector seeded by the text hash.
func (m *MockEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	return m.deterministicVector(text), nil
}

// EmbedBatch returns deterministic vectors for each text.
func (m *MockEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, t := range texts {
		result[i] = m.deterministicVector(t)
	}
	return result, nil
}

// Dimension returns the configured vector dimensionality.
func (m *MockEmbedder) Dimension() int {
	return m.dim
}

func (m *MockEmbedder) deterministicVector(text string) []float32 {
	h := fnv.New64a()
	h.Write([]byte(text))
	rng := rand.New(rand.NewSource(int64(h.Sum64())))

	vec := make([]float32, m.dim)
	var norm float64
	for i := range vec {
		vec[i] = float32(rng.NormFloat64())
		norm += float64(vec[i]) * float64(vec[i])
	}
	// L2-normalize
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range vec {
			vec[i] /= float32(norm)
		}
	}
	return vec
}
