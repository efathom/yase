package vector

import (
	"errors"
	"fmt"

	"github.com/efathom/yase/pkg/cluster"
)

// ProductQuantizer compresses high-dimensional vectors by splitting them
// into M sub-vectors, each quantized to one of Ksub codebook centroids.
// Example: 1536-dim at M=96 → 96 bytes per vector (64x compression).
type ProductQuantizer struct {
	M         int           // number of sub-vectors (must divide VecDim)
	Ksub      int           // centroids per sub-space (typically 256, max 256 for byte codes)
	SubDim    int           // VecDim / M
	Codebooks [][][]float32 // M codebooks, each Ksub × SubDim
}

// NewProductQuantizer creates an untrained PQ with the given parameters.
// Call Train() before Encode/Decode.
func NewProductQuantizer(vecDim, m, ksub int) (*ProductQuantizer, error) {
	if vecDim%m != 0 {
		return nil, fmt.Errorf("pq: vecDim (%d) must be divisible by M (%d)", vecDim, m)
	}
	if ksub <= 0 || ksub > 256 {
		return nil, fmt.Errorf("pq: Ksub must be in [1, 256], got %d", ksub)
	}
	return &ProductQuantizer{
		M:      m,
		Ksub:   ksub,
		SubDim: vecDim / m,
	}, nil
}

// Train builds codebooks from a training set using K-means per sub-space.
func (pq *ProductQuantizer) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return errors.New("pq: empty training set")
	}
	if len(vectors) < pq.Ksub {
		return fmt.Errorf("pq: need at least %d training vectors, got %d", pq.Ksub, len(vectors))
	}

	vecDim := pq.M * pq.SubDim
	for i, v := range vectors {
		if len(v) != vecDim {
			return fmt.Errorf("pq: vector %d has dim %d, expected %d", i, len(v), vecDim)
		}
	}

	pq.Codebooks = make([][][]float32, pq.M)

	for m := 0; m < pq.M; m++ {
		// Extract sub-vectors for this sub-space
		subVectors := make([][]float32, len(vectors))
		offset := m * pq.SubDim
		for i, v := range vectors {
			subVectors[i] = v[offset : offset+pq.SubDim]
		}

		// Run K-means on this sub-space
		cfg := &cluster.KMeansConfig{
			K:             pq.Ksub,
			MaxIterations: 25,
			Seed:          int64(m + 1),
			DistFunc:      cluster.DistFunc(DistL2Squared),
		}
		result, err := cfg.Fit(subVectors)
		if err != nil {
			return fmt.Errorf("pq: kmeans sub-space %d: %w", m, err)
		}

		pq.Codebooks[m] = result.Centroids
	}

	return nil
}

// Encode compresses a vector to M bytes — one codebook index per sub-vector.
func (pq *ProductQuantizer) Encode(vec []float32) []byte {
	codes := make([]byte, pq.M)
	for m := 0; m < pq.M; m++ {
		offset := m * pq.SubDim
		sub := vec[offset : offset+pq.SubDim]
		codes[m] = pq.nearestCodeword(m, sub)
	}
	return codes
}

// Decode reconstructs an approximate vector from PQ codes.
func (pq *ProductQuantizer) Decode(codes []byte) []float32 {
	vec := make([]float32, pq.M*pq.SubDim)
	for m := 0; m < pq.M; m++ {
		centroid := pq.Codebooks[m][codes[m]]
		copy(vec[m*pq.SubDim:], centroid)
	}
	return vec
}

// nearestCodeword finds the closest codebook centroid for a sub-vector.
func (pq *ProductQuantizer) nearestCodeword(m int, sub []float32) byte {
	best := byte(0)
	bestDist := L2SquaredUnrolled(sub, pq.Codebooks[m][0])
	for c := 1; c < pq.Ksub; c++ {
		d := L2SquaredUnrolled(sub, pq.Codebooks[m][c])
		if d < bestDist {
			bestDist = d
			best = byte(c)
		}
	}
	return best
}

// ADCTable is a precomputed distance lookup table for asymmetric distance
// computation. Dimensions: M × Ksub.
type ADCTable [][]float32

// BuildADCTable precomputes distances from a query's sub-vectors to all
// codebook centroids. This enables O(M) distance per candidate instead of
// O(VecDim).
func (pq *ProductQuantizer) BuildADCTable(query []float32) ADCTable {
	table := make(ADCTable, pq.M)
	for m := 0; m < pq.M; m++ {
		offset := m * pq.SubDim
		sub := query[offset : offset+pq.SubDim]
		table[m] = make([]float32, pq.Ksub)
		for c := 0; c < pq.Ksub; c++ {
			table[m][c] = L2SquaredUnrolled(sub, pq.Codebooks[m][c])
		}
	}
	return table
}

// Distance computes the approximate L2 squared distance from the query
// (used to build this table) to a PQ-encoded vector. O(M) lookups.
func (t ADCTable) Distance(codes []byte) float32 {
	var sum float32
	for m, c := range codes {
		sum += t[m][c]
	}
	return sum
}
