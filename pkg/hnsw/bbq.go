package hnsw

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/efathom/yase/pkg/vector"
)

// SearchBBQ performs Binary Quantized search: fast Hamming-distance traversal
// of the HNSW graph followed by exact float32 rescoring of the top candidates.
// oversample controls how many candidates to retrieve before rescoring (e.g., 3
// means retrieve 3*k candidates via Hamming, then rescore to k).
func (g *Graph) SearchBBQ(queryVec []float32, k int, ef int, oversample int) []Candidate {
	ep := g.entryPoint.Load()
	if ep == nil {
		return nil
	}

	// Binary-quantize the query
	queryBin := vector.BinaryQuantize(queryVec)
	binByteLen := len(queryBin) * 8

	// Binary distance function: Hamming distance via arena byte lookup
	binaryDistFunc := func(id uint32) float32 {
		n := g.getNode(id)
		if n == nil || n.BinaryVectorLen == 0 {
			return math.MaxFloat32
		}
		nodeBytes := g.arena.GetBytes(n.BinaryVectorOffset, n.BinaryVectorLen)
		// Convert bytes back to []uint64 for Hamming distance
		nodeBlocks := bytesToUint64(nodeBytes, binByteLen)
		return float32(vector.HammingDistance(queryBin, nodeBlocks))
	}

	// Greedy descent through upper layers using binary distance
	g.mu.RLock()
	maxLvl := g.maxLevel
	g.mu.RUnlock()

	currentEP := ep.ID
	for l := maxLvl; l > 0; l-- {
		results := SearchLayer(currentEP, 1, l, g.getEdges, binaryDistFunc)
		if len(results) > 0 {
			currentEP = results[0].ID
		}
	}

	// Bounded search at layer 0 with oversampling
	efBBQ := ef
	if oversample*k > ef {
		efBBQ = oversample * k
	}
	candidates := SearchLayer(currentEP, efBBQ, 0, g.getEdges, binaryDistFunc)

	// Rescore with exact float32 cosine distance
	exactDist := g.distToQuery(queryVec)
	rescored := make([]Candidate, len(candidates))
	for i, c := range candidates {
		rescored[i] = Candidate{ID: c.ID, Distance: exactDist(c.ID)}
	}

	sort.Slice(rescored, func(i, j int) bool {
		return rescored[i].Distance < rescored[j].Distance
	})

	if len(rescored) > k {
		rescored = rescored[:k]
	}
	return rescored
}

// bytesToUint64 converts a byte slice to []uint64 for Hamming distance.
// expectedByteLen is the total expected size in bytes.
func bytesToUint64(b []byte, expectedByteLen int) []uint64 {
	n := expectedByteLen / 8
	if n == 0 {
		n = 1
	}
	result := make([]uint64, n)
	for i := 0; i < n && i*8 < len(b); i++ {
		end := (i + 1) * 8
		if end > len(b) {
			end = len(b)
		}
		// Read as many bytes as available (up to 8)
		var val uint64
		chunk := b[i*8 : end]
		if len(chunk) == 8 {
			val = binary.LittleEndian.Uint64(chunk)
		} else {
			for j, bv := range chunk {
				val |= uint64(bv) << (j * 8)
			}
		}
		result[i] = val
	}
	return result
}
