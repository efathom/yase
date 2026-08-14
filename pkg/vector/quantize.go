package vector

import (
	"math"
	"math/bits"
)

// ScalarQuantize compresses 32-bit floats into 8-bit integers for 4x memory reduction.
// Each value is projected into [-128, 127] range based on the provided min/max bounds.
func ScalarQuantize(vec []float32, min, max float32) []int8 {
	quantized := make([]int8, len(vec))
	if max == min {
		// Degenerate range: all values map to 0 (constant vector).
		return quantized
	}
	scale := float64(255.0) / float64(max-min)
	for i, v := range vec {
		val := math.Round(float64(v-min)*scale) - 128
		if val > 127 {
			val = 127
		}
		if val < -128 {
			val = -128
		}
		quantized[i] = int8(val)
	}
	return quantized
}

// ScalarDequantize reverses scalar quantization back to float32.
func ScalarDequantize(quantized []int8, min, max float32) []float32 {
	vec := make([]float32, len(quantized))
	scale := (max - min) / 255.0
	for i, q := range quantized {
		vec[i] = (float32(q)+128)*scale + min
	}
	return vec
}

// BinaryQuantize maps continuous floats to single bits: positive → 1, non-positive → 0.
// Packs into uint64 blocks for 32x compression.
func BinaryQuantize(vec []float32) []uint64 {
	blocks := (len(vec) + 63) / 64
	bitsArray := make([]uint64, blocks)
	for i, v := range vec {
		if v > 0 {
			bitsArray[i/64] |= 1 << (i % 64)
		}
	}
	return bitsArray
}

// HammingDistance computes the Hamming distance between two binary-quantized vectors
// using hardware XOR + POPCNT (single-cycle on modern CPUs).
func HammingDistance(a, b []uint64) int {
	var dist int
	for i := range a {
		dist += bits.OnesCount64(a[i] ^ b[i])
	}
	return dist
}
