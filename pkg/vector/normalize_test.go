package vector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalized(t *testing.T) {
	tests := []struct {
		name string
		in   []float32
		want []float32
	}{
		{"unit axis", []float32{3, 0, 0}, []float32{1, 0, 0}},
		{"3-4-5", []float32{3, 4}, []float32{0.6, 0.8}},
		{"zero", []float32{0, 0}, []float32{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Normalized(tt.in)
			assert.InDeltaSlice(t, tt.want, got, 1e-6)
		})
	}
}

func TestDistNormalizedDotMatchesCosine(t *testing.T) {
	a := Normalized([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9})
	b := Normalized([]float32{9, 1, 8, 2, 7, 3, 6, 4, 5})
	assert.InDelta(t, CosineDistance(a, b), DistNormalizedDot(a, b), 1e-5)
}
