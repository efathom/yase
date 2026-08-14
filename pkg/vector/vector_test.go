package vector

import (
	"math"
	"math/rand"
	"testing"
)

func TestCosineSimilarityIdentical(t *testing.T) {
	v := []float32{1, 2, 3, 4, 5}
	sim := CosineSimilarity(v, v)
	if math.Abs(float64(sim)-1.0) > 1e-6 {
		t.Errorf("identical vectors: got %f, want 1.0", sim)
	}
}

func TestCosineSimilarityOrthogonal(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	sim := CosineSimilarity(a, b)
	if math.Abs(float64(sim)) > 1e-6 {
		t.Errorf("orthogonal vectors: got %f, want 0.0", sim)
	}
}

func TestCosineSimilarityOpposite(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{-1, -2, -3}
	sim := CosineSimilarity(a, b)
	if math.Abs(float64(sim)+1.0) > 1e-6 {
		t.Errorf("opposite vectors: got %f, want -1.0", sim)
	}
}

func TestCosineSimilarityZeroVector(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{1, 2, 3}
	sim := CosineSimilarity(a, b)
	if sim != 0 {
		t.Errorf("zero vector: got %f, want 0.0", sim)
	}
}

func TestCosineDistance(t *testing.T) {
	v := []float32{1, 2, 3}
	d := CosineDistance(v, v)
	if math.Abs(float64(d)) > 1e-6 {
		t.Errorf("identical vectors distance: got %f, want 0.0", d)
	}
}

func TestScalarQuantizeRoundTrip(t *testing.T) {
	vec := []float32{-1.0, -0.5, 0.0, 0.5, 1.0}
	minVal := float32(-1.0)
	maxVal := float32(1.0)

	q := ScalarQuantize(vec, minVal, maxVal)
	dq := ScalarDequantize(q, minVal, maxVal)

	for i := range vec {
		err := math.Abs(float64(vec[i] - dq[i]))
		if err > 0.01 { // int8 quantization: max error ~1/255 ≈ 0.004
			t.Errorf("index %d: original %f, dequantized %f, error %f", i, vec[i], dq[i], err)
		}
	}
}

func TestScalarQuantizeClamping(t *testing.T) {
	vec := []float32{-2.0, 2.0} // outside min/max range
	q := ScalarQuantize(vec, -1.0, 1.0)
	if q[0] != -128 {
		t.Errorf("expected -128 for underflow, got %d", q[0])
	}
	if q[1] != 127 {
		t.Errorf("expected 127 for overflow, got %d", q[1])
	}
}

func TestBinaryQuantizeSign(t *testing.T) {
	vec := []float32{1.0, -1.0, 0.5, -0.5, 0.0, 3.14}
	bits := BinaryQuantize(vec)
	// Expected: index 0 (positive)=1, 1 (negative)=0, 2 (positive)=1,
	//           3 (negative)=0, 4 (zero/non-positive)=0, 5 (positive)=1
	expected := uint64(0b100101) // bits 0,2,5 set
	if bits[0] != expected {
		t.Errorf("binary quantize: got %b, want %b", bits[0], expected)
	}
}

func TestHammingDistanceIdentical(t *testing.T) {
	a := []uint64{0xFF00FF00, 0x12345678}
	d := HammingDistance(a, a)
	if d != 0 {
		t.Errorf("identical: got %d, want 0", d)
	}
}

func TestHammingDistanceKnown(t *testing.T) {
	a := []uint64{0}
	b := []uint64{7} // bits 0,1,2 set = 3 ones
	d := HammingDistance(a, b)
	if d != 3 {
		t.Errorf("got %d, want 3", d)
	}
}

func TestDotProduct(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	got := DotProduct(a, b)
	want := float32(32) // 1*4 + 2*5 + 3*6
	if math.Abs(float64(got-want)) > 1e-6 {
		t.Errorf("dot product: got %f, want %f", got, want)
	}
}

func BenchmarkCosineSimilarity768d(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	a := make([]float32, 768)
	bv := make([]float32, 768)
	for i := range a {
		a[i] = rng.Float32()
		bv[i] = rng.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSimilarity(a, bv)
	}
}

func BenchmarkHammingDistance768d(b *testing.B) {
	blocks := (768 + 63) / 64
	a := make([]uint64, blocks)
	bv := make([]uint64, blocks)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HammingDistance(a, bv)
	}
}
