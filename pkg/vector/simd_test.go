package vector

import (
	"math"
	"math/rand"
	"testing"
)

func TestDotProductUnrolledMatchesNaive(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dims := []int{1, 3, 7, 8, 15, 16, 128, 768, 1536}

	for _, dim := range dims {
		a := randVec(rng, dim)
		b := randVec(rng, dim)

		naive := DotProduct(a, b)
		fast := DotProductUnrolled(a, b)

		if relErr(naive, fast) > 1e-5 {
			t.Errorf("dim=%d: naive=%f, unrolled=%f", dim, naive, fast)
		}
	}
}

func TestCosineSimilarityFastMatchesNaive(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dims := []int{1, 3, 7, 8, 15, 16, 128, 768, 1536}

	for _, dim := range dims {
		a := randVec(rng, dim)
		b := randVec(rng, dim)

		naive := CosineSimilarity(a, b)
		fast := CosineSimilarityFast(a, b)

		if relErr(naive, fast) > 1e-5 {
			t.Errorf("dim=%d: naive=%f, fast=%f", dim, naive, fast)
		}
	}
}

func TestCosineSimilaritySinglePassMatchesNaive(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dims := []int{1, 3, 7, 8, 15, 16, 128, 768, 1536}

	for _, dim := range dims {
		a := randVec(rng, dim)
		b := randVec(rng, dim)

		naive := CosineSimilarity(a, b)
		sp := CosineSimilaritySinglePass(a, b)

		if relErr(naive, sp) > 1e-5 {
			t.Errorf("dim=%d: naive=%f, single_pass=%f", dim, naive, sp)
		}
	}
}

func TestL2SquaredUnrolled(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	got := L2SquaredUnrolled(a, b)
	want := float32(27) // (3^2 + 3^2 + 3^2)
	if math.Abs(float64(got-want)) > 1e-5 {
		t.Errorf("L2Squared: got %f, want %f", got, want)
	}
}

func TestCosineDistanceFastZeroVector(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{1, 2, 3}
	d := CosineDistanceFast(a, b)
	if d != 1.0 {
		t.Errorf("zero vector distance: got %f, want 1.0", d)
	}
}

func TestDistFuncVariants(t *testing.T) {
	a := []float32{1, 2, 3, 4, 5}
	b := []float32{5, 4, 3, 2, 1}

	tests := []struct {
		name string
		fn   DistFunc
	}{
		{"DistCosine", DistCosine},
		{"DistCosineUnrolled", DistCosineUnrolled},
		{"DistCosineSP", DistCosineSP},
	}

	baseline := DistCosine(a, b)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fn(a, b)
			if relErr(baseline, got) > 1e-5 {
				t.Errorf("%s: got %f, want %f", tt.name, got, baseline)
			}
		})
	}
}

// ── Benchmarks ──

func BenchmarkDotProduct_Naive_1536(b *testing.B) {
	benchDot(b, 1536, DotProduct)
}

func BenchmarkDotProduct_Unrolled_1536(b *testing.B) {
	benchDot(b, 1536, DotProductUnrolled)
}

func BenchmarkCosineSimilarity_Naive_1536(b *testing.B) {
	benchCosine(b, 1536, CosineSimilarity)
}

func BenchmarkCosineSimilarity_Fast_1536(b *testing.B) {
	benchCosine(b, 1536, CosineSimilarityFast)
}

func BenchmarkCosineSimilarity_SinglePass_1536(b *testing.B) {
	benchCosine(b, 1536, CosineSimilaritySinglePass)
}

func BenchmarkCosineDistance_Naive_768(b *testing.B) {
	benchCosine(b, 768, func(a, c []float32) float32 { return CosineDistance(a, c) })
}

func BenchmarkCosineDistance_Fast_768(b *testing.B) {
	benchCosine(b, 768, func(a, c []float32) float32 { return CosineDistanceFast(a, c) })
}

func BenchmarkL2Squared_Unrolled_1536(b *testing.B) {
	benchDot(b, 1536, L2SquaredUnrolled)
}

// ── Helpers ──

func randVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func relErr(a, b float32) float64 {
	if a == 0 && b == 0 {
		return 0
	}
	return math.Abs(float64(a-b)) / math.Max(math.Abs(float64(a)), math.Abs(float64(b)))
}

func benchDot(b *testing.B, dim int, fn func([]float32, []float32) float32) {
	rng := rand.New(rand.NewSource(42))
	a := randVec(rng, dim)
	bv := randVec(rng, dim)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(a, bv)
	}
}

func benchCosine(b *testing.B, dim int, fn func([]float32, []float32) float32) {
	rng := rand.New(rand.NewSource(42))
	a := randVec(rng, dim)
	bv := randVec(rng, dim)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(a, bv)
	}
}
