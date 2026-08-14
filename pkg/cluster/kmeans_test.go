package cluster

import (
	"math"
	"math/rand"
	"testing"
)

func TestKMeansBasicClustering(t *testing.T) {
	// 3 tight clusters in 2D
	vectors := [][]float32{
		{0, 0}, {0.1, 0.1}, {-0.1, 0.1},
		{10, 10}, {10.1, 9.9}, {9.9, 10.1},
		{0, 10}, {0.1, 10.1}, {-0.1, 9.9},
	}

	cfg := &KMeansConfig{K: 3, Seed: 42}
	result, err := cfg.Fit(vectors)
	if err != nil {
		t.Fatalf("Fit: %v", err)
	}

	// Verify 3 centroids returned
	if len(result.Centroids) != 3 {
		t.Fatalf("expected 3 centroids, got %d", len(result.Centroids))
	}

	// Vectors in same tight cluster should have same assignment
	if result.Assignments[0] != result.Assignments[1] || result.Assignments[1] != result.Assignments[2] {
		t.Error("first 3 vectors should be in same cluster")
	}
	if result.Assignments[3] != result.Assignments[4] || result.Assignments[4] != result.Assignments[5] {
		t.Error("second 3 vectors should be in same cluster")
	}
	if result.Assignments[6] != result.Assignments[7] || result.Assignments[7] != result.Assignments[8] {
		t.Error("third 3 vectors should be in same cluster")
	}

	// All 3 clusters should be different
	c0, c1, c2 := result.Assignments[0], result.Assignments[3], result.Assignments[6]
	if c0 == c1 || c1 == c2 || c0 == c2 {
		t.Errorf("clusters should be distinct: %d, %d, %d", c0, c1, c2)
	}

	t.Logf("Converged in %d iterations, inertia=%.4f", result.Iterations, result.Inertia)
}

func TestKMeansConvergence(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, 500)
	for i := range vectors {
		vectors[i] = []float32{rng.Float32() * 100, rng.Float32() * 100}
	}

	cfg := &KMeansConfig{K: 10, MaxIterations: 200, Seed: 42}
	result, err := cfg.Fit(vectors)
	if err != nil {
		t.Fatalf("Fit: %v", err)
	}

	if result.Iterations >= 200 {
		t.Error("did not converge within 200 iterations")
	}
	if result.Inertia <= 0 {
		t.Error("inertia should be positive")
	}
	t.Logf("Converged in %d iterations, inertia=%.2f", result.Iterations, result.Inertia)
}

func TestKMeansInertiaDecreases(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, 200)
	for i := range vectors {
		vectors[i] = []float32{rng.Float32() * 10, rng.Float32() * 10, rng.Float32() * 10}
	}

	// More clusters should give lower inertia
	var prevInertia float64 = math.MaxFloat64
	for _, k := range []int{2, 5, 10, 20} {
		cfg := &KMeansConfig{K: k, Seed: 42}
		result, err := cfg.Fit(vectors)
		if err != nil {
			t.Fatalf("K=%d: %v", k, err)
		}
		if result.Inertia >= prevInertia {
			t.Errorf("K=%d: inertia %.2f should be < %.2f", k, result.Inertia, prevInertia)
		}
		prevInertia = result.Inertia
		t.Logf("K=%d: inertia=%.2f, iterations=%d", k, result.Inertia, result.Iterations)
	}
}

func TestKMeansK1(t *testing.T) {
	vectors := [][]float32{{1, 2}, {3, 4}, {5, 6}}
	cfg := &KMeansConfig{K: 1, Seed: 42}
	result, err := cfg.Fit(vectors)
	if err != nil {
		t.Fatalf("Fit: %v", err)
	}

	// All assigned to cluster 0
	for i, a := range result.Assignments {
		if a != 0 {
			t.Errorf("vector %d: expected cluster 0, got %d", i, a)
		}
	}

	// Centroid should be the mean
	want := []float32{3, 4}
	for d := range want {
		if math.Abs(float64(result.Centroids[0][d]-want[d])) > 0.01 {
			t.Errorf("centroid[%d]: got %f, want %f", d, result.Centroids[0][d], want[d])
		}
	}
}

func TestKMeansKEqualsN(t *testing.T) {
	vectors := [][]float32{{1, 0}, {0, 1}, {1, 1}}
	cfg := &KMeansConfig{K: 3, Seed: 42}
	result, err := cfg.Fit(vectors)
	if err != nil {
		t.Fatalf("Fit: %v", err)
	}

	// Each vector should be its own cluster
	seen := make(map[int]bool)
	for _, a := range result.Assignments {
		seen[a] = true
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct clusters, got %d", len(seen))
	}
}

func TestKMeansErrors(t *testing.T) {
	tests := []struct {
		name    string
		vectors [][]float32
		k       int
	}{
		{"empty input", nil, 3},
		{"K=0", [][]float32{{1}}, 0},
		{"K > N", [][]float32{{1}, {2}}, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &KMeansConfig{K: tt.k}
			_, err := cfg.Fit(tt.vectors)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestKMeansCustomDistFunc(t *testing.T) {
	vectors := [][]float32{{0, 0}, {1, 0}, {10, 0}, {11, 0}}
	cfg := &KMeansConfig{K: 2, Seed: 42, DistFunc: l2Squared}
	result, err := cfg.Fit(vectors)
	if err != nil {
		t.Fatalf("Fit: %v", err)
	}

	// {0,0} and {1,0} should cluster, {10,0} and {11,0} should cluster
	if result.Assignments[0] != result.Assignments[1] {
		t.Error("expected {0,0} and {1,0} in same cluster")
	}
	if result.Assignments[2] != result.Assignments[3] {
		t.Error("expected {10,0} and {11,0} in same cluster")
	}
	if result.Assignments[0] == result.Assignments[2] {
		t.Error("expected two distinct clusters")
	}
}

func TestKMeansDeterministic(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = []float32{rng.Float32(), rng.Float32()}
	}

	cfg := &KMeansConfig{K: 5, Seed: 42}
	r1, _ := cfg.Fit(vectors)
	r2, _ := cfg.Fit(vectors)

	for i := range r1.Assignments {
		if r1.Assignments[i] != r2.Assignments[i] {
			t.Fatalf("non-deterministic: vector %d assigned to %d then %d", i, r1.Assignments[i], r2.Assignments[i])
		}
	}
}

func BenchmarkKMeans_1000x128_K16(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, 1000)
	for i := range vectors {
		v := make([]float32, 128)
		for j := range v {
			v[j] = rng.Float32()
		}
		vectors[i] = v
	}

	cfg := &KMeansConfig{K: 16, Seed: 42}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.Fit(vectors)
	}
}
