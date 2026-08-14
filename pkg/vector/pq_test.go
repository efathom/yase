package vector

import (
	"math"
	"math/rand"
	"testing"
)

func TestPQTrainAndEncodeDecode(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dim := 32
	m := 8 // 8 sub-vectors of dim 4
	ksub := 16

	// Generate training data
	vectors := make([][]float32, 500)
	for i := range vectors {
		vectors[i] = randVecPQ(rng, dim)
	}

	pq, err := NewProductQuantizer(dim, m, ksub)
	if err != nil {
		t.Fatalf("NewProductQuantizer: %v", err)
	}

	if err := pq.Train(vectors); err != nil {
		t.Fatalf("Train: %v", err)
	}

	// Verify codebooks
	if len(pq.Codebooks) != m {
		t.Fatalf("expected %d codebooks, got %d", m, len(pq.Codebooks))
	}
	for i, cb := range pq.Codebooks {
		if len(cb) != ksub {
			t.Errorf("codebook %d: expected %d centroids, got %d", i, ksub, len(cb))
		}
	}

	// Encode and decode a vector
	original := vectors[0]
	codes := pq.Encode(original)
	if len(codes) != m {
		t.Fatalf("expected %d codes, got %d", m, len(codes))
	}

	decoded := pq.Decode(codes)
	if len(decoded) != dim {
		t.Fatalf("decoded dim: got %d, want %d", len(decoded), dim)
	}

	// Reconstructed should be close to original (within quantization error)
	dist := L2SquaredUnrolled(original, decoded)
	t.Logf("Reconstruction L2²=%.4f for dim=%d, M=%d, Ksub=%d", dist, dim, m, ksub)

	// Should be significantly less than average random distance
	avgRandom := float32(0)
	for i := 0; i < 100; i++ {
		avgRandom += L2SquaredUnrolled(original, randVecPQ(rng, dim))
	}
	avgRandom /= 100

	if dist >= avgRandom {
		t.Errorf("reconstruction L2² (%.4f) should be much less than avg random (%.4f)", dist, avgRandom)
	}
}

func TestPQADCDistance(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dim := 32
	m := 8
	ksub := 16

	vectors := make([][]float32, 500)
	for i := range vectors {
		vectors[i] = randVecPQ(rng, dim)
	}

	pq, _ := NewProductQuantizer(dim, m, ksub)
	pq.Train(vectors)

	query := randVecPQ(rng, dim)
	table := pq.BuildADCTable(query)

	// ADC distance should approximate true L2² between query and decoded vector
	for i := 0; i < 50; i++ {
		codes := pq.Encode(vectors[i])
		adcDist := table.Distance(codes)
		decoded := pq.Decode(codes)
		trueDist := L2SquaredUnrolled(query, decoded)

		// ADC distance should exactly equal distance to decoded vector
		if math.Abs(float64(adcDist-trueDist)) > 1e-3 {
			t.Errorf("vector %d: ADC=%.4f, true(decoded)=%.4f", i, adcDist, trueDist)
		}
	}
}

func TestPQRecallAtK(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dim := 64
	m := 16
	ksub := 256
	nTrain := 2000
	nQuery := 50
	k := 10

	// Generate data
	database := make([][]float32, nTrain)
	for i := range database {
		database[i] = randVecPQ(rng, dim)
	}

	pq, _ := NewProductQuantizer(dim, m, ksub)
	pq.Train(database)

	// Encode all database vectors
	encodedDB := make([][]byte, nTrain)
	for i := range database {
		encodedDB[i] = pq.Encode(database[i])
	}

	// Measure recall@K
	totalRecall := 0
	for q := 0; q < nQuery; q++ {
		query := randVecPQ(rng, dim)

		// Brute-force true top-K
		trueScores := make([]pqScored, nTrain)
		for i := range database {
			trueScores[i] = pqScored{i, L2SquaredUnrolled(query, database[i])}
		}
		topKTrue := pqTopK(trueScores, k)

		// ADC top-K
		table := pq.BuildADCTable(query)
		adcScores := make([]pqScored, nTrain)
		for i := range encodedDB {
			adcScores[i] = pqScored{i, table.Distance(encodedDB[i])}
		}
		topKADC := pqTopK(adcScores, k)

		// Count overlap
		trueSet := make(map[int]bool)
		for _, s := range topKTrue {
			trueSet[s.idx] = true
		}
		for _, s := range topKADC {
			if trueSet[s.idx] {
				totalRecall++
			}
		}
	}

	recall := float64(totalRecall) / float64(nQuery*k)
	t.Logf("Recall@%d = %.2f%% (dim=%d, M=%d, Ksub=%d)", k, recall*100, dim, m, ksub)

	if recall < 0.50 {
		t.Errorf("recall@%d = %.2f%%, expected > 50%%", k, recall*100)
	}
}

func TestPQErrors(t *testing.T) {
	t.Run("dim not divisible by M", func(t *testing.T) {
		_, err := NewProductQuantizer(10, 3, 256)
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Ksub > 256", func(t *testing.T) {
		_, err := NewProductQuantizer(16, 4, 257)
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("too few training vectors", func(t *testing.T) {
		pq, _ := NewProductQuantizer(8, 2, 16)
		err := pq.Train([][]float32{{1, 2, 3, 4, 5, 6, 7, 8}})
		if err == nil {
			t.Error("expected error for fewer vectors than Ksub")
		}
	})
}

func BenchmarkPQEncode_1536_M96(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	dim := 1536
	m := 96

	vectors := make([][]float32, 1000)
	for i := range vectors {
		vectors[i] = randVecPQ(rng, dim)
	}

	pq, _ := NewProductQuantizer(dim, m, 256)
	pq.Train(vectors)
	query := randVecPQ(rng, dim)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.Encode(query)
	}
}

func BenchmarkPQADCDistance_M96(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	dim := 1536
	m := 96

	vectors := make([][]float32, 1000)
	for i := range vectors {
		vectors[i] = randVecPQ(rng, dim)
	}

	pq, _ := NewProductQuantizer(dim, m, 256)
	pq.Train(vectors)

	query := randVecPQ(rng, dim)
	table := pq.BuildADCTable(query)
	codes := pq.Encode(vectors[0])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Distance(codes)
	}
}

// ── Helpers ──

func randVecPQ(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

type pqScored struct {
	idx  int
	dist float32
}

func pqTopK(scores []pqScored, k int) []pqScored {
	result := make([]pqScored, len(scores))
	copy(result, scores)
	for i := 0; i < k && i < len(result); i++ {
		minIdx := i
		for j := i + 1; j < len(result); j++ {
			if result[j].dist < result[minIdx].dist {
				minIdx = j
			}
		}
		result[i], result[minIdx] = result[minIdx], result[i]
	}
	if k > len(result) {
		k = len(result)
	}
	return result[:k]
}
