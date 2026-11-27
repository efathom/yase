package index

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/efathom/yase/pkg/vector"
)

// clusteredVectors generates n vectors around k random centers, which is
// closer to real embedding distributions than uniform noise.
func clusteredVectors(rng *rand.Rand, n, k, dim int) [][]float32 {
	centers := make([][]float32, k)
	for i := range centers {
		c := make([]float32, dim)
		for j := range c {
			c[j] = rng.Float32()*2 - 1
		}
		centers[i] = c
	}
	out := make([][]float32, n)
	for i := range out {
		c := centers[rng.Intn(k)]
		v := make([]float32, dim)
		for j := range v {
			v[j] = c[j] + float32(rng.NormFloat64()*0.25)
		}
		out[i] = v
	}
	return out
}

// measureRecall returns mean recall@k of semantic-only HybridSearch against
// exact brute-force cosine search.
func measureRecall(t testing.TB, n, dim, queries, k int) float64 {
	rng := rand.New(rand.NewSource(7))
	he, err := NewHybridEngine(t.TempDir(), 1<<30, dim, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer he.Close()
	vecs := clusteredVectors(rng, n, 50, dim)
	ctx := context.Background()
	var batch []Document
	for i, v := range vecs {
		batch = append(batch, Document{ID: uint32(i), Text: fmt.Sprintf("d%d", i), Vector: v})
		if len(batch) == 1000 || i == n-1 {
			if err := he.IngestBatch(ctx, nil, batch); err != nil {
				t.Fatal(err)
			}
			batch = nil
		}
	}
	norm := make([][]float32, n)
	for i, v := range vecs {
		norm[i] = vector.Normalized(v)
	}

	total := 0.0
	for qi := 0; qi < queries; qi++ {
		q := clusteredVectors(rng, 1, 50, dim)[0]
		nq := vector.Normalized(q)
		type sc struct {
			id uint32
			s  float32
		}
		all := make([]sc, n)
		for i := range norm {
			all[i] = sc{uint32(i), vector.DotProductUnrolled(nq, norm[i])}
		}
		sort.Slice(all, func(a, b int) bool { return all[a].s > all[b].s })
		truth := make(map[uint32]bool, k)
		for _, x := range all[:k] {
			truth[x.id] = true
		}
		res, err := he.HybridSearch(ctx, "", q, nil, k)
		if err != nil {
			t.Fatal(err)
		}
		hit := 0
		for _, r := range res {
			if truth[r.ID] {
				hit++
			}
		}
		total += float64(hit) / float64(k)
	}
	return total / float64(queries)
}

// TestVectorRecall guards ANN quality: a change to HNSW, centroid routing or
// probing that degrades recall fails CI.
func TestVectorRecall(t *testing.T) {
	n := 20000
	if testing.Short() {
		n = 5000
	}
	recall := measureRecall(t, n, 32, 100, 10)
	t.Logf("recall@10 over %d docs: %.3f", n, recall)
	if recall < minRecallAt10 {
		t.Fatalf("recall@10 = %.3f, want >= %.2f", recall, minRecallAt10)
	}
}

// minRecallAt10 is the CI floor for semantic recall@10. Measured: 0.97 at 5k,
// 0.96 at 20k and 0.99 at 100k documents (clustered, 32-d).
const minRecallAt10 = 0.93
