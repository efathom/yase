package cluster

import (
	"errors"
	"math"
	"math/rand"
)

// DistFunc computes a distance between two float32 vectors.
type DistFunc func(a, b []float32) float32

// l2Squared is the default distance function for K-means.
func l2Squared(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// KMeansConfig configures the K-means clustering algorithm.
type KMeansConfig struct {
	K             int      // number of clusters
	MaxIterations int      // convergence limit (default 100)
	Tolerance     float64  // centroid movement threshold (default 1e-4)
	DistFunc      DistFunc // distance function (defaults to L2 squared)
	Seed          int64    // random seed for reproducibility (0 = non-deterministic)
}

// KMeansResult holds the output of K-means clustering.
type KMeansResult struct {
	Centroids   [][]float32 // K centroids, each of dimension dim
	Assignments []int       // vector index → cluster ID
	Iterations  int         // number of iterations until convergence
	Inertia     float64     // sum of squared distances to assigned centroid
}

// Fit runs K-means++ on the given vectors and returns cluster assignments.
func (cfg *KMeansConfig) Fit(vectors [][]float32) (*KMeansResult, error) {
	n := len(vectors)
	if n == 0 {
		return nil, errors.New("kmeans: empty input")
	}
	if cfg.K <= 0 {
		return nil, errors.New("kmeans: K must be > 0")
	}
	if cfg.K > n {
		return nil, errors.New("kmeans: K exceeds number of vectors")
	}

	dim := len(vectors[0])
	maxIter := cfg.MaxIterations
	if maxIter <= 0 {
		maxIter = 100
	}
	tol := cfg.Tolerance
	if tol <= 0 {
		tol = 1e-4
	}
	dist := cfg.DistFunc
	if dist == nil {
		dist = l2Squared
	}

	var rng *rand.Rand
	if cfg.Seed != 0 {
		rng = rand.New(rand.NewSource(cfg.Seed))
	} else {
		rng = rand.New(rand.NewSource(rand.Int63()))
	}

	// K-means++ initialization
	centroids := kmeansppInit(vectors, cfg.K, dim, dist, rng)
	assignments := make([]int, n)

	var iterations int
	for iter := 0; iter < maxIter; iter++ {
		iterations = iter + 1

		// Assign each vector to nearest centroid
		for i, v := range vectors {
			best := 0
			bestDist := dist(v, centroids[0])
			for c := 1; c < cfg.K; c++ {
				d := dist(v, centroids[c])
				if d < bestDist {
					bestDist = d
					best = c
				}
			}
			assignments[i] = best
		}

		// Recompute centroids
		newCentroids := make([][]float32, cfg.K)
		counts := make([]int, cfg.K)
		for c := range newCentroids {
			newCentroids[c] = make([]float32, dim)
		}
		for i, v := range vectors {
			c := assignments[i]
			counts[c]++
			for d := range v {
				newCentroids[c][d] += v[d]
			}
		}
		for c := range newCentroids {
			if counts[c] > 0 {
				scale := 1.0 / float32(counts[c])
				for d := range newCentroids[c] {
					newCentroids[c][d] *= scale
				}
			} else {
				// Empty cluster: reinitialize to a random vector
				newCentroids[c] = copyVec(vectors[rng.Intn(n)])
			}
		}

		// Check convergence: max centroid movement
		maxDrift := float64(0)
		for c := range centroids {
			drift := float64(dist(centroids[c], newCentroids[c]))
			if drift > maxDrift {
				maxDrift = drift
			}
		}
		centroids = newCentroids

		if maxDrift < tol {
			break
		}
	}

	// Compute final inertia
	inertia := float64(0)
	for i, v := range vectors {
		inertia += float64(dist(v, centroids[assignments[i]]))
	}

	return &KMeansResult{
		Centroids:   centroids,
		Assignments: assignments,
		Iterations:  iterations,
		Inertia:     inertia,
	}, nil
}

// kmeansppInit selects K initial centroids using the K-means++ algorithm.
func kmeansppInit(vectors [][]float32, k, dim int, dist DistFunc, rng *rand.Rand) [][]float32 {
	n := len(vectors)
	centroids := make([][]float32, 0, k)

	// First centroid: uniform random
	centroids = append(centroids, copyVec(vectors[rng.Intn(n)]))

	// Precompute distances to nearest centroid
	minDists := make([]float64, n)
	for i := range minDists {
		minDists[i] = math.MaxFloat64
	}

	for len(centroids) < k {
		// Update min distances with latest centroid
		last := centroids[len(centroids)-1]
		totalWeight := float64(0)
		for i, v := range vectors {
			d := float64(dist(v, last))
			if d < minDists[i] {
				minDists[i] = d
			}
			totalWeight += minDists[i]
		}

		// Weighted random selection (proportional to squared distance)
		threshold := rng.Float64() * totalWeight
		cumulative := float64(0)
		selected := n - 1
		for i, d := range minDists {
			cumulative += d
			if cumulative >= threshold {
				selected = i
				break
			}
		}
		centroids = append(centroids, copyVec(vectors[selected]))
	}

	return centroids
}

func copyVec(v []float32) []float32 {
	c := make([]float32, len(v))
	copy(c, v)
	return c
}
