package vector

// DistFunc computes a distance between two float32 vectors.
// Lower values mean more similar. Used as the pluggable distance
// metric in HNSW graph and hybrid search.
type DistFunc func(a, b []float32) float32

// Predefined distance functions. DistCosine (single-pass) is the default.
// Benchmark on your target hardware to pick the fastest variant.
var (
	DistCosine         DistFunc = CosineDistance     // single-pass, best cache locality
	DistCosineUnrolled DistFunc = CosineDistanceFast // 3-pass with 8-way unrolling (faster on real x86)
	DistCosineSP       DistFunc = func(a, b []float32) float32 { return 1 - CosineSimilaritySinglePass(a, b) }
	DistL2Squared      DistFunc = L2SquaredUnrolled
)
