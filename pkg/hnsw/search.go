package hnsw

import "container/heap"

// Candidate represents a node with its distance to the query vector.
type Candidate struct {
	ID       uint32
	Distance float32
}

// minHeap orders candidates closest-first (smallest distance at top).
type minHeap []Candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].Distance < h[j].Distance }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any) { *h = append(*h, x.(Candidate)) }
func (h *minHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

// maxHeap orders candidates furthest-first (largest distance at top) for eviction.
type maxHeap []Candidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].Distance > h[j].Distance }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any) { *h = append(*h, x.(Candidate)) }
func (h *maxHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

// SearchLayer implements Algorithm 2 from the HNSW paper.
// It performs a greedy best-first search at a single layer, returning up to ef
// nearest candidates. distFunc computes the distance from the query to a node ID.
func SearchLayer(entryPointID uint32, ef int, layer int, getEdges func(id uint32, layer int) []uint32, distFunc func(id uint32) float32) []Candidate {
	visited := make(map[uint32]struct{}, ef*2)

	epDist := distFunc(entryPointID)
	visited[entryPointID] = struct{}{}

	candidates := &minHeap{{ID: entryPointID, Distance: epDist}}
	heap.Init(candidates)

	results := &maxHeap{{ID: entryPointID, Distance: epDist}}
	heap.Init(results)

	for candidates.Len() > 0 {
		closest := heap.Pop(candidates).(Candidate)
		furthest := (*results)[0] // peek at furthest in results

		if closest.Distance > furthest.Distance && results.Len() >= ef {
			break // all remaining candidates are further than the worst result
		}

		edges := getEdges(closest.ID, layer)
		for _, neighborID := range edges {
			if _, seen := visited[neighborID]; seen {
				continue
			}
			visited[neighborID] = struct{}{}

			dist := distFunc(neighborID)
			furthest = (*results)[0]

			if dist < furthest.Distance || results.Len() < ef {
				heap.Push(candidates, Candidate{ID: neighborID, Distance: dist})
				heap.Push(results, Candidate{ID: neighborID, Distance: dist})
				if results.Len() > ef {
					heap.Pop(results) // evict furthest
				}
			}
		}
	}

	// Extract results sorted by distance ascending
	out := make([]Candidate, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(Candidate)
	}
	return out
}
