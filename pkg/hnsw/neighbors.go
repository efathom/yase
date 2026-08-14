package hnsw

import "sort"

// SelectNeighborsHeuristic implements Algorithm 4 from the HNSW paper.
// It selects up to maxNeighbors from candidates that promote graph diversity
// by preferring candidates that are closer to the base than to any already-selected
// neighbor (long-range navigability).
//
// distFunc(a, b uint32) returns the distance between two node IDs.
func SelectNeighborsHeuristic(baseID uint32, candidates []Candidate, maxNeighbors int, distFunc func(a, b uint32) float32) []Candidate {
	if len(candidates) <= maxNeighbors {
		return candidates
	}

	// Sort candidates by distance ascending (closest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Distance < candidates[j].Distance
	})

	selected := make([]Candidate, 0, maxNeighbors)
	discarded := make([]Candidate, 0)

	for _, c := range candidates {
		if len(selected) >= maxNeighbors {
			break
		}
		// Accept if candidate is closer to base than to any already-selected neighbor
		good := true
		for _, s := range selected {
			distToSelected := distFunc(c.ID, s.ID)
			if distToSelected < c.Distance {
				good = false
				break
			}
		}
		if good {
			selected = append(selected, c)
		} else {
			discarded = append(discarded, c)
		}
	}

	// Backfill from discards if we haven't reached maxNeighbors
	for _, d := range discarded {
		if len(selected) >= maxNeighbors {
			break
		}
		selected = append(selected, d)
	}

	return selected
}
