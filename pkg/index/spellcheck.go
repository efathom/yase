package index

import (
	"sort"
	"strings"
	"sync"
)

// SpellChecker provides "did you mean" suggestions using a term frequency
// dictionary built from indexed content and Levenshtein distance.
type SpellChecker struct {
	mu    sync.RWMutex
	terms map[string]int // term → frequency
}

// NewSpellChecker creates an empty spell checker.
func NewSpellChecker() *SpellChecker {
	return &SpellChecker{terms: make(map[string]int)}
}

// AddTerms indexes terms from document content into the dictionary.
func (sc *SpellChecker) AddTerms(text string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, word := range tokenize(strings.ToLower(text)) {
		if len(word) >= 2 {
			sc.terms[word]++
		}
	}
}

// Suggest returns a corrected query if the original terms appear to be misspelled.
// Returns empty string if the query looks correct (all terms found in dictionary).
func (sc *SpellChecker) Suggest(query string) string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	words := tokenize(strings.ToLower(query))
	if len(words) == 0 {
		return ""
	}

	anyCorrection := false
	corrected := make([]string, len(words))

	for i, word := range words {
		if _, found := sc.terms[word]; found {
			corrected[i] = word
			continue
		}

		// Find closest term by Levenshtein distance
		best := findClosest(word, sc.terms, 2) // max edit distance 2
		if best != "" {
			corrected[i] = best
			anyCorrection = true
		} else {
			corrected[i] = word
		}
	}

	if !anyCorrection {
		return ""
	}
	return strings.Join(corrected, " ")
}

// Len returns the dictionary size.
func (sc *SpellChecker) Len() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return len(sc.terms)
}

// findClosest returns the most frequent term within maxDist edit distance.
func findClosest(word string, terms map[string]int, maxDist int) string {
	type candidate struct {
		term string
		dist int
		freq int
	}

	var candidates []candidate

	for term, freq := range terms {
		// Quick length check to skip obvious non-matches
		lenDiff := len(term) - len(word)
		if lenDiff > maxDist || lenDiff < -maxDist {
			continue
		}

		dist := levenshtein(word, term)
		if dist <= maxDist && dist > 0 {
			candidates = append(candidates, candidate{term: term, dist: dist, freq: freq})
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	// Sort by distance first, then frequency (higher is better)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].dist != candidates[j].dist {
			return candidates[i].dist < candidates[j].dist
		}
		return candidates[i].freq > candidates[j].freq
	})

	return candidates[0].term
}

// levenshtein computes the edit distance between two strings.
func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	// Use two rows for O(min(m,n)) space
	if la < lb {
		a, b = b, a
		la, lb = lb, la
	}

	prev := make([]int, lb+1)
	curr := make([]int, lb+1)

	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min3(
				prev[j]+1,      // deletion
				curr[j-1]+1,    // insertion
				prev[j-1]+cost, // substitution
			)
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}
