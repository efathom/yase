package index

import (
	"sort"
	"strings"
	"sync"
)

// TenantField is the reserved metadata field that records which tenant owns a
// document. It is written at ingest and forced into search, delete, and
// suggest filters by the auth layer, so callers can never select another
// tenant's documents by supplying it themselves.
const TenantField = "_tenant"

// globalPartition holds terms from documents carrying no tenant metadata.
// It is reachable only by callers that supply no tenant filter.
const globalPartition = ""

// SuggestEngine provides prefix-based autocomplete suggestions
// built from indexed document titles and high-frequency terms.
//
// Terms are partitioned by tenant. The term dictionary would otherwise span
// every tenant, letting one walk the prefix space to recover another's
// vocabulary without ever retrieving a document.
type SuggestEngine struct {
	mu sync.RWMutex
	// tenant ID → term → frequency count. Documents with no tenant are
	// stored under globalPartition.
	partitions map[string]map[string]int
}

// NewSuggestEngine creates an empty suggest engine.
func NewSuggestEngine() *SuggestEngine {
	return &SuggestEngine{
		partitions: make(map[string]map[string]int),
	}
}

// AddDocument extracts terms from a document's text and title for suggestion,
// recording them against the tenant named in metadata["_tenant"].
func (s *SuggestEngine) AddDocument(title, text string, metadata map[string]string) {
	tenant, ok := metadata[TenantField]
	if !ok {
		tenant = globalPartition
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	terms := s.partitions[tenant]
	if terms == nil {
		terms = make(map[string]int)
		s.partitions[tenant] = terms
	}

	// Index title words with high weight
	for _, word := range tokenize(strings.ToLower(title)) {
		if len(word) >= 2 {
			terms[word] += 10
		}
	}

	// Index content words with lower weight
	for _, word := range tokenize(strings.ToLower(text)) {
		if len(word) >= 3 {
			terms[word]++
		}
	}

	// Index bigrams from title for phrase suggestions
	titleWords := tokenize(strings.ToLower(title))
	for i := 0; i < len(titleWords)-1; i++ {
		bigram := titleWords[i] + " " + titleWords[i+1]
		terms[bigram] += 5
	}
}

// Suggest returns up to limit suggestions matching the prefix, sorted by
// frequency.
//
// When filters names a tenant, only that tenant's partition is searched — the
// global partition is deliberately excluded, since untagged documents cannot be
// attributed to the caller. With no tenant filter, every partition is searched.
func (s *SuggestEngine) Suggest(prefix string, limit int, filters map[string]string) []Suggestion {
	if prefix == "" {
		return nil
	}
	if limit <= 0 {
		limit = 10
	}

	prefix = strings.ToLower(prefix)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Merge counts across the partitions this caller may read.
	merged := make(map[string]int)
	collect := func(terms map[string]int) {
		for term, count := range terms {
			if strings.HasPrefix(term, prefix) {
				merged[term] += count
			}
		}
	}

	if tenant, scoped := filters[TenantField]; scoped {
		collect(s.partitions[tenant])
	} else {
		for _, terms := range s.partitions {
			collect(terms)
		}
	}

	matches := make([]Suggestion, 0, len(merged))
	for term, count := range merged {
		matches = append(matches, Suggestion{Text: term, Score: count})
	}

	// Sort by frequency descending, breaking ties on the term so results are
	// stable across runs rather than dependent on map iteration order.
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		return matches[i].Text < matches[j].Text
	})

	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches
}

// Len returns the number of unique terms in the suggest index across all
// tenant partitions.
func (s *SuggestEngine) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, terms := range s.partitions {
		n += len(terms)
	}
	return n
}

// Suggestion is a single autocomplete suggestion.
type Suggestion struct {
	Text  string `json:"text"`
	Score int    `json:"score"`
}
