package index

import (
	"sort"
	"strings"
	"sync"
)

// SuggestEngine provides prefix-based autocomplete suggestions
// built from indexed document titles and high-frequency terms.
type SuggestEngine struct {
	mu    sync.RWMutex
	terms map[string]int // term → frequency count
}

// NewSuggestEngine creates an empty suggest engine.
func NewSuggestEngine() *SuggestEngine {
	return &SuggestEngine{
		terms: make(map[string]int),
	}
}

// AddDocument extracts terms from a document's text and title for suggestion.
func (s *SuggestEngine) AddDocument(title, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Index title words with high weight
	for _, word := range tokenize(strings.ToLower(title)) {
		if len(word) >= 2 {
			s.terms[word] += 10
		}
	}

	// Index content words with lower weight
	for _, word := range tokenize(strings.ToLower(text)) {
		if len(word) >= 3 {
			s.terms[word]++
		}
	}

	// Index bigrams from title for phrase suggestions
	titleWords := tokenize(strings.ToLower(title))
	for i := 0; i < len(titleWords)-1; i++ {
		bigram := titleWords[i] + " " + titleWords[i+1]
		s.terms[bigram] += 5
	}
}

// Suggest returns up to limit suggestions matching the prefix, sorted by frequency.
func (s *SuggestEngine) Suggest(prefix string, limit int) []Suggestion {
	if prefix == "" {
		return nil
	}
	if limit <= 0 {
		limit = 10
	}

	prefix = strings.ToLower(prefix)

	s.mu.RLock()
	defer s.mu.RUnlock()

	var matches []Suggestion
	for term, count := range s.terms {
		if strings.HasPrefix(term, prefix) {
			matches = append(matches, Suggestion{Text: term, Score: count})
		}
	}

	// Sort by frequency descending
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Score > matches[j].Score
	})

	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches
}

// Len returns the number of unique terms in the suggest index.
func (s *SuggestEngine) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.terms)
}

// Suggestion is a single autocomplete suggestion.
type Suggestion struct {
	Text  string `json:"text"`
	Score int    `json:"score"`
}
