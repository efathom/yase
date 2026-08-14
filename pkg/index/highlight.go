package index

import (
	"strings"
	"unicode"
)

// HighlightConfig controls snippet generation.
type HighlightConfig struct {
	MaxFragments   int    // max fragments per result (default 3)
	FragmentSize   int    // characters per fragment (default 150)
	PreTag         string // highlight start tag (default "<mark>")
	PostTag        string // highlight end tag (default "</mark>")
}

// DefaultHighlightConfig returns sensible defaults.
func DefaultHighlightConfig() HighlightConfig {
	return HighlightConfig{
		MaxFragments: 3,
		FragmentSize: 150,
		PreTag:       "<mark>",
		PostTag:      "</mark>",
	}
}

// Highlight extracts fragments from content that contain query terms,
// wrapping matched terms with pre/post tags.
// Returns up to MaxFragments snippets.
func Highlight(content, query string, cfg HighlightConfig) []string {
	if content == "" || query == "" {
		return nil
	}

	if cfg.MaxFragments <= 0 {
		cfg.MaxFragments = 3
	}
	if cfg.FragmentSize <= 0 {
		cfg.FragmentSize = 150
	}
	if cfg.PreTag == "" {
		cfg.PreTag = "<mark>"
	}
	if cfg.PostTag == "" {
		cfg.PostTag = "</mark>"
	}

	// Tokenize query into terms
	queryTerms := tokenize(strings.ToLower(query))
	if len(queryTerms) == 0 {
		return nil
	}

	termSet := make(map[string]bool, len(queryTerms))
	for _, t := range queryTerms {
		termSet[t] = true
	}

	// Find positions of query terms in content
	contentLower := strings.ToLower(content)
	words := tokenize(contentLower)

	type match struct {
		pos  int // byte position in content
		term string
	}
	var matches []match

	searchFrom := 0
	for _, word := range words {
		if termSet[word] {
			idx := strings.Index(contentLower[searchFrom:], word)
			if idx >= 0 {
				matches = append(matches, match{pos: searchFrom + idx, term: word})
				searchFrom = searchFrom + idx + len(word)
			}
		}
	}

	if len(matches) == 0 {
		// No matches — return first fragment as context
		frag := content
		if len(frag) > cfg.FragmentSize {
			frag = frag[:cfg.FragmentSize] + "..."
		}
		return []string{frag}
	}

	// Extract fragments around each match
	var fragments []string
	used := make(map[int]bool) // avoid overlapping fragments

	for _, m := range matches {
		if len(fragments) >= cfg.MaxFragments {
			break
		}

		// Find fragment boundaries
		fragStart := m.pos - cfg.FragmentSize/2
		if fragStart < 0 {
			fragStart = 0
		}
		fragEnd := fragStart + cfg.FragmentSize
		if fragEnd > len(content) {
			fragEnd = len(content)
		}

		// Snap to word boundaries
		for fragStart > 0 && !unicode.IsSpace(rune(content[fragStart])) {
			fragStart++
		}
		for fragEnd < len(content) && !unicode.IsSpace(rune(content[fragEnd-1])) {
			fragEnd++
			if fragEnd > len(content) {
				fragEnd = len(content)
				break
			}
		}

		// Skip if overlaps with existing fragment
		bucket := fragStart / cfg.FragmentSize
		if used[bucket] {
			continue
		}
		used[bucket] = true

		fragment := content[fragStart:fragEnd]
		fragment = strings.TrimSpace(fragment)

		// Highlight terms in fragment
		fragment = highlightTerms(fragment, termSet, cfg.PreTag, cfg.PostTag)

		prefix := ""
		suffix := ""
		if fragStart > 0 {
			prefix = "..."
		}
		if fragEnd < len(content) {
			suffix = "..."
		}

		fragments = append(fragments, prefix+fragment+suffix)
	}

	return fragments
}

// highlightTerms wraps matching terms in the fragment with pre/post tags.
func highlightTerms(fragment string, terms map[string]bool, preTag, postTag string) string {
	words := strings.Fields(fragment)
	for i, word := range words {
		clean := strings.ToLower(strings.TrimFunc(word, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}))
		if terms[clean] {
			// Case-preserving replacement
			idx := strings.Index(strings.ToLower(word), clean)
			if idx >= 0 {
				original := word[idx : idx+len(clean)]
				words[i] = word[:idx] + preTag + original + postTag + word[idx+len(clean):]
			}
		}
	}
	return strings.Join(words, " ")
}

// tokenize splits text into lowercase word tokens.
func tokenize(text string) []string {
	return strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
