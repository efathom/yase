package chunking

import (
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/efathom/yase/pkg/vector"
	"github.com/neurosnap/sentences"
	"github.com/neurosnap/sentences/english"
)

var (
	sentenceTokenizer *sentences.DefaultSentenceTokenizer
	tokenizerOnce     sync.Once
	tokenizerErr      error
)

func getTokenizer() (*sentences.DefaultSentenceTokenizer, error) {
	tokenizerOnce.Do(func() {
		sentenceTokenizer, tokenizerErr = english.NewSentenceTokenizer(nil)
	})
	return sentenceTokenizer, tokenizerErr
}

// Options bounds semantic chunking.
type Options struct {
	// Threshold is the adjacent-sentence cosine similarity below which a
	// topic shift starts a new chunk (default 0.75).
	Threshold float32
	// MaxChars caps chunk length so a chunk always fits the embedding model's
	// context window (default 2000 characters ≈ 500 tokens). Sentences longer
	// than this are split at word boundaries.
	MaxChars int
	// MinChars merges chunks shorter than this into a neighbour so retrieval
	// units are not single fragments (default 0 = disabled).
	MinChars int
	// OverlapSentences repeats the last N sentences of a chunk at the start of
	// the next one, preserving context across boundaries (default 0).
	OverlapSentences int
}

// DefaultOptions are suitable for ~512-token embedding models.
func DefaultOptions() Options {
	return Options{Threshold: 0.75, MaxChars: 2000, MinChars: 200}
}

// BatchEmbedFunc embeds many texts in one call.
type BatchEmbedFunc func(texts []string) ([][]float32, error)

// SemanticChunker splits text into semantically coherent chunks using cosine
// similarity valley detection between adjacent sentence embeddings.
//
// Uses neurosnap/sentences (English tokenizer with built-in training data) for
// sentence boundary detection that handles abbreviations (Dr., Mr., U.S.),
// decimal numbers (3.14), URLs, and Unicode. Chunks are capped at the default
// MaxChars; use SemanticChunkerWithOptions for full control.
func SemanticChunker(text string, embedder func(string) ([]float32, error), threshold float32) ([]string, error) {
	batch := func(texts []string) ([][]float32, error) {
		out := make([][]float32, len(texts))
		for i, t := range texts {
			v, err := embedder(t)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	opts := DefaultOptions()
	opts.Threshold = threshold
	opts.MinChars = 0
	return SemanticChunkerWithOptions(text, batch, opts)
}

// SemanticChunkerWithOptions is SemanticChunker with explicit bounds and a
// batch embedder: all sentences are embedded in a single call.
func SemanticChunkerWithOptions(text string, embed BatchEmbedFunc, opts Options) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	if opts.Threshold <= 0 {
		opts.Threshold = 0.75
	}
	if opts.MaxChars <= 0 {
		opts.MaxChars = 2000
	}

	tokenizer, err := getTokenizer()
	if err != nil {
		return nil, err
	}

	var sents []string
	for _, s := range tokenizer.Tokenize(text) {
		t := strings.TrimSpace(s.Text)
		if t == "" {
			continue
		}
		sents = append(sents, splitLong(t, opts.MaxChars)...)
	}
	if len(sents) == 0 {
		return nil, nil
	}
	if len(sents) == 1 {
		return sents, nil
	}

	embeddings, err := embed(sents)
	if err != nil {
		return nil, err
	}
	if len(embeddings) != len(sents) {
		return nil, fmt.Errorf("embedder returned %d vectors for %d sentences", len(embeddings), len(sents))
	}

	// Group sentence indices into chunks: split on a similarity valley or
	// when the chunk would exceed MaxChars.
	var groups [][]int
	cur := []int{0}
	curLen := len(sents[0])
	for i := 1; i < len(sents); i++ {
		sim := vector.CosineSimilarity(embeddings[i-1], embeddings[i])
		if sim < opts.Threshold || curLen+1+len(sents[i]) > opts.MaxChars {
			groups = append(groups, cur)
			cur, curLen = []int{i}, len(sents[i])
			continue
		}
		cur = append(cur, i)
		curLen += 1 + len(sents[i])
	}
	groups = append(groups, cur)

	groups = mergeSmall(groups, sents, opts.MinChars, opts.MaxChars)
	return render(groups, sents, opts.OverlapSentences, opts.MaxChars), nil
}

func groupLen(g []int, sents []string) int {
	n := 0
	for _, i := range g {
		n += len(sents[i]) + 1
	}
	return n
}

// mergeSmall folds groups shorter than minChars into the following group (or
// the previous one, for the last group) when the result still fits maxChars.
func mergeSmall(groups [][]int, sents []string, minChars, maxChars int) [][]int {
	if minChars <= 0 || len(groups) < 2 {
		return groups
	}
	out := make([][]int, 0, len(groups))
	for i := 0; i < len(groups); i++ {
		g := groups[i]
		for groupLen(g, sents) < minChars && i+1 < len(groups) &&
			groupLen(g, sents)+groupLen(groups[i+1], sents) <= maxChars {
			i++
			g = append(append([]int(nil), g...), groups[i]...)
		}
		out = append(out, g)
	}
	if n := len(out); n >= 2 {
		last := out[n-1]
		if groupLen(last, sents) < minChars && groupLen(out[n-2], sents)+groupLen(last, sents) <= maxChars {
			out[n-2] = append(out[n-2], last...)
			out = out[:n-1]
		}
	}
	return out
}

func render(groups [][]int, sents []string, overlap, maxChars int) []string {
	chunks := make([]string, 0, len(groups))
	for gi, g := range groups {
		idx := g
		if overlap > 0 && gi > 0 {
			prev := groups[gi-1]
			start := len(prev) - overlap
			if start < 0 {
				start = 0
			}
			withOverlap := append(append([]int(nil), prev[start:]...), g...)
			if groupLen(withOverlap, sents) <= maxChars {
				idx = withOverlap
			}
		}
		parts := make([]string, len(idx))
		for i, si := range idx {
			parts[i] = sents[si]
		}
		chunks = append(chunks, strings.Join(parts, " "))
	}
	return chunks
}

// splitLong breaks s into pieces of at most maxChars, preferring whitespace.
func splitLong(s string, maxChars int) []string {
	if len(s) <= maxChars {
		return []string{s}
	}
	var out []string
	for len(s) > maxChars {
		cut := strings.LastIndexAny(s[:maxChars], " \t\n")
		if cut <= 0 {
			cut = maxChars
			for cut > 0 && !utf8.RuneStart(s[cut]) {
				cut-- // never split inside a UTF-8 sequence
			}
		}
		out = append(out, strings.TrimSpace(s[:cut]))
		s = strings.TrimSpace(s[cut:])
	}
	if s != "" {
		out = append(out, s)
	}
	return out
}
