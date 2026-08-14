package chunking

import (
	"strings"
	"sync"

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

// SemanticChunker splits text into semantically coherent chunks using cosine
// similarity valley detection between adjacent sentence embeddings.
//
// Uses neurosnap/sentences (English tokenizer with built-in training data) for
// production-grade sentence boundary detection that correctly handles
// abbreviations (Dr., Mr., U.S.), decimal numbers (3.14), URLs, and Unicode.
func SemanticChunker(text string, embedder func(string) ([]float32, error), threshold float32) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}

	tokenizer, err := getTokenizer()
	if err != nil {
		return nil, err
	}

	sentenceList := tokenizer.Tokenize(text)
	if len(sentenceList) == 0 {
		return nil, nil
	}

	// Extract text from sentence tokens
	sTexts := make([]string, len(sentenceList))
	for i, s := range sentenceList {
		sTexts[i] = strings.TrimSpace(s.Text)
	}

	// Single sentence → single chunk
	if len(sTexts) == 1 {
		return []string{sTexts[0]}, nil
	}

	// Embed each sentence
	embeddings := make([][]float32, len(sTexts))
	for i, s := range sTexts {
		vec, err := embedder(s)
		if err != nil {
			return nil, err
		}
		embeddings[i] = vec
	}

	// Valley detection: split where cosine similarity drops below threshold
	var chunks []string
	currentChunk := sTexts[0]

	for i := 1; i < len(embeddings); i++ {
		sim := vector.CosineSimilarity(embeddings[i-1], embeddings[i])
		if sim < threshold {
			// Topic shift detected — finalize current chunk
			chunks = append(chunks, currentChunk)
			currentChunk = sTexts[i]
		} else {
			// Same topic — aggregate
			currentChunk += " " + sTexts[i]
		}
	}
	chunks = append(chunks, currentChunk)
	return chunks, nil
}
