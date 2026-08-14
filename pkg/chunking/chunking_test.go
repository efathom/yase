package chunking

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/smacker/go-tree-sitter/golang"
)

// mockEmbedder returns a deterministic pseudo-random embedding for testing.
// Texts starting with the same first word get similar embeddings.
func mockEmbedder(dim int) func(string) ([]float32, error) {
	cache := make(map[string][]float32)
	return func(text string) ([]float32, error) {
		if v, ok := cache[text]; ok {
			return v, nil
		}
		// Use text hash as seed for deterministic embeddings
		seed := int64(0)
		for _, c := range text {
			seed = seed*31 + int64(c)
		}
		rng := rand.New(rand.NewSource(seed))
		v := make([]float32, dim)
		for i := range v {
			v[i] = rng.Float32()*2 - 1
		}
		cache[text] = v
		return v, nil
	}
}

// topicEmbedder returns high-similarity embeddings for sentences in the same
// topic and low-similarity for different topics.
func topicEmbedder() func(string) ([]float32, error) {
	return func(text string) ([]float32, error) {
		dim := 64
		v := make([]float32, dim)
		if strings.Contains(text, "physics") || strings.Contains(text, "gravity") || strings.Contains(text, "Newton") {
			// Topic A: physics
			for i := range v {
				v[i] = float32(i) * 0.1
			}
		} else if strings.Contains(text, "cooking") || strings.Contains(text, "recipe") || strings.Contains(text, "bake") {
			// Topic B: cooking
			for i := range v {
				v[i] = -float32(i) * 0.1
			}
		} else {
			// Neutral
			rng := rand.New(rand.NewSource(42))
			for i := range v {
				v[i] = rng.Float32()
			}
		}
		return v, nil
	}
}

func TestSemanticChunkerTopicShift(t *testing.T) {
	text := "Newton discovered gravity. The physics of motion are fundamental. Meanwhile, cooking a recipe requires patience. You should bake at 350 degrees."
	embedder := topicEmbedder()

	chunks, err := SemanticChunker(text, embedder, 0.5)
	if err != nil {
		t.Fatalf("SemanticChunker: %v", err)
	}

	if len(chunks) < 2 {
		t.Errorf("expected at least 2 chunks for topic shift, got %d: %v", len(chunks), chunks)
	}
	t.Logf("chunks: %v", chunks)
}

func TestSemanticChunkerAbbreviations(t *testing.T) {
	text := "Dr. Smith went to the U.S. for 3.14 reasons."
	embedder := mockEmbedder(64)

	chunks, err := SemanticChunker(text, embedder, 0.3)
	if err != nil {
		t.Fatalf("SemanticChunker: %v", err)
	}

	// The text should stay as a single chunk — abbreviations are not sentence boundaries
	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk (abbreviations should not split), got %d: %v", len(chunks), chunks)
	}
}

func TestSemanticChunkerEmptyInput(t *testing.T) {
	embedder := mockEmbedder(64)
	chunks, err := SemanticChunker("", embedder, 0.5)
	if err != nil {
		t.Fatalf("SemanticChunker: %v", err)
	}
	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks for empty input, got %d", len(chunks))
	}
}

func TestSemanticChunkerSingleSentence(t *testing.T) {
	embedder := mockEmbedder(64)
	chunks, err := SemanticChunker("This is a single sentence.", embedder, 0.5)
	if err != nil {
		t.Fatalf("SemanticChunker: %v", err)
	}
	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(chunks))
	}
}

func TestASTChunkerGoFunctions(t *testing.T) {
	code := []byte(`package main

func hello() {
	println("hello")
}

func world() {
	println("world")
}

func add(a, b int) int {
	return a + b
}
`)
	chunks := ASTChunker(code, golang.GetLanguage())
	if len(chunks) != 3 {
		t.Errorf("expected 3 chunks, got %d", len(chunks))
		for i, c := range chunks {
			t.Logf("chunk %d: %s", i, c)
		}
	}
}

func TestASTChunkerPreservesScope(t *testing.T) {
	// A single large function should stay as one chunk
	var sb strings.Builder
	sb.WriteString("package main\n\nfunc bigFunc() {\n")
	for i := 0; i < 50; i++ {
		sb.WriteString("\tx := " + strings.Repeat("a", 20) + "\n")
	}
	sb.WriteString("}\n")

	chunks := ASTChunker([]byte(sb.String()), golang.GetLanguage())
	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk for single function, got %d", len(chunks))
	}
}

func TestASTChunkerEmptyInput(t *testing.T) {
	chunks := ASTChunker([]byte("package main\n"), golang.GetLanguage())
	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks for code with no functions, got %d", len(chunks))
	}
}

func TestASTChunkerNilLanguage(t *testing.T) {
	// Passing nil language should not panic
	chunks := ASTChunker([]byte("func foo() {}"), nil)
	// tree-sitter with nil language may panic or return nil — just ensure no crash
	_ = chunks
}
