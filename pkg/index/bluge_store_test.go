package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func tempBlugeStore(t *testing.T) (*BlugeStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "bluge-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	bs, err := NewBlugeStore(filepath.Join(dir, "test.bluge"))
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewBlugeStore: %v", err)
	}
	return bs, func() {
		bs.Close()
		os.RemoveAll(dir)
	}
}

func TestIndexAndSearchSingleDoc(t *testing.T) {
	bs, cleanup := tempBlugeStore(t)
	defer cleanup()

	err := bs.IndexDocument("1", "the quick brown fox jumps over the lazy dog", nil)
	if err != nil {
		t.Fatalf("IndexDocument: %v", err)
	}

	hits, err := bs.Search(context.Background(), "quick fox", nil, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(hits) == 0 {
		t.Fatal("expected at least 1 hit")
	}
	if hits[0].ID != "1" {
		t.Errorf("expected ID '1', got %q", hits[0].ID)
	}
	if hits[0].Score <= 0 {
		t.Errorf("expected positive BM25 score, got %f", hits[0].Score)
	}
}

func TestKeywordFilter(t *testing.T) {
	bs, cleanup := tempBlugeStore(t)
	defer cleanup()

	bs.IndexDocument("1", "golang programming", map[string]string{"lang": "go"})
	bs.IndexDocument("2", "python programming", map[string]string{"lang": "python"})
	bs.IndexDocument("3", "rust programming", map[string]string{"lang": "go"})

	hits, err := bs.Search(context.Background(), "", map[string]string{"lang": "go"}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(hits) != 2 {
		t.Fatalf("expected 2 hits for lang=go, got %d", len(hits))
	}

	for _, h := range hits {
		if h.ID != "1" && h.ID != "3" {
			t.Errorf("unexpected ID %q in results", h.ID)
		}
	}
}

func TestBooleanMustAndShould(t *testing.T) {
	bs, cleanup := tempBlugeStore(t)
	defer cleanup()

	bs.IndexDocument("1", "advanced golang concurrency patterns", map[string]string{"category": "tutorial"})
	bs.IndexDocument("2", "golang beginner guide", map[string]string{"category": "tutorial"})
	bs.IndexDocument("3", "advanced golang for experts", map[string]string{"category": "reference"})

	// Must: category=tutorial, Should: "advanced"
	hits, err := bs.Search(context.Background(), "advanced", map[string]string{"category": "tutorial"}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	// Both tutorials should match (MUST), but the "advanced" one should score higher (SHOULD)
	if len(hits) < 1 {
		t.Fatal("expected at least 1 hit")
	}

	// The "reference" doc should not appear (fails MUST filter)
	for _, h := range hits {
		if h.ID == "3" {
			t.Error("reference doc should not match category=tutorial filter")
		}
	}
}

func TestEmptyFilterReturnsAll(t *testing.T) {
	bs, cleanup := tempBlugeStore(t)
	defer cleanup()

	bs.IndexDocument("1", "document one", nil)
	bs.IndexDocument("2", "document two", nil)
	bs.IndexDocument("3", "document three", nil)

	hits, err := bs.Search(context.Background(), "", nil, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 3 {
		t.Errorf("expected 3 hits, got %d", len(hits))
	}
}

func TestBM25Scoring(t *testing.T) {
	bs, cleanup := tempBlugeStore(t)
	defer cleanup()

	// Doc with higher term frequency of "golang" should score higher
	bs.IndexDocument("1", "golang", nil)
	bs.IndexDocument("2", "golang golang golang golang golang", nil)

	hits, err := bs.Search(context.Background(), "golang", nil, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(hits) < 2 {
		t.Fatalf("expected 2 hits, got %d", len(hits))
	}

	// Higher TF doc should have higher BM25 score
	var score1, score2 float64
	for _, h := range hits {
		if h.ID == "1" {
			score1 = h.Score
		} else if h.ID == "2" {
			score2 = h.Score
		}
	}
	if score2 <= score1 {
		t.Errorf("expected doc 2 (higher TF) to score higher: doc1=%f, doc2=%f", score1, score2)
	}
}

func TestSearchWithScores(t *testing.T) {
	bs, cleanup := tempBlugeStore(t)
	defer cleanup()

	bs.IndexDocument("10", "search engine design", map[string]string{"topic": "tech"})
	bs.IndexDocument("20", "cooking recipes", map[string]string{"topic": "food"})

	scores, err := bs.SearchWithScores(context.Background(), "search engine", map[string]string{"topic": "tech"}, 100)
	if err != nil {
		t.Fatalf("SearchWithScores: %v", err)
	}

	if _, ok := scores[10]; !ok {
		t.Error("expected doc 10 in results")
	}
	if _, ok := scores[20]; ok {
		t.Error("doc 20 should not match topic=tech filter")
	}
}
