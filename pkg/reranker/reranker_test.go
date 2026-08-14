package reranker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/efathom/yase/pkg/config"
)

func TestTEIRerankerRerank(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rerank" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var req rerankRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if req.Query == "" {
			t.Error("expected non-empty query")
		}

		// Return reversed order (last text is most relevant)
		resp := make([]rerankResponseItem, len(req.Texts))
		for i := range req.Texts {
			resp[i] = rerankResponseItem{
				Index: len(req.Texts) - 1 - i,
				Score: float32(len(req.Texts)-i) * 0.1,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	r := NewTEIReranker(server.URL, "test-model",
		WithTEIHTTPClient(server.Client()))

	results, err := r.Rerank(context.Background(), "test query", []string{"doc0", "doc1", "doc2"})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First result should be index 2 (reversed)
	if results[0].Index != 2 {
		t.Errorf("results[0].Index = %d, want 2", results[0].Index)
	}
	if results[0].Score != 0.3 {
		t.Errorf("results[0].Score = %f, want 0.3", results[0].Score)
	}
}

func TestTEIRerankerEmpty(t *testing.T) {
	r := NewTEIReranker("http://unused", "model")
	results, err := r.Rerank(context.Background(), "query", nil)
	if err != nil {
		t.Fatalf("Rerank empty: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestTEIRerankerRetryOn429(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("rate limited"))
			return
		}

		resp := []rerankResponseItem{{Index: 0, Score: 0.9}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	r := NewTEIReranker(server.URL, "model",
		WithTEIHTTPClient(server.Client()))

	results, err := r.Rerank(context.Background(), "query", []string{"doc"})
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls (2 retries + success), got %d", callCount)
	}
}

func TestMockReranker(t *testing.T) {
	m := NewMockReranker()
	results, err := m.Rerank(context.Background(), "query", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Mock returns original order
	for i, r := range results {
		if r.Index != i {
			t.Errorf("results[%d].Index = %d, want %d", i, r.Index, i)
		}
	}

	// Scores should decrease
	if results[0].Score <= results[2].Score {
		t.Error("expected decreasing scores")
	}
}

func TestNewFromConfig_TEI(t *testing.T) {
	r := NewFromConfig(config.RerankerConfig{
		Enabled:  true,
		Provider: "tei",
		BaseURL:  "http://localhost:8081",
		Timeout:  2 * time.Second,
	})
	if r == nil {
		t.Fatal("expected non-nil reranker for enabled tei provider")
	}
}

func TestNewFromConfig_Mock(t *testing.T) {
	r := NewFromConfig(config.RerankerConfig{
		Enabled:  true,
		Provider: "mock",
	})
	if r == nil {
		t.Fatal("expected non-nil reranker for mock provider")
	}
}

func TestNewFromConfig_Disabled(t *testing.T) {
	r := NewFromConfig(config.RerankerConfig{
		Enabled: false,
	})
	if r != nil {
		t.Error("expected nil reranker when disabled")
	}
}

func TestNewFromConfig_UnknownProvider(t *testing.T) {
	r := NewFromConfig(config.RerankerConfig{
		Enabled:  true,
		Provider: "nonexistent",
	})
	if r != nil {
		t.Error("expected nil reranker for unknown provider")
	}
}
