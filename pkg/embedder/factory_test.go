package embedder

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/efathom/yase/pkg/config"
)

// newMockAPIServer returns a test server that mimics the OpenAI /v1/embeddings endpoint.
func newMockAPIServer(t *testing.T, wantModel string, wantAuthPrefix string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if wantAuthPrefix != "" {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				t.Error("missing Authorization header")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		var req embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if wantModel != "" && req.Model != wantModel {
			t.Errorf("model = %q, want %q", req.Model, wantModel)
		}

		resp := embeddingResponse{}
		for i := range req.Input {
			resp.Data = append(resp.Data, struct {
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{
				Embedding: []float32{0.1, 0.2, 0.3},
				Index:     i,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
}

func TestNewFromConfig_Mock(t *testing.T) {
	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "mock",
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	if emb.Dimension() != 128 {
		t.Errorf("dimension = %d, want 128", emb.Dimension())
	}
	vec, err := emb.Embed(context.Background(), "test")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 128 {
		t.Errorf("vec len = %d, want 128", len(vec))
	}
}

func TestNewFromConfig_Ollama(t *testing.T) {
	server := newMockAPIServer(t, "nomic-embed-text", "Bearer ollama")
	defer server.Close()

	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "ollama",
		Model:     "nomic-embed-text",
		Dimension: 3,
		BaseURL:   server.URL,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	vec, err := emb.Embed(context.Background(), "test ollama")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 3 {
		t.Errorf("vec len = %d, want 3", len(vec))
	}
	if vec[0] != 0.1 || vec[1] != 0.2 || vec[2] != 0.3 {
		t.Errorf("unexpected vector: %v", vec)
	}
}

func TestNewFromConfig_TEI(t *testing.T) {
	server := newMockAPIServer(t, "gte-modernbert-base", "Bearer tei")
	defer server.Close()

	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "tei",
		Model:     "gte-modernbert-base",
		Dimension: 3,
		BaseURL:   server.URL,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	vec, err := emb.Embed(context.Background(), "test tei")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 3 {
		t.Errorf("vec len = %d, want 3", len(vec))
	}
	if vec[0] != 0.1 || vec[1] != 0.2 || vec[2] != 0.3 {
		t.Errorf("unexpected vector: %v", vec)
	}
}

func TestNewFromConfig_TEIDefaultURL(t *testing.T) {
	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "tei",
		Model:     "gte-modernbert-base",
		Dimension: 768,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	if emb.Dimension() != 768 {
		t.Errorf("dimension = %d, want 768", emb.Dimension())
	}
}

func TestNewFromConfig_OllamaDefaultURL(t *testing.T) {
	// When BaseURL is the OpenAI default, Ollama should override to localhost:11434
	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "ollama",
		Model:     "nomic-embed-text",
		Dimension: 768,
		BaseURL:   "https://api.openai.com/v1",
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	// Can't easily verify the URL without calling it, but it should create successfully
	if emb.Dimension() != 768 {
		t.Errorf("dimension = %d, want 768", emb.Dimension())
	}
}

func TestNewFromConfig_OpenAI(t *testing.T) {
	server := newMockAPIServer(t, "text-embedding-3-small", "Bearer test-key")
	defer server.Close()

	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "openai",
		Model:     "text-embedding-3-small",
		APIKey:    "test-key",
		Dimension: 3,
		BaseURL:   server.URL,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	vec, err := emb.Embed(context.Background(), "test openai")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 3 {
		t.Errorf("vec len = %d, want 3", len(vec))
	}
}

func TestNewFromConfig_DefaultProvider(t *testing.T) {
	// Empty provider string should default to openai
	server := newMockAPIServer(t, "text-embedding-3-small", "")
	defer server.Close()

	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "",
		Model:     "text-embedding-3-small",
		APIKey:    "key",
		Dimension: 3,
		BaseURL:   server.URL,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	vec, err := emb.Embed(context.Background(), "test default")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 3 {
		t.Errorf("vec len = %d, want 3", len(vec))
	}
}

func TestNewFromConfig_Unknown(t *testing.T) {
	_, err := NewFromConfig(config.EmbedderConfig{
		Provider: "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestNewFromConfig_WithCache(t *testing.T) {
	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "mock",
		Dimension: 64,
		CacheSize: 100,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	// Verify it's wrapped in a CachedEmbedder
	cached, ok := emb.(*CachedEmbedder)
	if !ok {
		t.Fatalf("expected *CachedEmbedder, got %T", emb)
	}

	ctx := context.Background()
	cached.Embed(ctx, "a")
	cached.Embed(ctx, "b")
	if cached.Len() != 2 {
		t.Errorf("cache len = %d, want 2", cached.Len())
	}
}

func TestNewFromConfig_NoCacheWhenZero(t *testing.T) {
	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "mock",
		Dimension: 64,
		CacheSize: 0,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	if _, ok := emb.(*CachedEmbedder); ok {
		t.Error("expected no cache wrapping when CacheSize is 0")
	}
}

func TestNewFromConfig_OllamaBatchSize(t *testing.T) {
	var batchSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req embeddingRequest
		json.NewDecoder(r.Body).Decode(&req)
		batchSizes = append(batchSizes, len(req.Input))

		resp := embeddingResponse{}
		for i := range req.Input {
			resp.Data = append(resp.Data, struct {
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{
				Embedding: []float32{0.1},
				Index:     i,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	emb, err := NewFromConfig(config.EmbedderConfig{
		Provider:  "ollama",
		Model:     "test-model",
		Dimension: 1,
		BaseURL:   server.URL,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	// Send 300 texts — should be split into batches of 256 + 44
	texts := make([]string, 300)
	for i := range texts {
		texts[i] = "text"
	}

	_, err = emb.EmbedBatch(context.Background(), texts)
	if err != nil {
		t.Fatalf("EmbedBatch: %v", err)
	}

	if len(batchSizes) != 2 {
		t.Fatalf("expected 2 API calls, got %d", len(batchSizes))
	}
	if batchSizes[0] != 256 {
		t.Errorf("first batch = %d, want 256", batchSizes[0])
	}
	if batchSizes[1] != 44 {
		t.Errorf("second batch = %d, want 44", batchSizes[1])
	}
}
