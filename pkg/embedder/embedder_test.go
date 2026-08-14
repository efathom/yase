package embedder

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMockDeterministic(t *testing.T) {
	mock := NewMockEmbedder(128)
	ctx := context.Background()

	vec1, err := mock.Embed(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	vec2, err := mock.Embed(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}

	if len(vec1) != 128 {
		t.Fatalf("expected 128-dim, got %d", len(vec1))
	}
	for i := range vec1 {
		if vec1[i] != vec2[i] {
			t.Fatalf("vectors differ at index %d: %f vs %f", i, vec1[i], vec2[i])
		}
	}
}

func TestMockDifferentInputs(t *testing.T) {
	mock := NewMockEmbedder(64)
	ctx := context.Background()

	vec1, _ := mock.Embed(ctx, "hello")
	vec2, _ := mock.Embed(ctx, "world")

	same := true
	for i := range vec1 {
		if vec1[i] != vec2[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("different inputs should produce different vectors")
	}
}

func TestMockNormalized(t *testing.T) {
	mock := NewMockEmbedder(256)
	vec, _ := mock.Embed(context.Background(), "test normalization")

	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	norm = math.Sqrt(norm)

	if math.Abs(norm-1.0) > 0.01 {
		t.Errorf("expected unit norm, got %f", norm)
	}
}

func TestMockBatch(t *testing.T) {
	mock := NewMockEmbedder(32)
	texts := []string{"a", "b", "c"}

	vecs, err := mock.EmbedBatch(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 3 {
		t.Fatalf("expected 3 vectors, got %d", len(vecs))
	}
	for i, v := range vecs {
		if len(v) != 32 {
			t.Errorf("vec[%d] has dim %d, want 32", i, len(v))
		}
	}
}

func TestMockDimension(t *testing.T) {
	mock := NewMockEmbedder(768)
	if mock.Dimension() != 768 {
		t.Errorf("expected dimension 768, got %d", mock.Dimension())
	}
}

func TestCacheHit(t *testing.T) {
	inner := NewMockEmbedder(64)
	cached := NewCachedEmbedder(inner, 100)
	ctx := context.Background()

	// First call — cache miss
	vec1, err := cached.Embed(ctx, "cached text")
	if err != nil {
		t.Fatal(err)
	}
	if cached.Len() != 1 {
		t.Errorf("expected 1 cache entry, got %d", cached.Len())
	}

	// Second call — cache hit
	vec2, err := cached.Embed(ctx, "cached text")
	if err != nil {
		t.Fatal(err)
	}

	for i := range vec1 {
		if vec1[i] != vec2[i] {
			t.Fatalf("cache hit returned different vector at index %d", i)
		}
	}
}

func TestCacheMiss(t *testing.T) {
	inner := NewMockEmbedder(64)
	cached := NewCachedEmbedder(inner, 100)
	ctx := context.Background()

	cached.Embed(ctx, "text A")
	cached.Embed(ctx, "text B")

	if cached.Len() != 2 {
		t.Errorf("expected 2 cache entries, got %d", cached.Len())
	}
}

func TestCacheEviction(t *testing.T) {
	inner := NewMockEmbedder(32)
	cached := NewCachedEmbedder(inner, 3)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cached.Embed(ctx, fmt.Sprintf("text-%d", i))
	}

	if cached.Len() != 3 {
		t.Errorf("expected cache capped at 3, got %d", cached.Len())
	}
}

func TestCacheBatch(t *testing.T) {
	inner := NewMockEmbedder(32)
	cached := NewCachedEmbedder(inner, 100)
	ctx := context.Background()

	// Pre-populate one entry
	cached.Embed(ctx, "a")

	// Batch with one hit and two misses
	vecs, err := cached.EmbedBatch(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 3 {
		t.Fatalf("expected 3 vectors, got %d", len(vecs))
	}
	if cached.Len() != 3 {
		t.Errorf("expected 3 cache entries, got %d", cached.Len())
	}
}

func TestOpenAIEmbedBatch(t *testing.T) {
	// Mock OpenAI API server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var req embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
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
	defer server.Close()

	emb := NewOpenAIEmbedder("test-key", "text-embedding-3-small", 3,
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)

	vecs, err := emb.EmbedBatch(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("EmbedBatch error: %v", err)
	}
	if len(vecs) != 2 {
		t.Fatalf("expected 2 vectors, got %d", len(vecs))
	}
	if len(vecs[0]) != 3 {
		t.Errorf("expected 3-dim vector, got %d", len(vecs[0]))
	}
	if vecs[0][0] != 0.1 || vecs[0][1] != 0.2 || vecs[0][2] != 0.3 {
		t.Errorf("unexpected vector values: %v", vecs[0])
	}
}

func TestOpenAIRetryOn429(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":{"message":"rate limited"}}`))
			return
		}

		var req embeddingRequest
		json.NewDecoder(r.Body).Decode(&req)

		resp := embeddingResponse{}
		for i := range req.Input {
			resp.Data = append(resp.Data, struct {
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{
				Embedding: []float32{1.0},
				Index:     i,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	emb := NewOpenAIEmbedder("key", "model", 1,
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)

	vec, err := emb.Embed(context.Background(), "retry test")
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if vec[0] != 1.0 {
		t.Errorf("expected 1.0, got %f", vec[0])
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls (2 retries + 1 success), got %d", callCount)
	}
}

func TestOpenAIDimension(t *testing.T) {
	emb := NewOpenAIEmbedder("key", "model", 1536)
	if emb.Dimension() != 1536 {
		t.Errorf("expected 1536, got %d", emb.Dimension())
	}
}
