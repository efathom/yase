package gateway

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/memory"
)

type ragTestSearcher struct {
	engine *index.HybridEngine
}

func (s *ragTestSearcher) HybridSearch(ctx context.Context, textQuery string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	return s.engine.HybridSearch(ctx, textQuery, queryVec, filters, topK)
}

func (s *ragTestSearcher) NodeCount() int {
	return s.engine.Graph.Len()
}

func (s *ragTestSearcher) GetDocumentsByIDs(ctx context.Context, ids []uint32) (map[uint32]index.SearchHit, error) {
	return s.engine.BlugeStore.GetDocumentsByIDs(ctx, ids)
}

func setupRAGTest(t *testing.T) (*Handler, func()) {
	t.Helper()
	dir := t.TempDir()
	const dim = 32

	arena, err := memory.NewOffHeapArena(1 << 20)
	if err != nil {
		t.Fatal(err)
	}

	engine, err := index.NewHybridEngineWithArena(filepath.Join(dir, "bluge"), arena, dim, 5)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	docs := []struct {
		id   uint32
		text string
		url  string
	}{
		{0, "Go is a statically typed compiled language", "https://go.dev/"},
		{5, "HNSW builds a multi-layer navigable small world graph", "https://arxiv.org/abs/1603.09320"},
		{10, "BM25 is a ranking function used in information retrieval", "https://en.wikipedia.org/wiki/BM25"},
		{15, "Reciprocal rank fusion combines multiple ranked lists", "https://research.google/"},
		{20, "Vector search enables semantic similarity matching", "https://vectordb.dev/"},
	}

	for _, d := range docs {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		err := engine.Ingest(ctx, index.Document{
			ID:       d.id,
			Text:     d.text,
			Vector:   vec,
			Metadata: map[string]string{"url": d.url},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	emb := embedder.NewMockEmbedder(dim)
	handler := NewHandler(&ragTestSearcher{engine: engine}, emb)

	return handler, func() { engine.Close() }
}

func TestRAGEndpoint(t *testing.T) {
	handler, cleanup := setupRAGTest(t)
	defer cleanup()

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `{"query": "graph search algorithm", "top_k": 3, "include_text": true}`
	req := httptest.NewRequest("POST", "/v1/rag", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200. Body: %s", w.Code, w.Body.String())
	}

	var resp RAGResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("status: got %q, want success", resp.Status)
	}
	if resp.Query != "graph search algorithm" {
		t.Errorf("query: got %q", resp.Query)
	}
	if len(resp.Citations) == 0 {
		t.Error("expected at least one citation")
	}
	if resp.Context == "" {
		t.Error("expected non-empty context")
	}

	// Verify citations have enriched fields
	for i, c := range resp.Citations {
		t.Logf("Citation[%d]: id=%d url=%s score=%.4f text=%q",
			i, c.DocID, c.SourceURL, c.RelevanceScore, c.ChunkText)
		if c.ChunkText == "" {
			t.Errorf("citation[%d]: expected chunk text with include_text=true", i)
		}
	}

	// Verify context has source markers
	if !strings.Contains(resp.Context, "[1]") {
		t.Error("expected [1] source marker in context")
	}
}

func TestRAGEndpointEmptyQuery(t *testing.T) {
	handler, cleanup := setupRAGTest(t)
	defer cleanup()

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `{"query": "", "top_k": 3}`
	req := httptest.NewRequest("POST", "/v1/rag", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestRAGEndpointWithoutText(t *testing.T) {
	handler, cleanup := setupRAGTest(t)
	defer cleanup()

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	body := `{"query": "test", "top_k": 2, "include_text": false}`
	req := httptest.NewRequest("POST", "/v1/rag", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp RAGResponse
	json.Unmarshal(w.Body.Bytes(), &resp)

	for _, c := range resp.Citations {
		if c.ChunkText != "" {
			t.Error("expected empty chunk text with include_text=false")
		}
	}
}
