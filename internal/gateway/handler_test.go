package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
)

func makeTestEngine(t *testing.T, dim int) *index.HybridEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "gateway-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	engine, err := index.NewHybridEngine(dir, 64*1024*1024, dim, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func seedEngine(t *testing.T, engine *index.HybridEngine, emb embedder.Embedder, n int) {
	t.Helper()
	rng := rand.New(rand.NewSource(42))
	dim := emb.Dimension()

	texts := []string{
		"Machine learning transforms data into predictions using statistical models.",
		"Neural networks are composed of interconnected layers of artificial neurons.",
		"Deep learning uses multiple layers to progressively extract higher-level features.",
		"Natural language processing enables computers to understand human text.",
		"Computer vision algorithms analyze and interpret images and video frames.",
		"Reinforcement learning trains agents through rewards and penalties.",
		"Transfer learning applies knowledge from one domain to another related task.",
		"Generative adversarial networks create synthetic data by competing models.",
		"Convolutional neural networks excel at spatial pattern recognition.",
		"Recurrent neural networks handle sequential data like time series.",
		"Gradient descent optimizes parameters by following the steepest slope.",
		"Batch normalization accelerates training by normalizing layer inputs.",
		"Attention mechanisms allow models to focus on relevant parts of the input.",
		"Transformers revolutionized NLP with self-attention instead of recurrence.",
		"BERT pre-trains deep bidirectional representations from unlabeled text.",
		"GPT generates coherent text by predicting the next token in a sequence.",
		"Word embeddings map words to dense vectors that capture semantic meaning.",
		"Sentence embeddings represent entire sentences as fixed-length vectors.",
		"Cosine similarity measures the angle between two vectors in embedding space.",
		"Vector databases enable efficient nearest neighbor search at scale.",
		"Kubernetes orchestrates containerized applications across clusters.",
		"Docker containers package applications with their dependencies.",
		"Microservices architecture decomposes systems into small independent services.",
		"API gateways route requests and enforce authentication and rate limiting.",
		"Load balancers distribute traffic across multiple server instances.",
		"The solar system contains eight planets orbiting our star.",
		"Photosynthesis converts sunlight into chemical energy in plant cells.",
		"DNA carries the genetic instructions for all living organisms.",
		"Quantum mechanics describes the behavior of matter at atomic scales.",
		"Relativity theory connects space, time, and gravity in a unified framework.",
		"The water cycle involves evaporation, condensation, and precipitation.",
		"Plate tectonics explains the movement of Earth's lithospheric plates.",
		"Evolution by natural selection drives the diversity of life on Earth.",
		"Antibiotics fight bacterial infections but do not work against viruses.",
		"Vaccines train the immune system to recognize and fight pathogens.",
		"The stock market reflects aggregate investor sentiment about companies.",
		"Compound interest grows investments exponentially over long time periods.",
		"Inflation erodes the purchasing power of money over time.",
		"Supply and demand determine the equilibrium price in free markets.",
		"Cryptocurrency uses blockchain technology for decentralized transactions.",
		"Gödel's incompleteness theorems limit what formal systems can prove.",
		"The halting problem shows that some questions about programs are undecidable.",
		"P versus NP asks whether every problem verifiable quickly is also solvable quickly.",
		"Big O notation classifies algorithm efficiency by growth rate.",
		"Hash tables provide average O(1) lookup time using hash functions.",
		"Binary search trees maintain sorted data for efficient range queries.",
		"Graph algorithms solve problems about networks and connections.",
		"Dynamic programming solves complex problems by breaking them into subproblems.",
		"Greedy algorithms make locally optimal choices at each step.",
		"Divide and conquer splits problems in half recursively to solve them.",
	}

	for i := 0; i < n && i < len(texts); i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		meta := map[string]string{"source": "test", "topic": "ml"}
		if i >= 25 {
			meta["topic"] = "science"
		}
		if i >= 35 {
			meta["topic"] = "finance"
		}
		if i >= 40 {
			meta["topic"] = "cs"
		}

		err := engine.Ingest(context.Background(), index.Document{
			ID:       uint32(i),
			Text:     texts[i],
			Vector:   vec,
			Metadata: meta,
		})
		if err != nil {
			t.Fatalf("seed doc %d: %v", i, err)
		}
	}
}

func TestSearchEndToEnd(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	seedEngine(t, engine, emb, 50)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(SearchRequest{
		Query: "neural networks deep learning",
		TopK:  5,
	})
	req := httptest.NewRequest("POST", "/search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp SearchResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Errorf("expected success, got %q", resp.Status)
	}
	if resp.Count == 0 {
		t.Error("expected at least one result")
	}
	if resp.DurationMs < 0 {
		t.Error("expected non-negative duration")
	}
	for _, r := range resp.Results {
		if r.FusedScore == 0 {
			t.Error("expected non-zero fused score")
		}
	}
}

func TestSearchWithFilters(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	seedEngine(t, engine, emb, 50)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(SearchRequest{
		Query:   "algorithms",
		Filters: map[string]string{"topic": "cs"},
		TopK:    10,
	})
	req := httptest.NewRequest("POST", "/search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp SearchResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "success" {
		t.Errorf("expected success, got %q", resp.Status)
	}
}

func TestSearchEmptyIndex(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(SearchRequest{
		Query: "anything",
		TopK:  5,
	})
	req := httptest.NewRequest("POST", "/search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp SearchResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "success" {
		t.Errorf("expected success, got %q", resp.Status)
	}
	if resp.Count != 0 {
		t.Errorf("expected 0 results, got %d", resp.Count)
	}
}

func TestSearchBadJSON(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/search", bytes.NewReader([]byte("not json")))
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestSearchEmptyQuery(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(SearchRequest{Query: "", TopK: 5})
	req := httptest.NewRequest("POST", "/search", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestHealthEndpoint(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	json.NewDecoder(rec.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("expected ok, got %q", body["status"])
	}
}

func TestReadyEndpointEmptyIndexReady(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/ready", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	// An empty index is still Ready — readiness reflects "can serve requests"
	// (embedder reachable), not "index is non-empty".
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestReadyEndpointReady(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	seedEngine(t, engine, emb, 10)

	h := NewHandler(&LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/ready", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}
