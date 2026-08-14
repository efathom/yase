package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/efathom/yase/pkg/audit"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/cache"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/metrics"
	ingestionv1 "github.com/efathom/yase/proto/v1"
)

// SearchRequest is the JSON body for POST /search.
type SearchRequest struct {
	Query    string            `json:"query"`
	Filters  map[string]string `json:"filters,omitempty"`
	TopK     int               `json:"top_k"`
	PageSize int               `json:"page_size,omitempty"` // results per page (default = top_k)
	Offset   int               `json:"offset,omitempty"`    // pagination offset
}

// SearchResult is a single item in the response.
type SearchResult struct {
	ID            uint32            `json:"id"`
	Text          string            `json:"text,omitempty"`
	CollectionID  string            `json:"collection_id,omitempty"`
	FusedScore    float64           `json:"fused_score"`
	BM25Score     float64           `json:"bm25_score"`
	SemanticScore float32           `json:"semantic_score"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// CrossCollectionSearchRequest is the JSON body for POST /v1/collections/search.
type CrossCollectionSearchRequest struct {
	Query       string            `json:"query"`
	Collections []string          `json:"collections,omitempty"` // empty = all collections
	Filters     map[string]string `json:"filters,omitempty"`
	TopK        int               `json:"top_k"`
	PageSize    int               `json:"page_size,omitempty"`
	Offset      int               `json:"offset,omitempty"`
}

// SearchResponse is the JSON response for POST /search.
type SearchResponse struct {
	Status     string         `json:"status"`
	Count      int            `json:"count"`
	Total      int            `json:"total"`  // total matching results (before pagination)
	Offset     int            `json:"offset"` // current offset
	DurationMs int64          `json:"duration_ms"`
	Results    []SearchResult `json:"results"`
}

// Searcher abstracts the search backend so the handler works with both
// a local HybridEngine and a remote gRPC IndexService client.
type Searcher interface {
	HybridSearch(ctx context.Context, textQuery string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error)
	NodeCount() int
}

// Handler serves the search gateway HTTP endpoints.
type Handler struct {
	Searcher           Searcher
	Embedder           embedder.Embedder
	CollectionSearcher *collection.Searcher // nil if collections not initialized
	Audit              *audit.Logger        // nil = audit disabled
	Cache              *cache.SearchCache   // nil = cache disabled
	authEnabled        bool                 // enables per-route scope enforcement
}

// maxTopK caps the number of results a client may request, preventing
// resource exhaustion and int32 overflow into the gRPC layer.
const maxTopK = 1000

// NewHandler creates a gateway handler with a searcher backend and embedder.
func NewHandler(searcher Searcher, emb embedder.Embedder) *Handler {
	return &Handler{Searcher: searcher, Embedder: emb}
}

// WithCollectionSearcher sets the collection-aware searcher on the handler.
func (h *Handler) WithCollectionSearcher(cs *collection.Searcher) *Handler {
	h.CollectionSearcher = cs
	return h
}

// SetAuthEnabled enables per-route scope enforcement (delete/admin). When
// false, routes are served without scope checks (auth fully disabled).
func (h *Handler) SetAuthEnabled(enabled bool) *Handler {
	h.authEnabled = enabled
	return h
}

// SetAudit attaches an audit logger for search/delete events.
func (h *Handler) SetAudit(a *audit.Logger) *Handler {
	h.Audit = a
	return h
}

// SetCache attaches a search result cache for the legacy /search path.
func (h *Handler) SetCache(c *cache.SearchCache) *Handler {
	h.Cache = c
	return h
}

func (h *Handler) auditSearch(r *http.Request, query string, count int, durationMs int64) {
	if h.Audit == nil {
		return
	}
	tenantID, userID := "", ""
	if ac := auth.FromContext(r.Context()); ac != nil {
		tenantID, userID = ac.TenantID, ac.UserID
	}
	h.Audit.LogSearch(tenantID, userID, query, count, durationMs, clientIP(r))
}

func (h *Handler) auditDelete(r *http.Request, count int) {
	if h.Audit == nil {
		return
	}
	tenantID, userID := "", ""
	if ac := auth.FromContext(r.Context()); ac != nil {
		tenantID, userID = ac.TenantID, ac.UserID
	}
	h.Audit.LogDelete(tenantID, userID, count, clientIP(r))
}

// clientIP extracts the client IP, honoring X-Forwarded-For for proxies.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// DeleteSearcher extends Searcher with document deletion capability.
type DeleteSearcher interface {
	Searcher
	Delete(ctx context.Context, docIDs []uint32) (int, error)
	DeleteByFilter(ctx context.Context, filters map[string]string) (int, error)
}

// RegisterRoutes mounts all gateway endpoints on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Legacy search → _default collection (backward compat)
	mux.HandleFunc("POST /search", h.handleSearch)
	mux.HandleFunc("POST /v1/rag", h.handleRAG)
	mux.Handle("DELETE /v1/documents", h.requireScope("delete", http.HandlerFunc(h.handleDelete)))
	mux.Handle("DELETE /v1/documents/query", h.requireScope("delete", http.HandlerFunc(h.handleDeleteByQuery)))
	mux.HandleFunc("GET /v1/suggest", h.handleSuggest)
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /ready", h.handleReady)

	// Collection search endpoints
	mux.HandleFunc("POST /v1/collections/search", h.handleCrossCollectionSearch)
	mux.HandleFunc("POST /v1/collections/{id}/search", h.handleCollectionSearch)
}

func clampTopK(topK int) int {
	if topK <= 0 {
		return 10
	}
	if topK > maxTopK {
		return maxTopK
	}
	return topK
}

// applyPagination safely slices results for offset/pageSize with overflow guards.
func applyPagination(results []SearchResult, offset, pageSize int) []SearchResult {
	total := len(results)
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return nil
	}
	if pageSize <= 0 || pageSize > total-offset {
		return results[offset:]
	}
	return results[offset : offset+pageSize]
}

func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, SearchResponse{
			Status: "error: invalid JSON",
		})
		return
	}

	if req.Query == "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{
			Status: "error: query is required",
		})
		return
	}

	req.TopK = clampTopK(req.TopK)
	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), req.Filters)

	// Serve from the search cache when available (skips embed + search).
	var scored []index.ScoredResult
	if h.Cache != nil {
		scored = h.Cache.Get(req.Query, filters, req.TopK)
	}

	if scored == nil {
		// Vectorize query
		queryVec, err := h.Embedder.Embed(r.Context(), req.Query)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, SearchResponse{
				Status: "error: embedding failed",
			})
			return
		}

		// Hybrid search via backend (local or remote)
		scored, err = h.Searcher.HybridSearch(r.Context(), req.Query, queryVec, filters, req.TopK)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, SearchResponse{
				Status: "error: search failed",
			})
			return
		}
		if h.Cache != nil && scored != nil {
			h.Cache.Put(req.Query, filters, req.TopK, scored)
		}
	}

	elapsed := time.Since(start)
	metrics.SearchLatency.WithLabelValues("hybrid").Observe(elapsed.Seconds())

	allResults := make([]SearchResult, len(scored))
	for i, s := range scored {
		allResults[i] = SearchResult{
			ID:            s.ID,
			FusedScore:    s.FusedScore,
			BM25Score:     s.BM25Score,
			SemanticScore: s.SemanticScore,
		}
	}

	// Apply pagination
	total := len(allResults)
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	paged := applyPagination(allResults, req.Offset, req.PageSize)

	metrics.SearchResultsCount.WithLabelValues("hybrid").Observe(float64(len(paged)))

	h.auditSearch(r, req.Query, len(paged), elapsed.Milliseconds())

	writeJSON(w, http.StatusOK, SearchResponse{
		Status:     "success",
		Count:      len(paged),
		Total:      total,
		Offset:     offset,
		DurationMs: elapsed.Milliseconds(),
		Results:    paged,
	})
}

// handleCollectionSearch handles POST /v1/collections/{id}/search — single collection.
func (h *Handler) handleCollectionSearch(w http.ResponseWriter, r *http.Request) {
	if h.CollectionSearcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, SearchResponse{Status: "error: collections not initialized"})
		return
	}

	colID := r.PathValue("id")
	if colID == "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: collection id is required"})
		return
	}

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: invalid JSON"})
		return
	}
	if req.Query == "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: query is required"})
		return
	}
	req.TopK = clampTopK(req.TopK)
	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), req.Filters)

	emb, err := h.CollectionSearcher.GetEmbedder(colID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, SearchResponse{Status: "error: collection not found"})
		return
	}

	start := time.Now()
	queryVec, err := emb.Embed(r.Context(), req.Query)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, SearchResponse{Status: "error: embedding failed"})
		return
	}

	results, err := h.CollectionSearcher.SearchSingle(r.Context(), colID, req.Query, queryVec, filters, req.TopK)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, SearchResponse{Status: "error: search failed"})
		return
	}

	h.auditSearch(r, req.Query, len(results), time.Since(start).Milliseconds())
	h.writeCollectionSearchResponse(w, results, req, start)
}

// handleCrossCollectionSearch handles POST /v1/collections/search — multi-collection.
func (h *Handler) handleCrossCollectionSearch(w http.ResponseWriter, r *http.Request) {
	if h.CollectionSearcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, SearchResponse{Status: "error: collections not initialized"})
		return
	}

	var req CrossCollectionSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: invalid JSON"})
		return
	}
	if req.Query == "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: query is required"})
		return
	}
	req.TopK = clampTopK(req.TopK)
	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), req.Filters)

	// Use _default embedder for cross-collection queries
	emb, err := h.CollectionSearcher.GetEmbedder("")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, SearchResponse{Status: "error: embedder unavailable"})
		return
	}

	start := time.Now()
	queryVec, err := emb.Embed(r.Context(), req.Query)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, SearchResponse{Status: "error: embedding failed"})
		return
	}

	results, err := h.CollectionSearcher.Search(r.Context(), req.Collections, req.Query, queryVec, filters, req.TopK)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, SearchResponse{Status: "error: search failed"})
		return
	}

	h.auditSearch(r, req.Query, len(results), time.Since(start).Milliseconds())

	searchReq := SearchRequest{
		Query:    req.Query,
		Filters:  req.Filters,
		TopK:     req.TopK,
		PageSize: req.PageSize,
		Offset:   req.Offset,
	}
	h.writeCollectionSearchResponse(w, results, searchReq, start)
}

func (h *Handler) writeCollectionSearchResponse(w http.ResponseWriter, results []collection.SearchResult, req SearchRequest, start time.Time) {
	elapsed := time.Since(start)
	metrics.SearchLatency.WithLabelValues("hybrid").Observe(elapsed.Seconds())

	allResults := make([]SearchResult, len(results))
	for i, s := range results {
		allResults[i] = SearchResult{
			ID:            s.ID,
			CollectionID:  s.CollectionID,
			FusedScore:    s.FusedScore,
			BM25Score:     s.BM25Score,
			SemanticScore: s.SemanticScore,
		}
	}

	total := len(allResults)
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	paged := applyPagination(allResults, req.Offset, req.PageSize)

	metrics.SearchResultsCount.WithLabelValues("hybrid").Observe(float64(len(paged)))

	writeJSON(w, http.StatusOK, SearchResponse{
		Status:     "success",
		Count:      len(paged),
		Total:      total,
		Offset:     offset,
		DurationMs: elapsed.Milliseconds(),
		Results:    paged,
	})
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	// Readiness reflects "can serve requests": the embedder must be reachable.
	// It intentionally does NOT require a non-empty index, so a freshly
	// deployed cluster becomes Ready as soon as it can serve queries.
	readyCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	_, err := h.Embedder.Embed(readyCtx, "ping")
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "embedder unreachable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")

	// Encode to a buffer first so a marshal error is caught before the status
	// code is committed to the wire (avoids a silently truncated body).
	data, err := json.Marshal(v)
	if err != nil {
		slog.Error("gateway: failed to encode JSON response", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(code)
	if _, err := w.Write(data); err != nil {
		slog.Error("gateway: failed to write JSON response", "error", err)
	}
}

// LocalSearcher wraps a HybridEngine for in-process use (cmd/local).
type LocalSearcher struct {
	Engine *index.HybridEngine
}

func (s *LocalSearcher) HybridSearch(ctx context.Context, textQuery string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	return s.Engine.HybridSearch(ctx, textQuery, queryVec, filters, topK)
}

func (s *LocalSearcher) NodeCount() int {
	return s.Engine.Graph.Len()
}

func (s *LocalSearcher) GetDocumentsByIDs(ctx context.Context, ids []uint32) (map[uint32]index.SearchHit, error) {
	return s.Engine.BlugeStore.GetDocumentsByIDs(ctx, ids)
}

func (s *LocalSearcher) Delete(ctx context.Context, docIDs []uint32) (int, error) {
	return s.Engine.Delete(ctx, docIDs)
}

func (s *LocalSearcher) DeleteByFilter(ctx context.Context, filters map[string]string) (int, error) {
	return s.Engine.DeleteByFilter(ctx, filters)
}

// RemoteSearcher calls the IndexService gRPC server for search.
type RemoteSearcher struct {
	Client ingestionv1.IndexServiceClient
}

func (s *RemoteSearcher) HybridSearch(ctx context.Context, textQuery string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	resp, err := s.Client.Search(ctx, &ingestionv1.SearchRequest{
		Query:       textQuery,
		QueryVector: queryVec,
		Filters:     filters,
		TopK:        int32(topK),
	})
	if err != nil {
		return nil, err
	}

	results := make([]index.ScoredResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = index.ScoredResult{
			ID:            r.Id,
			FusedScore:    r.FusedScore,
			BM25Score:     r.Bm25Score,
			SemanticScore: r.SemanticScore,
		}
	}
	return results, nil
}

func (s *RemoteSearcher) NodeCount() int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := s.Client.Stats(ctx, &ingestionv1.StatsRequest{})
	if err != nil {
		return 0
	}
	return int(resp.HnswNodes)
}
