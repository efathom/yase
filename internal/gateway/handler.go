package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/audit"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/cache"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/metrics"
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
	Searcher       Searcher
	Embedder       embedder.Embedder
	Collections    CollectionSearchBackend // nil if collections not initialized
	trustedProxies []netip.Prefix
	Audit          *audit.Logger      // nil = audit disabled
	Cache          *cache.SearchCache // nil = cache disabled
	authEnabled    bool               // enables per-route scope enforcement
}

// maxTopK caps the number of results a client may request, preventing
// resource exhaustion and int32 overflow into the gRPC layer.
const maxTopK = 1000

// NewHandler creates a gateway handler with a searcher backend and embedder.
func NewHandler(searcher Searcher, emb embedder.Embedder) *Handler {
	return &Handler{Searcher: searcher, Embedder: emb}
}

// WithCollectionSearcher sets an in-process collection searcher.
func (h *Handler) WithCollectionSearcher(cs *collection.Searcher) *Handler {
	h.Collections = &LocalCollectionSearch{Searcher: cs}
	return h
}

// WithCollectionBackend sets the collection search backend (local or remote).
func (h *Handler) WithCollectionBackend(b CollectionSearchBackend) *Handler {
	h.Collections = b
	return h
}

// SetTrustedProxies lists the proxy networks whose X-Forwarded-For headers
// are believed when determining the client IP for audit logs.
func (h *Handler) SetTrustedProxies(cidrs []string) error {
	prefixes, err := parseTrustedProxies(cidrs)
	if err != nil {
		return err
	}
	h.trustedProxies = prefixes
	return nil
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
	h.Audit.LogSearch(tenantID, userID, query, count, durationMs, clientIP(r, h.trustedProxies))
}

func (h *Handler) auditDelete(r *http.Request, count int) {
	if h.Audit == nil {
		return
	}
	tenantID, userID := "", ""
	if ac := auth.FromContext(r.Context()); ac != nil {
		tenantID, userID = ac.TenantID, ac.UserID
	}
	h.Audit.LogDelete(tenantID, userID, count, clientIP(r, h.trustedProxies))
}

// parseTrustedProxies parses CIDRs or bare IPs.
func parseTrustedProxies(cidrs []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		if p, err := netip.ParsePrefix(c); err == nil {
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(c)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy %q", c)
		}
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

func trusted(addr string, proxies []netip.Prefix) bool {
	a, err := netip.ParseAddr(strings.TrimSpace(addr))
	if err != nil {
		return false
	}
	a = a.Unmap()
	for _, p := range proxies {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// clientIP returns the client address. X-Forwarded-For is honored only when
// the direct peer is a trusted proxy; the client is then the right-most
// address that is not itself a trusted proxy. Without this, any client could
// forge its address in audit logs by sending the header.
func clientIP(r *http.Request, proxies []netip.Prefix) string {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	if len(proxies) == 0 || !trusted(peer, proxies) {
		return peer
	}
	hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		h := strings.TrimSpace(hops[i])
		if h == "" {
			continue
		}
		if !trusted(h, proxies) {
			return h
		}
	}
	return peer
}

// DeleteSearcher extends Searcher with document deletion capability.
// Both methods take metadata filters so tenant scoping is part of the
// signature rather than something a handler can forget to apply.
type DeleteSearcher interface {
	Searcher
	Delete(ctx context.Context, docIDs []uint32, filters map[string]string) (int, error)
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

	if msg := validateQuery(req.Query); msg != "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: " + msg})
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
	if h.Collections == nil {
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
	if msg := validateQuery(req.Query); msg != "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: " + msg})
		return
	}
	req.TopK = clampTopK(req.TopK)
	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), req.Filters)

	start := time.Now()
	results, err := h.Collections.SearchCollections(r.Context(), callerTenant(r), []string{colID}, false, req.Query, filters, req.TopK)
	if err != nil {
		h.writeSearchError(w, err)
		return
	}

	h.auditSearch(r, req.Query, len(results), time.Since(start).Milliseconds())
	h.writeCollectionSearchResponse(w, results, req, start)
}

// handleCrossCollectionSearch handles POST /v1/collections/search — multi-collection.
func (h *Handler) handleCrossCollectionSearch(w http.ResponseWriter, r *http.Request) {
	if h.Collections == nil {
		writeJSON(w, http.StatusServiceUnavailable, SearchResponse{Status: "error: collections not initialized"})
		return
	}

	var req CrossCollectionSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: invalid JSON"})
		return
	}
	if msg := validateQuery(req.Query); msg != "" {
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: " + msg})
		return
	}
	req.TopK = clampTopK(req.TopK)
	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), req.Filters)

	start := time.Now()
	results, err := h.Collections.SearchCollections(r.Context(), callerTenant(r), req.Collections, true, req.Query, filters, req.TopK)
	if err != nil {
		h.writeSearchError(w, err)
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

// writeSearchError maps backend errors to HTTP statuses without leaking detail.
func (h *Handler) writeSearchError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, collection.ErrNotFound):
		writeJSON(w, http.StatusNotFound, SearchResponse{Status: "error: collection not found"})
	case errors.Is(err, index.ErrDimensionMismatch), errors.Is(err, collection.ErrInvalidConfig):
		writeJSON(w, http.StatusBadRequest, SearchResponse{Status: "error: invalid search request"})
	default:
		slog.Error("search failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, SearchResponse{Status: "error: search failed"})
	}
}

// maxQueryBytes bounds query text so a request cannot force huge BM25
// expansions or embedding calls.
const maxQueryBytes = 4096

func validateQuery(q string) string {
	switch {
	case strings.TrimSpace(q) == "":
		return "query is required"
	case len(q) > maxQueryBytes:
		return "query too long"
	}
	return ""
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

func (s *LocalSearcher) Delete(ctx context.Context, docIDs []uint32, filters map[string]string) (int, error) {
	return s.Engine.Delete(ctx, docIDs, filters)
}

func (s *LocalSearcher) DeleteByFilter(ctx context.Context, filters map[string]string) (int, error) {
	return s.Engine.DeleteByFilter(ctx, filters)
}
