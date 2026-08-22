package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/index"
)

// RAGRequest is the JSON body for POST /v1/rag.
type RAGRequest struct {
	Query       string            `json:"query"`
	Filters     map[string]string `json:"filters,omitempty"`
	TopK        int               `json:"top_k"`
	IncludeText bool              `json:"include_text"`
}

// Citation represents a source passage with provenance metadata.
type Citation struct {
	DocID          uint32            `json:"doc_id"`
	ChunkText      string            `json:"chunk_text,omitempty"`
	SourceURL      string            `json:"source_url,omitempty"`
	RelevanceScore float64           `json:"relevance_score"`
	BM25Score      float64           `json:"bm25_score"`
	SemanticScore  float32           `json:"semantic_score"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// RAGResponse is the JSON response for POST /v1/rag.
type RAGResponse struct {
	Status     string     `json:"status"`
	Query      string     `json:"query"`
	Citations  []Citation `json:"citations"`
	Context    string     `json:"context"`
	DurationMs int64      `json:"duration_ms"`
}

// RAGSearcher extends Searcher with document retrieval for citation enrichment.
type RAGSearcher interface {
	Searcher
	GetDocumentsByIDs(ctx context.Context, ids []uint32) (map[uint32]index.SearchHit, error)
}

func (h *Handler) handleRAG(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req RAGRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RAGResponse{Status: "error: invalid JSON"})
		return
	}
	if req.Query == "" {
		writeJSON(w, http.StatusBadRequest, RAGResponse{Status: "error: query is required"})
		return
	}
	if req.TopK <= 0 {
		req.TopK = 5
	}

	// Embed query
	queryVec, err := h.Embedder.Embed(r.Context(), req.Query)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, RAGResponse{Status: "error: embedding failed"})
		return
	}

	// Hybrid search — tenant scoping is forced here and cannot be overridden
	// by req.Filters, since RAG returns raw document text.
	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), req.Filters)
	scored, err := h.Searcher.HybridSearch(r.Context(), req.Query, queryVec, filters, req.TopK)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, RAGResponse{Status: "error: search failed"})
		return
	}

	// Enrich with citations if the searcher supports it
	citations := make([]Citation, len(scored))
	var contextParts []string

	ragSearcher, canEnrich := h.Searcher.(RAGSearcher)
	if canEnrich && len(scored) > 0 {
		ids := make([]uint32, len(scored))
		for i, s := range scored {
			ids[i] = s.ID
		}
		docs, err := ragSearcher.GetDocumentsByIDs(r.Context(), ids)
		if err != nil {
			slog.Warn("rag: enrichment error", "error", err)
			// Continue without enrichment
		}

		for i, s := range scored {
			c := Citation{
				DocID:          s.ID,
				RelevanceScore: s.FusedScore,
				BM25Score:      s.BM25Score,
				SemanticScore:  s.SemanticScore,
			}

			if hit, ok := docs[s.ID]; ok {
				if req.IncludeText {
					c.ChunkText = hit.Content
				}
				c.SourceURL = hit.Metadata["url"]
				c.Metadata = hit.Metadata
				contextParts = append(contextParts, fmt.Sprintf("[%d] %s", i+1, hit.Content))
			}
			citations[i] = c
		}
	} else {
		for i, s := range scored {
			citations[i] = Citation{
				DocID:          s.ID,
				RelevanceScore: s.FusedScore,
				BM25Score:      s.BM25Score,
				SemanticScore:  s.SemanticScore,
			}
		}
	}

	elapsed := time.Since(start)
	h.auditSearch(r, req.Query, len(scored), elapsed.Milliseconds())
	writeJSON(w, http.StatusOK, RAGResponse{
		Status:     "success",
		Query:      req.Query,
		Citations:  citations,
		Context:    strings.Join(contextParts, "\n\n"),
		DurationMs: elapsed.Milliseconds(),
	})
}
