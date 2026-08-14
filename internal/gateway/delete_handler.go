package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type deleteRequest struct {
	DocIDs []uint32 `json:"doc_ids"`
}

type deleteByQueryRequest struct {
	Filters map[string]string `json:"filters"`
}

type deleteResponse struct {
	Status       string `json:"status"`
	DeletedCount int    `json:"deleted_count"`
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	ds, ok := h.Searcher.(DeleteSearcher)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "delete not supported by this search backend",
		})
		return
	}

	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if len(req.DocIDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "doc_ids is required"})
		return
	}

	count, err := ds.Delete(r.Context(), req.DocIDs)
	if err != nil {
		slog.Error("delete error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error during delete"})
		return
	}

	if h.Cache != nil {
		h.Cache.Invalidate()
	}
	h.auditDelete(r, count)
	writeJSON(w, http.StatusOK, deleteResponse{Status: "success", DeletedCount: count})
}

func (h *Handler) handleDeleteByQuery(w http.ResponseWriter, r *http.Request) {
	ds, ok := h.Searcher.(DeleteSearcher)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "delete not supported by this search backend",
		})
		return
	}

	var req deleteByQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if len(req.Filters) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "filters is required"})
		return
	}

	count, err := ds.DeleteByFilter(r.Context(), req.Filters)
	if err != nil {
		slog.Error("delete-by-query error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error during delete"})
		return
	}

	if h.Cache != nil {
		h.Cache.Invalidate()
	}
	h.auditDelete(r, count)
	writeJSON(w, http.StatusOK, deleteResponse{Status: "success", DeletedCount: count})
}
