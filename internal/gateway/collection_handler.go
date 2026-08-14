package gateway

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/efathom/yase/pkg/collection"
)

// CollectionHandler serves the /v1/collections CRUD endpoints.
type CollectionHandler struct {
	Manager *collection.Manager
}

// NewCollectionHandler creates a collection CRUD handler.
func NewCollectionHandler(mgr *collection.Manager) *CollectionHandler {
	return &CollectionHandler{Manager: mgr}
}

// RegisterRoutes mounts collection CRUD endpoints on the given mux.
func (ch *CollectionHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/collections", ch.handleCreate)
	mux.HandleFunc("GET /v1/collections", ch.handleList)
	mux.HandleFunc("GET /v1/collections/{id}", ch.handleGet)
	mux.HandleFunc("PUT /v1/collections/{id}", ch.handleUpdate)
	mux.HandleFunc("DELETE /v1/collections/{id}", ch.handleDelete)
	mux.HandleFunc("POST /v1/collections/{id}/connectors", ch.handleBindConnector)
	mux.HandleFunc("DELETE /v1/collections/{id}/connectors/{connectorId}", ch.handleUnbindConnector)
	mux.HandleFunc("GET /v1/collections/{id}/stats", ch.handleStats)
}

// --- Request/Response types ---

type createCollectionRequest struct {
	ID          string                      `json:"id"`
	Name        string                      `json:"name"`
	TenantID    string                      `json:"tenant_id,omitempty"`
	Description string                      `json:"description,omitempty"`
	Config      collection.CollectionConfig `json:"config"`
}

type updateCollectionRequest struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
}

type bindConnectorRequest struct {
	ConnectorJobID string `json:"connector_job_id"`
}

type collectionStatsResponse struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	HNSWNodes  int    `json:"hnsw_nodes"`
	Connectors int    `json:"connectors"`
}

// --- Handlers ---

// writeError logs the full error server-side and returns a generic message
// with a status code derived from the error kind.
func (ch *CollectionHandler) writeError(w http.ResponseWriter, err error) {
	slog.Error("collection request failed", "error", err)

	code := http.StatusInternalServerError
	msg := "internal error"
	switch {
	case errors.Is(err, collection.ErrAlreadyExists):
		code, msg = http.StatusConflict, "collection already exists"
	case errors.Is(err, collection.ErrNotFound):
		code, msg = http.StatusNotFound, "collection not found"
	case errors.Is(err, collection.ErrDefaultDelete):
		code, msg = http.StatusForbidden, "cannot delete the default collection"
	}
	writeJSON(w, code, map[string]string{"error": msg})
}

func (ch *CollectionHandler) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	if req.ID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}

	c, err := ch.Manager.Create(r.Context(), req.TenantID, req.ID, req.Name, req.Config)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (ch *CollectionHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := ch.Manager.Get(r.Context(), id)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (ch *CollectionHandler) handleList(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	cols, err := ch.Manager.List(r.Context(), tenantID)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cols)
}

func (ch *CollectionHandler) handleUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req updateCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	c, err := ch.Manager.Update(r.Context(), id, req.Name, req.Description)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (ch *CollectionHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := ch.Manager.Delete(r.Context(), id); err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (ch *CollectionHandler) handleBindConnector(w http.ResponseWriter, r *http.Request) {
	colID := r.PathValue("id")
	var req bindConnectorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.ConnectorJobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "connector_job_id is required"})
		return
	}

	if err := ch.Manager.BindConnector(r.Context(), colID, req.ConnectorJobID); err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "bound"})
}

func (ch *CollectionHandler) handleUnbindConnector(w http.ResponseWriter, r *http.Request) {
	colID := r.PathValue("id")
	connID := r.PathValue("connectorId")

	if err := ch.Manager.UnbindConnector(r.Context(), colID, connID); err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "unbound"})
}

func (ch *CollectionHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := ch.Manager.Get(r.Context(), id)
	if err != nil {
		ch.writeError(w, err)
		return
	}

	eng, err := ch.Manager.GetEngine(id)
	if err != nil {
		ch.writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, collectionStatsResponse{
		ID:         c.ID,
		Name:       c.Name,
		Status:     string(c.Status),
		HNSWNodes:  eng.Graph.Len(),
		Connectors: len(c.Connectors),
	})
}
