package gateway

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
)

// CollectionHandler serves the /v1/collections CRUD endpoints.
type CollectionHandler struct {
	Manager     *collection.Manager
	authEnabled bool
}

// NewCollectionHandler creates a collection CRUD handler.
func NewCollectionHandler(mgr *collection.Manager) *CollectionHandler {
	return &CollectionHandler{Manager: mgr}
}

// SetAuthEnabled turns on per-route scope enforcement. When false the routes
// are served without scope checks, matching a deployment with auth disabled.
func (ch *CollectionHandler) SetAuthEnabled(enabled bool) *CollectionHandler {
	ch.authEnabled = enabled
	return ch
}

// requireScope wraps a handler in a scope check when auth is enabled.
func (ch *CollectionHandler) requireScope(scope string, next http.HandlerFunc) http.Handler {
	if !ch.authEnabled {
		return next
	}
	return auth.RequireScope(scope)(next)
}

// callerTenant returns the authenticated tenant for this request. It is the
// only source of tenant identity — request bodies and query parameters are
// never trusted for it.
func callerTenant(r *http.Request) string {
	if ac := auth.FromContext(r.Context()); ac != nil {
		return ac.TenantID
	}
	return ""
}

// RegisterRoutes mounts collection CRUD endpoints on the given mux.
//
// Reads require the "search" scope; anything that creates, mutates, or
// destroys a collection requires "admin".
func (ch *CollectionHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("POST /v1/collections", ch.requireScope("admin", ch.handleCreate))
	mux.Handle("GET /v1/collections", ch.requireScope("search", ch.handleList))
	mux.Handle("GET /v1/collections/{id}", ch.requireScope("search", ch.handleGet))
	mux.Handle("PUT /v1/collections/{id}", ch.requireScope("admin", ch.handleUpdate))
	mux.Handle("DELETE /v1/collections/{id}", ch.requireScope("admin", ch.handleDelete))
	mux.Handle("POST /v1/collections/{id}/connectors", ch.requireScope("admin", ch.handleBindConnector))
	mux.Handle("DELETE /v1/collections/{id}/connectors/{connectorId}", ch.requireScope("admin", ch.handleUnbindConnector))
	mux.Handle("GET /v1/collections/{id}/stats", ch.requireScope("search", ch.handleStats))
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

	c, err := ch.Manager.Create(r.Context(), callerTenant(r), req.ID, req.Name, req.Config)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (ch *CollectionHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := ch.Manager.Get(r.Context(), callerTenant(r), id)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (ch *CollectionHandler) handleList(w http.ResponseWriter, r *http.Request) {
	// Scoped to the authenticated tenant — a tenant_id query parameter must
	// not be able to widen or redirect the listing.
	cols, err := ch.Manager.List(r.Context(), callerTenant(r))
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

	c, err := ch.Manager.Update(r.Context(), callerTenant(r), id, req.Name, req.Description)
	if err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (ch *CollectionHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := ch.Manager.Delete(r.Context(), callerTenant(r), id); err != nil {
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

	if err := ch.Manager.BindConnector(r.Context(), callerTenant(r), colID, req.ConnectorJobID); err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "bound"})
}

func (ch *CollectionHandler) handleUnbindConnector(w http.ResponseWriter, r *http.Request) {
	colID := r.PathValue("id")
	connID := r.PathValue("connectorId")

	if err := ch.Manager.UnbindConnector(r.Context(), callerTenant(r), colID, connID); err != nil {
		ch.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "unbound"})
}

func (ch *CollectionHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := ch.Manager.Get(r.Context(), callerTenant(r), id)
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
