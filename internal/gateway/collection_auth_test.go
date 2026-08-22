package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collectionTestConfig() *config.Config {
	return &config.Config{
		Embedder: config.EmbedderConfig{Provider: "mock", Dimension: 32},
		Index:    config.IndexConfig{ArenaSize: 16 * 1024 * 1024, CentroidRate: 5},
	}
}

// collectionMux builds a handler over a manager holding one collection per
// tenant, with scope enforcement enabled.
func collectionMux(t *testing.T) *http.ServeMux {
	t.Helper()
	dir := t.TempDir()
	store, err := collection.NewFileStore(dir)
	require.NoError(t, err)
	mgr := collection.NewManager(store, dir, collectionTestConfig(), slog.Default())

	ctx := context.Background()
	_, err = mgr.Create(ctx, "tenant-a", "col-a", "A's collection", collection.CollectionConfig{})
	require.NoError(t, err)
	_, err = mgr.Create(ctx, "tenant-b", "col-b", "B's collection", collection.CollectionConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { mgr.Close() })

	mux := http.NewServeMux()
	NewCollectionHandler(mgr).SetAuthEnabled(true).RegisterRoutes(mux)
	return mux
}

// scopedRequest builds a request carrying a tenant and an explicit scope set.
func scopedRequest(method, target, tenant string, scopes []string, body []byte) *http.Request {
	var req *http.Request
	if body == nil {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, bytes.NewReader(body))
	}
	ac := &auth.AuthContext{TenantID: tenant, UserID: "u1", Scopes: scopes}
	return req.WithContext(auth.WithContext(req.Context(), ac))
}

// C-01: a reader-level principal must not be able to delete a collection.
func TestDeleteCollectionRequiresAdminScope(t *testing.T) {
	mux := collectionMux(t)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, scopedRequest("DELETE", "/v1/collections/col-a", "tenant-a", []string{"search"}, nil))

	assert.Equal(t, http.StatusForbidden, w.Code,
		"the search scope must not authorize destroying a collection")
}

func TestCreateCollectionRequiresAdminScope(t *testing.T) {
	mux := collectionMux(t)

	body, err := json.Marshal(createCollectionRequest{ID: "new-col", Name: "New"})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, scopedRequest("POST", "/v1/collections", "tenant-a", []string{"search"}, body))

	assert.Equal(t, http.StatusForbidden, w.Code)
}

// C-01: even with admin scope, a tenant must not reach another tenant's data.
func TestDeleteCollectionRejectsOtherTenant(t *testing.T) {
	mux := collectionMux(t)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, scopedRequest("DELETE", "/v1/collections/col-b", "tenant-a", []string{"admin"}, nil))

	assert.Equal(t, http.StatusNotFound, w.Code,
		"tenant-a must not delete tenant-b's collection")

	// And it must still be there for its owner.
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, scopedRequest("GET", "/v1/collections/col-b", "tenant-b", []string{"search"}, nil))
	assert.Equal(t, http.StatusOK, w2.Code)
}

func TestGetCollectionRejectsOtherTenant(t *testing.T) {
	mux := collectionMux(t)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, scopedRequest("GET", "/v1/collections/col-b", "tenant-a", []string{"search"}, nil))

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// C-02: the listing must come from the authenticated tenant, not the query
// string — otherwise ?tenant_id=victim enumerates another tenant.
func TestListIgnoresTenantQueryParameter(t *testing.T) {
	mux := collectionMux(t)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, scopedRequest("GET", "/v1/collections?tenant_id=tenant-b", "tenant-a", []string{"search"}, nil))

	require.Equal(t, http.StatusOK, w.Code)

	var got []*collection.Collection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	for _, c := range got {
		assert.Equal(t, "tenant-a", c.TenantID,
			"a tenant_id query parameter must not widen the listing")
	}
}

// C-02: creating a collection must bind it to the caller's tenant, not to one
// named in the request body.
func TestCreateIgnoresTenantInRequestBody(t *testing.T) {
	mux := collectionMux(t)

	body, err := json.Marshal(createCollectionRequest{
		ID: "planted", Name: "Planted", TenantID: "tenant-b",
	})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, scopedRequest("POST", "/v1/collections", "tenant-a", []string{"admin"}, body))
	require.Equal(t, http.StatusCreated, w.Code)

	var created collection.Collection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	assert.Equal(t, "tenant-a", created.TenantID,
		"the collection must belong to the authenticated tenant")
}
