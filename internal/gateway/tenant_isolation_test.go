package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingSearcher captures the filters each handler passes down so tests can
// assert that tenant scoping was applied before the search backend was reached.
type recordingSearcher struct {
	gotSearchFilters map[string]string
	gotDeleteFilters map[string]string
	gotSuggestFilter map[string]string
	deletedIDs       []uint32
}

func (r *recordingSearcher) HybridSearch(_ context.Context, _ string, _ []float32, filters map[string]string, _ int) ([]index.ScoredResult, error) {
	r.gotSearchFilters = filters
	return nil, nil
}

func (r *recordingSearcher) NodeCount() int { return 1 }

func (r *recordingSearcher) Delete(_ context.Context, ids []uint32, filters map[string]string) (int, error) {
	r.deletedIDs = ids
	r.gotDeleteFilters = filters
	return len(ids), nil
}

func (r *recordingSearcher) DeleteByFilter(_ context.Context, filters map[string]string) (int, error) {
	r.gotDeleteFilters = filters
	return 0, nil
}

func (r *recordingSearcher) GetDocumentsByIDs(_ context.Context, _ []uint32) (map[uint32]index.SearchHit, error) {
	return map[uint32]index.SearchHit{}, nil
}

func (r *recordingSearcher) Suggest(_ string, _ int, filters map[string]string) []index.Suggestion {
	r.gotSuggestFilter = filters
	return nil
}

// tenantRequest builds a request already carrying an authenticated tenant, as
// the auth middleware would have left it.
func tenantRequest(method, target, tenant string, body []byte) *http.Request {
	var req *http.Request
	if body == nil {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, bytes.NewReader(body))
	}
	ac := &auth.AuthContext{
		TenantID: tenant,
		UserID:   "u1",
		Roles:    []string{"admin"},
		Scopes:   []string{"search", "ingest", "delete", "admin"},
	}
	return req.WithContext(auth.WithContext(req.Context(), ac))
}

func newRecordingHandler(t *testing.T) (*Handler, *recordingSearcher, *http.ServeMux) {
	t.Helper()
	rec := &recordingSearcher{}
	emb := embedder.NewMockEmbedder(8)
	h := NewHandler(rec, emb).SetAuthEnabled(true)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return h, rec, mux
}

// C-03: the RAG endpoint returns raw document text, so losing the tenant filter
// there discloses another tenant's content.
func TestRAGInjectsTenantFilter(t *testing.T) {
	_, rec, mux := newRecordingHandler(t)

	body, err := json.Marshal(RAGRequest{Query: "anything", TopK: 3})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, tenantRequest("POST", "/v1/rag", "tenant-a", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant-a", rec.gotSearchFilters["_tenant"],
		"RAG must scope the search to the caller's tenant")
}

// C-03: a caller must not be able to target another tenant by supplying the
// filter themselves — the injected value has to win.
func TestRAGTenantFilterOverridesCallerSuppliedValue(t *testing.T) {
	_, rec, mux := newRecordingHandler(t)

	body, err := json.Marshal(RAGRequest{
		Query:   "anything",
		TopK:    3,
		Filters: map[string]string{"_tenant": "victim"},
	})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, tenantRequest("POST", "/v1/rag", "tenant-a", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant-a", rec.gotSearchFilters["_tenant"],
		"a caller-supplied _tenant filter must be overwritten, not honored")
}

// H-05: delete-by-query forwards the caller's filters straight to the backend.
func TestDeleteByQueryInjectsTenantFilter(t *testing.T) {
	_, rec, mux := newRecordingHandler(t)

	body, err := json.Marshal(deleteByQueryRequest{Filters: map[string]string{"source": "wiki"}})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, tenantRequest("DELETE", "/v1/documents/query", "tenant-a", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant-a", rec.gotDeleteFilters["_tenant"],
		"delete-by-query must be scoped to the caller's tenant")
	assert.Equal(t, "wiki", rec.gotDeleteFilters["source"],
		"the caller's own filters must be preserved alongside the tenant filter")
}

// H-05: the spoofing case — the whole point of forcing the filter.
func TestDeleteByQueryTenantFilterOverridesCallerSuppliedValue(t *testing.T) {
	_, rec, mux := newRecordingHandler(t)

	body, err := json.Marshal(deleteByQueryRequest{Filters: map[string]string{"_tenant": "victim"}})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, tenantRequest("DELETE", "/v1/documents/query", "tenant-a", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant-a", rec.gotDeleteFilters["_tenant"],
		"a caller must not be able to delete another tenant's documents")
}

// H-05: deleting by raw document ID must still be constrained to the caller's
// tenant, or IDs alone are enough to destroy another tenant's documents.
func TestDeleteByIDIsTenantScoped(t *testing.T) {
	_, rec, mux := newRecordingHandler(t)

	body, err := json.Marshal(deleteRequest{DocIDs: []uint32{1, 2, 3}})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, tenantRequest("DELETE", "/v1/documents", "tenant-a", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant-a", rec.gotDeleteFilters["_tenant"],
		"delete-by-id must carry the caller's tenant constraint to the backend")
}

// M-14: autocomplete leaks terms across tenants when it is not scoped.
func TestSuggestIsTenantScoped(t *testing.T) {
	_, rec, mux := newRecordingHandler(t)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, tenantRequest("GET", "/v1/suggest?q=acme", "tenant-a", nil))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant-a", rec.gotSuggestFilter["_tenant"],
		"suggestions must be drawn only from the caller's tenant")
}
