package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efathom/yase/internal/gateway"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCollectionConfig() *config.Config {
	return &config.Config{
		Embedder: config.EmbedderConfig{
			Provider:  "mock",
			Dimension: 32,
		},
		Index: config.IndexConfig{
			ArenaSize:    64 * 1024 * 1024,
			CentroidRate: 5,
		},
	}
}

func setupCollectionManager(t *testing.T) *collection.Manager {
	t.Helper()
	dir := t.TempDir()
	store, err := collection.NewFileStore(dir)
	require.NoError(t, err)

	mgr := collection.NewManager(store, dir, testCollectionConfig(), nil)
	require.NoError(t, mgr.RestoreAll(context.Background()))
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

// TestCollectionLifecycle tests the full CRUD lifecycle via HTTP handlers.
func TestCollectionLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	mgr := setupCollectionManager(t)
	ch := gateway.NewCollectionHandler(mgr)

	mux := http.NewServeMux()
	ch.RegisterRoutes(mux)

	// 1. Create collection
	body := `{"id":"test-col","name":"Test Collection","tenant_id":"t1"}`
	req := httptest.NewRequest("POST", "/v1/collections", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)

	var created collection.Collection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	assert.Equal(t, "test-col", created.ID)
	assert.Equal(t, "Test Collection", created.Name)
	assert.Equal(t, collection.StatusReady, created.Status)

	// 2. Get collection
	req = httptest.NewRequest("GET", "/v1/collections/test-col", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 3. List collections (should have _default + test-col)
	req = httptest.NewRequest("GET", "/v1/collections", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var listed []*collection.Collection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &listed))
	assert.Len(t, listed, 2) // _default + test-col

	// 4. Update collection
	body = `{"name":"Renamed Collection","description":"Updated"}`
	req = httptest.NewRequest("PUT", "/v1/collections/test-col", strings.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var updated collection.Collection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &updated))
	assert.Equal(t, "Renamed Collection", updated.Name)

	// 5. Bind connector
	body = `{"connector_job_id":"conn-1"}`
	req = httptest.NewRequest("POST", "/v1/collections/test-col/connectors", strings.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 6. Stats
	req = httptest.NewRequest("GET", "/v1/collections/test-col/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 7. Unbind connector
	req = httptest.NewRequest("DELETE", "/v1/collections/test-col/connectors/conn-1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 8. Delete collection
	req = httptest.NewRequest("DELETE", "/v1/collections/test-col", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 9. Verify deleted
	req = httptest.NewRequest("GET", "/v1/collections/test-col", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// 10. Cannot delete _default
	req = httptest.NewRequest("DELETE", "/v1/collections/_default", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// TestCollectionSearchIntegration tests ingest + search across collections.
func TestCollectionSearchIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	mgr := setupCollectionManager(t)
	ctx := context.Background()

	// Create two collections
	_, err := mgr.Create(ctx, "", "docs", "Documentation", collection.CollectionConfig{})
	require.NoError(t, err)
	_, err = mgr.Create(ctx, "", "code", "Source Code", collection.CollectionConfig{})
	require.NoError(t, err)

	// Get embedder and generate vectors
	emb, err := mgr.GetEmbedder("docs")
	require.NoError(t, err)

	docsVec, err := emb.Embed(ctx, "API documentation guide")
	require.NoError(t, err)
	codeVec, err := emb.Embed(ctx, "func main implementation")
	require.NoError(t, err)
	defaultVec, err := emb.Embed(ctx, "default collection content")
	require.NoError(t, err)

	// Ingest into different collections
	docsEngine, err := mgr.GetEngine("docs")
	require.NoError(t, err)
	require.NoError(t, docsEngine.Ingest(ctx, index.Document{
		ID: 100, Text: "API documentation guide", Vector: docsVec,
		Metadata: map[string]string{"source": "wiki"},
	}))

	codeEngine, err := mgr.GetEngine("code")
	require.NoError(t, err)
	require.NoError(t, codeEngine.Ingest(ctx, index.Document{
		ID: 200, Text: "func main implementation", Vector: codeVec,
		Metadata: map[string]string{"source": "github"},
	}))

	defaultEngine, err := mgr.GetEngine(collection.DefaultCollectionID)
	require.NoError(t, err)
	require.NoError(t, defaultEngine.Ingest(ctx, index.Document{
		ID: 300, Text: "default collection content", Vector: defaultVec,
		Metadata: map[string]string{},
	}))

	searcher := collection.NewSearcher(mgr)

	// Test 1: Search single collection "docs"
	queryVec, _ := emb.Embed(ctx, "API")
	results, err := searcher.SearchSingle(ctx, "docs", "API", queryVec, nil, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	for _, r := range results {
		assert.Equal(t, "docs", r.CollectionID)
	}

	// Test 2: Search single collection "code"
	queryVec, _ = emb.Embed(ctx, "func")
	results, err = searcher.SearchSingle(ctx, "code", "func", queryVec, nil, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	for _, r := range results {
		assert.Equal(t, "code", r.CollectionID)
	}

	// Test 3: Search _default
	queryVec, _ = emb.Embed(ctx, "default")
	results, err = searcher.SearchDefault(ctx, "default", queryVec, nil, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	for _, r := range results {
		assert.Equal(t, collection.DefaultCollectionID, r.CollectionID)
	}

	// Test 4: Cross-collection search (all)
	queryVec, _ = emb.Embed(ctx, "content")
	results, err = searcher.Search(ctx, nil, "content", queryVec, nil, 10)
	require.NoError(t, err)
	// Should have results from multiple collections
	collectionIDs := make(map[string]bool)
	for _, r := range results {
		collectionIDs[r.CollectionID] = true
	}
	assert.True(t, len(collectionIDs) >= 1, "should have results from at least 1 collection")

	// Test 5: Cross-collection search (subset)
	results, err = searcher.Search(ctx, []string{"docs", "code"}, "implementation", queryVec, nil, 10)
	require.NoError(t, err)
	for _, r := range results {
		assert.Contains(t, []string{"docs", "code"}, r.CollectionID)
	}

	// Test 6: Delete collection and verify search fails
	require.NoError(t, mgr.Delete(ctx, "docs"))
	_, err = searcher.SearchSingle(ctx, "docs", "API", queryVec, nil, 10)
	assert.Error(t, err)
}
