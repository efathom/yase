package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/efathom/yase/internal/indexsvc"
	"github.com/efathom/yase/pkg/cache"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// splitDeployment wires a gateway mux to an indexer over an in-memory gRPC
// connection, mirroring cmd/gateway + cmd/indexer.
type splitDeployment struct {
	mux    *http.ServeMux
	mgr    *collection.Manager
	remote *RemoteSearcher
	emb    embedder.Embedder
}

func newSplitDeployment(t *testing.T) *splitDeployment {
	t.Helper()
	dir := t.TempDir()
	store, err := collection.NewFileStore(dir)
	require.NoError(t, err)
	cfg := &config.Config{Embedder: config.EmbedderConfig{Provider: "mock", Dimension: 16}}
	mgr := collection.NewManager(store, dir, cfg, nil)
	require.NoError(t, mgr.RestoreAll(context.Background()))
	t.Cleanup(func() { mgr.Close() })

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ingestionv1.RegisterIndexServiceServer(srv, indexsvc.NewCollectionServer(mgr))
	ingestionv1.RegisterCollectionServiceServer(srv, indexsvc.NewCollectionService(mgr))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { cc.Close() })

	emb := embedder.NewMockEmbedder(16)
	remote := &RemoteSearcher{Client: ingestionv1.NewIndexServiceClient(cc)}
	h := NewHandler(remote, emb)
	h.WithCollectionBackend(&RemoteCollectionSearch{Client: ingestionv1.NewIndexServiceClient(cc)})
	h.SetCache(cache.NewSearchCache(collection.DefaultCollectionID, 100, time.Hour).SetGeneration(remote.Generation))
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	NewCollectionHandler(&RemoteCollections{Client: ingestionv1.NewCollectionServiceClient(cc)}).RegisterRoutes(mux)
	return &splitDeployment{mux: mux, mgr: mgr, remote: remote, emb: emb}
}

func (d *splitDeployment) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	d.mux.ServeHTTP(rec, httptest.NewRequest(method, path, bytes.NewReader(b)))
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func (d *splitDeployment) ingest(t *testing.T, colID string, id uint32, text string) {
	t.Helper()
	eng, err := d.mgr.GetEngine(colID)
	require.NoError(t, err)
	vec, err := d.emb.Embed(context.Background(), text)
	require.NoError(t, err)
	require.NoError(t, eng.Ingest(context.Background(), index.Document{ID: id, Text: text, Vector: vec,
		Metadata: map[string]string{"url": "https://example.com/" + text}}))
}

func TestSplitDeploymentEndToEnd(t *testing.T) {
	d := newSplitDeployment(t)

	// Collections CRUD works remotely and never returns credentials.
	code, body := d.do(t, "POST", "/v1/collections", map[string]any{
		"id": "docs", "name": "Docs",
		"config": map[string]any{"embedder": map[string]any{"provider": "mock", "dimension": 16, "api_key": "sk-secret"}},
	})
	require.Equal(t, http.StatusCreated, code, body)
	assert.Equal(t, "***", body["config"].(map[string]any)["embedder"].(map[string]any)["api_key"])

	code, _ = d.do(t, "POST", "/v1/collections", map[string]any{"id": "../etc", "name": "bad"})
	assert.Equal(t, http.StatusBadRequest, code, "validation errors surface as 400 through gRPC")

	d.ingest(t, "docs", 1, "hybrid search fuses bm25 and vectors")
	d.ingest(t, collection.DefaultCollectionID, 2, "reciprocal rank fusion merges lists")

	// Single-collection search: the indexer embeds with the collection's model.
	code, body = d.do(t, "POST", "/v1/collections/docs/search", map[string]any{"query": "hybrid search", "top_k": 5})
	require.Equal(t, http.StatusOK, code, body)
	assert.EqualValues(t, 1, body["count"])

	code, body = d.do(t, "POST", "/v1/collections/search", map[string]any{"query": "fusion", "top_k": 5})
	require.Equal(t, http.StatusOK, code, body)
	assert.EqualValues(t, 2, body["count"], "cross-collection search spans both collections")

	code, _ = d.do(t, "POST", "/v1/collections/missing/search", map[string]any{"query": "x"})
	assert.Equal(t, http.StatusNotFound, code)

	// RAG enrichment returns stored text through GetDocuments.
	code, body = d.do(t, "POST", "/v1/rag", map[string]any{"query": "rank fusion", "top_k": 3, "include_text": true})
	require.Equal(t, http.StatusOK, code, body)
	cits := body["citations"].([]any)
	require.NotEmpty(t, cits)
	assert.Equal(t, "reciprocal rank fusion merges lists", cits[0].(map[string]any)["chunk_text"])

	// Search is cached; a remote delete bumps the generation and evicts it.
	code, body = d.do(t, "POST", "/search", map[string]any{"query": "rank fusion", "top_k": 3})
	require.Equal(t, http.StatusOK, code)
	require.EqualValues(t, 1, body["count"])
	code, body = d.do(t, "DELETE", "/v1/documents", map[string]any{"doc_ids": []uint32{2}})
	require.Equal(t, http.StatusOK, code, body)
	assert.EqualValues(t, 1, body["deleted_count"])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.remote.WatchGeneration(ctx, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		_, b := d.do(t, "POST", "/search", map[string]any{"query": "rank fusion", "top_k": 3})
		return b["count"] == float64(0)
	}, 2*time.Second, 20*time.Millisecond, "deleted document must stop being served from cache")

	code, body = d.do(t, "GET", "/v1/collections/docs/stats", nil)
	require.Equal(t, http.StatusOK, code, body)
	assert.EqualValues(t, 1, body["documents"])
}

func TestClientIPTrustedProxies(t *testing.T) {
	proxies, err := parseTrustedProxies([]string{"10.0.0.0/8"})
	require.NoError(t, err)
	tests := []struct {
		name    string
		remote  string
		xff     string
		proxies bool
		want    string
	}{
		{"untrusted peer cannot spoof", "203.0.113.9:1234", "1.2.3.4", true, "203.0.113.9"},
		{"no proxies configured ignores header", "10.0.0.5:1", "1.2.3.4", false, "10.0.0.5"},
		{"trusted proxy forwards client", "10.0.0.5:1", "198.51.100.7", true, "198.51.100.7"},
		{"client-supplied prefix is skipped", "10.0.0.5:1", "1.2.3.4, 198.51.100.7, 10.1.1.1", true, "198.51.100.7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tt.remote
			r.Header.Set("X-Forwarded-For", tt.xff)
			var p = proxies
			if !tt.proxies {
				p = nil
			}
			assert.Equal(t, tt.want, clientIP(r, p))
		})
	}
}
