package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

// --- SyncState tests ---

func TestSyncStateGetSet(t *testing.T) {
	state := NewSyncState()

	state.SetStreamState("pages", json.RawMessage(`{"cursor": "abc123"}`))
	got := state.GetStreamState("pages")
	if string(got) != `{"cursor": "abc123"}` {
		t.Errorf("got %s, want cursor abc123", got)
	}

	// Missing stream returns nil
	if state.GetStreamState("nonexistent") != nil {
		t.Error("expected nil for nonexistent stream")
	}
}

func TestSyncStateNilSafe(t *testing.T) {
	var state *SyncState
	if state.GetStreamState("any") != nil {
		t.Error("expected nil from nil state")
	}
}

// --- Registry tests ---

func TestRegistryRegisterAndCreate(t *testing.T) {
	reg := NewRegistry()

	reg.Register("test", func(cfg ConnectorConfig) (Connector, error) {
		return &mockConnector{id: "test-instance"}, nil
	})

	types := reg.List()
	if len(types) != 1 || types[0] != "test" {
		t.Errorf("List: got %v, want [test]", types)
	}

	conn, err := reg.Create(ConnectorConfig{Type: "test"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if conn.ID() != "test-instance" {
		t.Errorf("ID: got %q, want test-instance", conn.ID())
	}
}

func TestRegistryUnknownType(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Create(ConnectorConfig{Type: "nonexistent"})
	if err == nil {
		t.Error("expected error for unknown type")
	}
}

// --- Auth tests ---

func TestNewAuthenticatorBasic(t *testing.T) {
	auth, err := NewAuthenticator(&AuthConfig{
		Method:   AuthBasic,
		Username: "user",
		Password: "pass",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/test", nil)
	auth.Apply(req)

	got := req.Header.Get("Authorization")
	if got == "" {
		t.Error("expected Authorization header")
	}
	if got != "Basic dXNlcjpwYXNz" {
		t.Errorf("got %q, want Basic dXNlcjpwYXNz", got)
	}
}

func TestNewAuthenticatorBearer(t *testing.T) {
	auth, err := NewAuthenticator(&AuthConfig{
		Method: AuthBearer,
		APIKey: "my-token",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/test", nil)
	auth.Apply(req)

	got := req.Header.Get("Authorization")
	if got != "Bearer my-token" {
		t.Errorf("got %q, want Bearer my-token", got)
	}
}

func TestNewAuthenticatorAPIKey(t *testing.T) {
	auth, err := NewAuthenticator(&AuthConfig{
		Method: AuthAPIKey,
		APIKey: "key123",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/test", nil)
	auth.Apply(req)

	got := req.Header.Get("X-API-Key")
	if got != "key123" {
		t.Errorf("got %q, want key123", got)
	}
}

func TestNewAuthenticatorNil(t *testing.T) {
	auth, err := NewAuthenticator(nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/test", nil)
	auth.Apply(req)
	// Should not panic
}

// --- HTTPConnectorBase tests ---

func TestHTTPConnectorBaseDoRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "ok"}`))
	}))
	defer server.Close()

	auth, _ := NewAuthenticator(&AuthConfig{Method: AuthBearer, APIKey: "test-token"})
	base := NewHTTPConnectorBase(server.URL, auth, 100)

	_, body, err := base.DoRequest(context.Background(), "GET", "/test", nil)
	if err != nil {
		t.Fatalf("DoRequest: %v", err)
	}

	var resp map[string]string
	json.Unmarshal(body, &resp)
	if resp["status"] != "ok" {
		t.Errorf("got %v", resp)
	}
}

func TestHTTPConnectorBaseRetry(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(503)
			w.Write([]byte("unavailable"))
			return
		}
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	base := NewHTTPConnectorBase(server.URL, nil, 100)
	base.Backoff = BackoffConfig{MaxRetries: 4, BaseDelay: 10 * time.Millisecond}

	_, body, err := base.DoRequest(context.Background(), "GET", "/", nil)
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if string(body) != "ok" {
		t.Errorf("got %q, want ok", body)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

// --- Paginator tests ---

func TestCursorPaginator(t *testing.T) {
	page := 0
	p := NewCursorPaginator("cursor", "limit", 10, func(body []byte) (string, bool, error) {
		page++
		if page >= 3 {
			return "", false, nil
		}
		return fmt.Sprintf("cursor-%d", page), true, nil
	})

	if !p.HasNextPage() {
		t.Error("expected first page")
	}

	params := p.NextPageParams()
	if params.Get("limit") != "10" {
		t.Errorf("limit: got %q", params.Get("limit"))
	}

	p.ProcessResponse(nil) // page 1
	if !p.HasNextPage() {
		t.Error("expected more pages after page 1")
	}

	p.ProcessResponse(nil) // page 2
	if !p.HasNextPage() {
		t.Error("expected more pages after page 2")
	}

	p.ProcessResponse(nil) // page 3
	if p.HasNextPage() {
		t.Error("expected no more pages after page 3")
	}
}

func TestOffsetPaginator(t *testing.T) {
	p := NewOffsetPaginator("startAt", "maxResults", 25, func(body []byte) (int, error) {
		return 60, nil
	})

	// Page 1: offset=0
	params := p.NextPageParams()
	if params.Get("startAt") != "0" {
		t.Errorf("startAt: got %q", params.Get("startAt"))
	}

	p.ProcessResponse(nil) // total=60, offset advances to 25
	if !p.HasNextPage() {
		t.Error("expected more pages (25 < 60)")
	}

	p.ProcessResponse(nil) // offset advances to 50
	if !p.HasNextPage() {
		t.Error("expected more pages (50 < 60)")
	}

	p.ProcessResponse(nil) // offset advances to 75
	if p.HasNextPage() {
		t.Error("expected no more pages (75 >= 60)")
	}
}

// --- FileStateStore tests ---

func TestFileStateStoreRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store, err := NewFileStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Save state
	state := NewSyncState()
	state.SetStreamState("pages", json.RawMessage(`{"cursor": "abc"}`))
	if err := store.SaveState(ctx, "job-1", state); err != nil {
		t.Fatal(err)
	}

	// Load state
	loaded, err := store.LoadState(ctx, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	var cursor map[string]string
	json.Unmarshal(loaded.GetStreamState("pages"), &cursor)
	if cursor["cursor"] != "abc" {
		t.Errorf("got %v", cursor)
	}

	// Load missing state returns empty
	empty, err := store.LoadState(ctx, "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.StreamStates) != 0 {
		t.Error("expected empty state for missing job")
	}
}

func TestFileStateStoreJobs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store, _ := NewFileStateStore(dir)
	ctx := context.Background()

	job := &SyncJob{
		ID:     "test-job",
		Status: JobIdle,
		Mode:   Incremental,
	}

	store.SaveJob(ctx, job)

	loaded, err := store.LoadJob(ctx, "test-job")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID != "test-job" || loaded.Status != JobIdle {
		t.Errorf("got %+v", loaded)
	}

	// List
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
	}

	// Delete
	store.DeleteJob(ctx, "test-job")
	jobs, _ = store.ListJobs(ctx)
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs after delete, got %d", len(jobs))
	}
}

// --- Mock connector for testing ---

type mockConnector struct {
	id string
}

func (m *mockConnector) ID() string                       { return m.id }
func (m *mockConnector) DisplayName() string              { return "Mock" }
func (m *mockConnector) Spec() *ConnectorSpec             { return &ConnectorSpec{} }
func (m *mockConnector) Validate(_ context.Context) error { return nil }
func (m *mockConnector) Discover(_ context.Context) (*Catalog, error) {
	return &Catalog{Streams: []Stream{{Name: "test"}}}, nil
}
func (m *mockConnector) Read(_ context.Context, _ []ConfiguredStream, _ *SyncState) (<-chan Record, <-chan error) {
	records := make(chan Record)
	errs := make(chan error)
	close(records)
	close(errs)
	return records, errs
}
func (m *mockConnector) Close() error { return nil }
