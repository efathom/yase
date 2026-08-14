package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/efathom/yase/pkg/connector"

	// Register connectors
	_ "github.com/efathom/yase/internal/connector/confluence"
)

func TestConfluenceConnectorE2E(t *testing.T) {
	// Stand up a fake Confluence API server
	pageCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/api/v2/spaces" && r.URL.Query().Get("keys") != "":
			// Space lookup
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{"id": "123456"},
				},
			})

		case r.URL.Path == "/api/v2/spaces" && r.URL.Query().Get("limit") == "1":
			// Validate call
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{"id": "123456", "key": "ENG", "name": "Engineering"},
				},
			})

		case r.URL.Path == "/api/v2/pages":
			pageCount++
			pages := []map[string]interface{}{
				{
					"id":      "page-001",
					"title":   "Getting Started with YASE",
					"spaceId": "123456",
					"status":  "current",
					"body": map[string]interface{}{
						"storage": map[string]interface{}{
							"value": "<h1>Getting Started</h1><p>YASE is a hybrid search engine built in Go.</p>",
						},
					},
					"version": map[string]interface{}{
						"number":    1,
						"createdAt": "2026-03-28T10:00:00Z",
						"authorId":  "user-001",
					},
				},
				{
					"id":      "page-002",
					"title":   "Architecture Overview",
					"spaceId": "123456",
					"status":  "current",
					"body": map[string]interface{}{
						"storage": map[string]interface{}{
							"value": "<h1>Architecture</h1><p>HNSW + BM25 hybrid search with RRF fusion.</p>",
						},
					},
					"version": map[string]interface{}{
						"number":    3,
						"createdAt": "2026-03-28T12:00:00Z",
						"authorId":  "user-002",
					},
				},
			}

			// Only return pages on first request (no pagination)
			resp := map[string]interface{}{
				"results": pages,
				"_links":  map[string]interface{}{},
			}
			json.NewEncoder(w).Encode(resp)

		default:
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found: " + r.URL.Path})
		}
	}))
	defer server.Close()

	// Create connector via registry
	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "confluence",
		Config: map[string]interface{}{
			"base_url":   server.URL,
			"space_keys": []interface{}{"ENG"},
			"page_size":  50,
		},
		Auth: &connector.AuthConfig{
			Method:   connector.AuthBasic,
			Username: "test@example.com",
			Password: "test-token",
		},
	})
	if err != nil {
		t.Fatalf("Create confluence connector: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Validate
	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Discover
	catalog, err := conn.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(catalog.Streams) == 0 {
		t.Fatal("expected at least one stream")
	}
	t.Logf("Discovered %d streams: %v", len(catalog.Streams), catalog.Streams)

	// Read pages
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "pages", SyncMode: connector.FullRefresh},
	}, state)

	var collected []connector.Record
	for r := range records {
		collected = append(collected, r)
		t.Logf("Record: id=%s url=%s mime=%s title=%s", r.ID, r.URL, r.MimeType, r.Metadata["title"])
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}

	if len(collected) != 2 {
		t.Fatalf("expected 2 records, got %d", len(collected))
	}

	// Verify first record
	r := collected[0]
	if r.MimeType != "text/html" {
		t.Errorf("MimeType: got %q, want text/html", r.MimeType)
	}
	if r.Metadata["source"] != "confluence" {
		t.Errorf("source: got %q", r.Metadata["source"])
	}
	if r.Metadata["title"] != "Getting Started with YASE" {
		t.Errorf("title: got %q", r.Metadata["title"])
	}
	if len(r.Content) == 0 {
		t.Error("expected non-empty content")
	}
}

func TestConfluenceConnectorIncremental(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		callCount++

		if r.URL.Path == "/api/v2/spaces" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{{"id": "1"}},
			})
			return
		}

		// Return different pages based on call count to simulate incremental
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"id": "page-new", "title": "New Page", "spaceId": "1", "status": "current",
					"body":    map[string]interface{}{"storage": map[string]interface{}{"value": "<p>New content</p>"}},
					"version": map[string]interface{}{"number": 1, "createdAt": "2026-03-29T10:00:00Z", "authorId": "u1"},
				},
			},
			"_links": map[string]interface{}{},
		})
	}))
	defer server.Close()

	conn, _ := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "confluence",
		Config: map[string]interface{}{
			"base_url":   server.URL,
			"space_keys": []interface{}{"ENG"},
		},
		Auth: &connector.AuthConfig{Method: connector.AuthBasic, Username: "u", Password: "p"},
	})
	defer conn.Close()
	ctx := context.Background()

	// First sync: full refresh
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "pages", SyncMode: connector.Incremental},
	}, state)

	var count int
	for range records {
		count++
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}
	t.Logf("First sync: %d records", count)

	// Verify state was saved
	raw := state.GetStreamState("pages")
	if raw == nil {
		t.Fatal("expected incremental state to be saved")
	}
	t.Logf("Saved state: %s", raw)
}
