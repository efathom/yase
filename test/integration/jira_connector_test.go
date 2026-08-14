package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/efathom/yase/pkg/connector"

	_ "github.com/efathom/yase/internal/connector/jira"
)

func TestJiraConnectorE2E(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/rest/api/3/myself":
			json.NewEncoder(w).Encode(map[string]string{
				"accountId":   "user-001",
				"displayName": "Test User",
			})

		case "/rest/api/3/search":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"total": 2,
				"issues": []map[string]interface{}{
					{
						"key": "BUG-101",
						"fields": map[string]interface{}{
							"summary":     "Login page returns 500",
							"description": map[string]interface{}{"type": "doc", "content": []interface{}{map[string]interface{}{"type": "text", "text": "Steps to reproduce..."}}},
							"status":      map[string]interface{}{"name": "Open"},
							"assignee":    map[string]interface{}{"displayName": "Alice", "accountId": "a1"},
							"updated":     "2026-03-28T10:00:00.000+0000",
							"comment": map[string]interface{}{
								"total":    1,
								"comments": []map[string]interface{}{{"body": map[string]interface{}{"type": "doc", "content": []interface{}{}}, "author": map[string]interface{}{"displayName": "Bob"}, "created": "2026-03-28T11:00:00.000+0000"}},
							},
						},
					},
					{
						"key": "BUG-102",
						"fields": map[string]interface{}{
							"summary":     "Search results not sorted",
							"description": nil,
							"status":      map[string]interface{}{"name": "In Progress"},
							"assignee":    nil,
							"updated":     "2026-03-28T12:00:00.000+0000",
							"comment":     nil,
						},
					},
				},
			})

		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()

	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "jira",
		Config: map[string]interface{}{
			"base_url":     server.URL,
			"project_keys": []interface{}{"BUG"},
			"page_size":    50,
		},
		Auth: &connector.AuthConfig{
			Method:   connector.AuthBasic,
			Username: "test@example.com",
			Password: "test-token",
		},
	})
	if err != nil {
		t.Fatalf("Create jira connector: %v", err)
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
	t.Logf("Discovered %d streams", len(catalog.Streams))

	// Read issues
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "issues", SyncMode: connector.FullRefresh},
	}, state)

	var collected []connector.Record
	for r := range records {
		collected = append(collected, r)
		t.Logf("Issue: key=%s status=%s assignee=%s", r.Metadata["issue_key"], r.Metadata["status"], r.Metadata["assignee"])
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}

	if len(collected) != 2 {
		t.Fatalf("expected 2 issues, got %d", len(collected))
	}

	// Verify first issue
	r := collected[0]
	if r.Metadata["issue_key"] != "BUG-101" {
		t.Errorf("issue_key: got %q", r.Metadata["issue_key"])
	}
	if r.Metadata["status"] != "Open" {
		t.Errorf("status: got %q", r.Metadata["status"])
	}
	if r.Metadata["source"] != "jira" {
		t.Errorf("source: got %q", r.Metadata["source"])
	}
	if len(r.Content) == 0 {
		t.Error("expected non-empty content")
	}

	// Verify second issue (nil assignee)
	r2 := collected[1]
	if r2.Metadata["issue_key"] != "BUG-102" {
		t.Errorf("issue_key: got %q", r2.Metadata["issue_key"])
	}
}
