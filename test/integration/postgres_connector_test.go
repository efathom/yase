package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/efathom/yase/pkg/connector"
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/efathom/yase/internal/connector/postgres"
)

func startPostgres(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		tcpostgres.BasicWaitStrategies(),
		tcpostgres.WithSQLDriver("pgx"),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	// Wait for readiness
	_ = wait.ForLog("database system is ready to accept connections")

	// Create test table and seed data
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	_, err = db.Exec(`
		CREATE TABLE articles (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			body TEXT NOT NULL,
			author TEXT,
			updated_at TIMESTAMP DEFAULT NOW()
		);
		INSERT INTO articles (title, body, author, updated_at) VALUES
			('Getting Started with Go', 'Go is a statically typed language...', 'Alice', '2026-03-28 10:00:00'),
			('HNSW Algorithm', 'Hierarchical Navigable Small World graphs...', 'Bob', '2026-03-28 11:00:00'),
			('BM25 Scoring', 'Best Match 25 is a ranking function...', 'Charlie', '2026-03-28 12:00:00');
	`)
	if err != nil {
		t.Fatalf("seed data: %v", err)
	}

	return connStr
}

func TestPostgresConnectorE2E(t *testing.T) {
	connStr := startPostgres(t)
	ctx := context.Background()

	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "postgres",
		Config: map[string]interface{}{
			"connection_string": connStr,
			"tables":            []interface{}{"articles"},
			"content_columns":   []interface{}{"title", "body"},
			"id_column":         "id",
			"timestamp_column":  "updated_at",
			"url_template":      "https://wiki.internal/articles/{id}",
		},
		Auth: &connector.AuthConfig{Method: ""},
	})
	if err != nil {
		t.Fatalf("Create postgres connector: %v", err)
	}
	defer conn.Close()

	// Validate
	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Discover
	catalog, err := conn.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(catalog.Streams) != 1 || catalog.Streams[0].Name != "articles" {
		t.Fatalf("unexpected streams: %v", catalog.Streams)
	}

	// Full refresh read
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "articles", SyncMode: connector.FullRefresh},
	}, state)

	var collected []connector.Record
	for r := range records {
		collected = append(collected, r)
		t.Logf("Article: id=%s url=%s content_len=%d", r.ID, r.URL, len(r.Content))
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}

	if len(collected) != 3 {
		t.Fatalf("expected 3 articles, got %d", len(collected))
	}

	// Verify record content
	for _, r := range collected {
		if r.Metadata["source"] != "postgres" {
			t.Errorf("source: got %q", r.Metadata["source"])
		}
		if r.Metadata["table"] != "articles" {
			t.Errorf("table: got %q", r.Metadata["table"])
		}
		if len(r.Content) == 0 {
			t.Errorf("empty content for %s", r.ID)
		}
		if r.URL == "" {
			t.Errorf("empty URL for %s", r.ID)
		}
	}
}

func TestPostgresConnectorIncremental(t *testing.T) {
	connStr := startPostgres(t)
	ctx := context.Background()

	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "postgres",
		Config: map[string]interface{}{
			"connection_string": connStr,
			"tables":            []interface{}{"articles"},
			"content_columns":   []interface{}{"title", "body"},
			"id_column":         "id",
			"timestamp_column":  "updated_at",
		},
		Auth: &connector.AuthConfig{Method: ""},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer conn.Close()

	// First sync: full
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "articles", SyncMode: connector.Incremental},
	}, state)

	count := 0
	for range records {
		count++
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}
	t.Logf("First sync: %d records", count)

	if count != 3 {
		t.Fatalf("expected 3 on first sync, got %d", count)
	}

	// Verify state saved
	raw := state.GetStreamState("articles")
	if raw == nil {
		t.Fatal("expected state to be saved")
	}
	t.Logf("Saved state: %s", raw)

	// Insert a new row
	db, _ := sql.Open("pgx", connStr)
	defer db.Close()
	_, err = db.Exec("INSERT INTO articles (title, body, author, updated_at) VALUES ('New Article', 'Fresh content', 'Dave', '2026-03-29 10:00:00')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Second sync: incremental — should only get the new row
	records2, errs2 := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "articles", SyncMode: connector.Incremental},
	}, state)

	count2 := 0
	for r := range records2 {
		count2++
		t.Logf("Incremental record: id=%s", r.ID)
	}
	for err := range errs2 {
		t.Errorf("Error: %v", err)
	}

	if count2 != 1 {
		t.Errorf("expected 1 on incremental sync, got %d", count2)
	}
}
