package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/efathom/yase/pkg/connector"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/efathom/yase/internal/connector/mysql"
)

func startMySQL(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "testpass",
				"MYSQL_DATABASE":      "testdb",
			},
			WaitingFor: wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	t.Cleanup(func() { container.Terminate(ctx) })

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "3306")
	dsn := fmt.Sprintf("root:testpass@tcp(%s:%s)/testdb?parseTime=true", host, port.Port())

	// Wait and seed
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	_, err = db.Exec(`
		CREATE TABLE articles (
			id INT AUTO_INCREMENT PRIMARY KEY,
			title VARCHAR(255) NOT NULL,
			body TEXT NOT NULL,
			author VARCHAR(100),
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO articles (title, body, author, updated_at) VALUES
			('Go Concurrency', 'Goroutines and channels...', 'Alice', '2026-03-28 10:00:00'),
			('MySQL Indexing', 'B-tree indexes in MySQL...', 'Bob', '2026-03-28 11:00:00'),
			('Search Engines', 'How search engines work...', 'Charlie', '2026-03-28 12:00:00')
	`)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	return dsn
}

func TestMySQLConnectorE2E(t *testing.T) {
	dsn := startMySQL(t)
	ctx := context.Background()

	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "mysql",
		Config: map[string]interface{}{
			"dsn":              dsn,
			"tables":           []interface{}{"articles"},
			"content_columns":  []interface{}{"title", "body"},
			"id_column":        "id",
			"timestamp_column": "updated_at",
			"url_template":     "https://app.internal/articles/{id}",
		},
		Auth: &connector.AuthConfig{Method: ""},
	})
	if err != nil {
		t.Fatalf("Create mysql connector: %v", err)
	}
	defer conn.Close()

	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	catalog, err := conn.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(catalog.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(catalog.Streams))
	}

	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "articles", SyncMode: connector.FullRefresh},
	}, state)

	var collected []connector.Record
	for r := range records {
		collected = append(collected, r)
		t.Logf("Article: id=%s url=%s", r.ID, r.URL)
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}

	if len(collected) != 3 {
		t.Fatalf("expected 3 articles, got %d", len(collected))
	}

	for _, r := range collected {
		if r.Metadata["source"] != "mysql" {
			t.Errorf("source: got %q", r.Metadata["source"])
		}
		if len(r.Content) == 0 {
			t.Errorf("empty content for %s", r.ID)
		}
	}
}

func TestMySQLConnectorIncremental(t *testing.T) {
	dsn := startMySQL(t)
	ctx := context.Background()

	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "mysql",
		Config: map[string]interface{}{
			"dsn":              dsn,
			"tables":           []interface{}{"articles"},
			"content_columns":  []interface{}{"title", "body"},
			"id_column":        "id",
			"timestamp_column": "updated_at",
		},
		Auth: &connector.AuthConfig{Method: ""},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer conn.Close()

	// First sync
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
		t.Fatalf("expected 3, got %d", count)
	}

	raw := state.GetStreamState("articles")
	if raw == nil {
		t.Fatal("expected state saved")
	}
	t.Logf("State: %s", raw)

	// Insert new row
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	_, err = db.Exec("INSERT INTO articles (title, body, author, updated_at) VALUES ('New Article', 'Fresh content', 'Dave', '2026-03-29 10:00:00')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Second sync: incremental
	records2, errs2 := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "articles", SyncMode: connector.Incremental},
	}, state)

	count2 := 0
	for r := range records2 {
		count2++
		t.Logf("Incremental: id=%s", r.ID)
	}
	for err := range errs2 {
		t.Errorf("Error: %v", err)
	}

	if count2 != 1 {
		t.Errorf("expected 1 on incremental, got %d", count2)
	}
}
