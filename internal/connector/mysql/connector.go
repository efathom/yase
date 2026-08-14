// Package mysql implements a YASE connector for MySQL/MariaDB databases.
// Uses timestamp-based incremental sync via configurable column.
// Supports password and TLS authentication.
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
	_ "github.com/go-sql-driver/mysql"
)

const connectorType = "mysql"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds MySQL-specific configuration.
type Config struct {
	DSN             string   `json:"dsn"`              // e.g., "user:pass@tcp(localhost:3306)/db"
	Tables          []string `json:"tables"`           // tables to sync
	TimestampColumn string   `json:"timestamp_column"` // column for incremental (default "updated_at")
	ContentColumns  []string `json:"content_columns"`  // columns to concatenate as content
	IDColumn        string   `json:"id_column"`        // primary key column (default "id")
	URLTemplate     string   `json:"url_template"`     // e.g., "https://app.internal/records/{id}"
}

// Connector implements the YASE Connector interface for MySQL.
type Connector struct {
	id     string
	config Config
	db     *sql.DB
}

// NewConnector creates a MySQL connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse mysql config: %w", err)
	}

	if config.DSN == "" {
		return nil, fmt.Errorf("mysql: dsn is required")
	}
	if len(config.Tables) == 0 {
		return nil, fmt.Errorf("mysql: at least one table is required")
	}
	if config.TimestampColumn == "" {
		config.TimestampColumn = "updated_at"
	}
	if config.IDColumn == "" {
		config.IDColumn = "id"
	}

	// Apply auth credentials to DSN if provided
	dsn := config.DSN
	if cfg.Auth != nil && cfg.Auth.Method == connector.AuthBasic {
		if cfg.Auth.Username != "" && cfg.Auth.Password != "" {
			// Replace or inject user:pass in DSN
			if idx := strings.Index(dsn, "@"); idx >= 0 {
				dsn = cfg.Auth.Username + ":" + cfg.Auth.Password + dsn[idx:]
			} else {
				dsn = cfg.Auth.Username + ":" + cfg.Auth.Password + "@" + dsn
			}
		}
	}

	// Ensure parseTime=true for proper time.Time scanning
	if !strings.Contains(dsn, "parseTime") {
		if strings.Contains(dsn, "?") {
			dsn += "&parseTime=true"
		} else {
			dsn += "?parseTime=true"
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql open: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &Connector{
		id:     cfg.Type,
		config: config,
		db:     db,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "MySQL" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthBasic, connector.AuthMTLS},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	streams := make([]connector.Stream, len(c.config.Tables))
	for i, table := range c.config.Tables {
		streams[i] = connector.Stream{
			Name:               table,
			SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental},
			DefaultCursorField: c.config.TimestampColumn,
		}
	}
	return &connector.Catalog{Streams: streams}, nil
}

func (c *Connector) Read(ctx context.Context, streams []connector.ConfiguredStream, state *connector.SyncState) (<-chan connector.Record, <-chan error) {
	records := make(chan connector.Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		for _, stream := range streams {
			c.readTable(ctx, stream, state, records, errs)
		}
	}()

	return records, errs
}

func (c *Connector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Connector) readTable(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	table := stream.Name

	// Validate all identifiers to prevent SQL injection
	if err := connector.ValidateSQLIdentifier(table); err != nil {
		errs <- err
		return
	}
	if err := connector.ValidateSQLIdentifier(c.config.IDColumn); err != nil {
		errs <- err
		return
	}
	if err := connector.ValidateSQLIdentifier(c.config.TimestampColumn); err != nil {
		errs <- err
		return
	}
	for _, col := range c.config.ContentColumns {
		if err := connector.ValidateSQLIdentifier(col); err != nil {
			errs <- err
			return
		}
	}

	// Build SELECT columns
	selectCols := []string{c.config.IDColumn, c.config.TimestampColumn}
	selectCols = append(selectCols, c.config.ContentColumns...)

	query := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(backtickAll(selectCols), ", "), table)

	var args []interface{}
	var lastTimestamp string

	// Incremental filter
	if stream.SyncMode == connector.Incremental {
		if raw := state.GetStreamState(stream.Name); raw != nil {
			var cursorState struct {
				LastTimestamp string `json:"last_timestamp"`
			}
			_ = json.Unmarshal(raw, &cursorState)
			lastTimestamp = cursorState.LastTimestamp
		}
		if lastTimestamp != "" {
			parsed, err := time.Parse(time.RFC3339, lastTimestamp)
			if err == nil {
				query += fmt.Sprintf(" WHERE `%s` > ?", c.config.TimestampColumn)
				args = append(args, parsed)
			}
		}
	}

	query += fmt.Sprintf(" ORDER BY `%s` ASC", c.config.TimestampColumn)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		errs <- fmt.Errorf("query %s: %w", table, err)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		errs <- fmt.Errorf("columns %s: %w", table, err)
		return
	}

	var latestTimestamp string

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			errs <- fmt.Errorf("scan %s: %w", table, err)
			continue
		}

		rowMap := make(map[string]string)
		for i, col := range cols {
			if values[i] != nil {
				switch v := values[i].(type) {
				case time.Time:
					rowMap[col] = v.Format(time.RFC3339)
				case []byte:
					rowMap[col] = string(v)
				default:
					rowMap[col] = fmt.Sprintf("%v", v)
				}
			}
		}

		docID := rowMap[c.config.IDColumn]
		timestamp := rowMap[c.config.TimestampColumn]

		// Build content
		var contentParts []string
		for _, col := range c.config.ContentColumns {
			if val, ok := rowMap[col]; ok && val != "" {
				contentParts = append(contentParts, val)
			}
		}
		content := strings.Join(contentParts, "\n\n")

		docURL := fmt.Sprintf("mysql://%s/%s", table, docID)
		if c.config.URLTemplate != "" {
			docURL = strings.ReplaceAll(c.config.URLTemplate, "{id}", docID)
		}

		metadata := map[string]string{
			"table":     table,
			"source":    "mysql",
			"row_id":    docID,
			"timestamp": timestamp,
		}

		connector.SendRecord(ctx, records, connector.Record{
			StreamName: stream.Name,
			ID:         table + "/" + docID,
			Content:    []byte(content),
			MimeType:   "text/plain",
			URL:        docURL,
			Metadata:   metadata,
			Action:     connector.Upsert,
			EmittedAt:  time.Now(),
		})

		if timestamp > latestTimestamp {
			latestTimestamp = timestamp
		}
	}

	if err := rows.Err(); err != nil {
		errs <- fmt.Errorf("rows %s: %w", table, err)
	}

	if latestTimestamp != "" {
		cursorJSON, _ := json.Marshal(map[string]string{"last_timestamp": latestTimestamp})
		state.SetStreamState(stream.Name, cursorJSON)
	}
}

func backtickAll(cols []string) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = "`" + col + "`"
	}
	return result
}
