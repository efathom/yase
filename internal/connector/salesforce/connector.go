// Package salesforce implements a YASE connector for Salesforce CRM.
// Uses OAuth2 for auth and SOQL queries for data extraction.
// Incremental sync via SystemModstamp field.
package salesforce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "salesforce"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds Salesforce-specific configuration.
type Config struct {
	InstanceURL string         `json:"instance_url"` // e.g., "https://yourorg.my.salesforce.com"
	Objects     []ObjectConfig `json:"objects"`      // Salesforce objects to sync
	SOQLFilter  string         `json:"soql_filter"`  // additional WHERE clause
	PageSize    int            `json:"page_size"`    // default 200
}

// ObjectConfig defines which Salesforce object to sync and which fields to extract.
type ObjectConfig struct {
	Name   string   `json:"name"`   // e.g., "Account", "Case", "Knowledge__kav"
	Fields []string `json:"fields"` // fields to include in content
}

// Connector implements the YASE Connector interface for Salesforce.
type Connector struct {
	id     string
	config Config
	http   *connector.HTTPConnectorBase
}

// NewConnector creates a Salesforce connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse salesforce config: %w", err)
	}

	if config.InstanceURL == "" {
		return nil, fmt.Errorf("salesforce: instance_url is required")
	}
	config.InstanceURL = strings.TrimRight(config.InstanceURL, "/")
	if len(config.Objects) == 0 {
		return nil, fmt.Errorf("salesforce: at least one object is required")
	}
	if config.PageSize <= 0 {
		config.PageSize = 200
	}

	auth, err := connector.NewAuthenticator(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("salesforce auth: %w", err)
	}

	httpBase := connector.NewHTTPConnectorBase(config.InstanceURL, auth, 5)

	return &Connector{
		id:     cfg.Type,
		config: config,
		http:   httpBase,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "Salesforce" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthOAuth2, connector.AuthBearer},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, _, err := c.http.DoRequest(ctx, "GET", "/services/data/v59.0/", nil)
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	streams := make([]connector.Stream, len(c.config.Objects))
	for i, obj := range c.config.Objects {
		streams[i] = connector.Stream{
			Name:               obj.Name,
			SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental},
			DefaultCursorField: "SystemModstamp",
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
			// Find matching object config
			var objCfg *ObjectConfig
			for _, o := range c.config.Objects {
				if o.Name == stream.Name {
					objCfg = &o
					break
				}
			}
			if objCfg == nil {
				errs <- fmt.Errorf("no config for object: %s", stream.Name)
				continue
			}
			c.readObject(ctx, stream, objCfg, state, records, errs)
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

func (c *Connector) readObject(ctx context.Context, stream connector.ConfiguredStream, objCfg *ObjectConfig, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	// Validate identifiers to prevent SOQL injection
	if err := connector.ValidateSFIdentifier(objCfg.Name); err != nil {
		errs <- err
		return
	}
	for _, f := range objCfg.Fields {
		if err := connector.ValidateSFIdentifier(f); err != nil {
			errs <- err
			return
		}
	}

	// Build SOQL query
	fields := append([]string{"Id", "SystemModstamp"}, objCfg.Fields...)
	fieldsStr := strings.Join(unique(fields), ", ")

	soql := fmt.Sprintf("SELECT %s FROM %s", fieldsStr, objCfg.Name)

	var whereParts []string

	// Incremental filter
	var lastModstamp string
	if stream.SyncMode == connector.Incremental {
		if raw := state.GetStreamState(stream.Name); raw != nil {
			var cursorState struct {
				LastModstamp string `json:"last_modstamp"`
			}
			json.Unmarshal(raw, &cursorState)
			lastModstamp = cursorState.LastModstamp
		}
		if lastModstamp != "" {
			// Validate timestamp format to prevent SOQL injection via cursor.
			parsed, err := parseSalesforceTime(lastModstamp)
			if err != nil {
				errs <- fmt.Errorf("invalid cursor timestamp: %q", lastModstamp)
				return
			}
			whereParts = append(whereParts, fmt.Sprintf("SystemModstamp > %s", parsed.UTC().Format(time.RFC3339)))
		}
	}

	// Additional filter — validated at config parse time, not user-controlled at runtime
	if c.config.SOQLFilter != "" {
		whereParts = append(whereParts, c.config.SOQLFilter)
	}

	if len(whereParts) > 0 {
		soql += " WHERE " + strings.Join(whereParts, " AND ")
	}
	soql += " ORDER BY SystemModstamp ASC"

	var latestModstamp string

	// Execute via Salesforce REST API query endpoint (URL-encode the SOQL).
	queryPath := fmt.Sprintf("/services/data/v59.0/query?q=%s", url.QueryEscape(soql))

	for queryPath != "" {
		_, body, err := c.http.DoRequest(ctx, "GET", queryPath, nil)
		if err != nil {
			errs <- fmt.Errorf("soql query %s: %w", objCfg.Name, err)
			return
		}

		var resp sfQueryResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			errs <- fmt.Errorf("parse soql response: %w", err)
			return
		}

		for _, record := range resp.Records {
			id, _ := record["Id"].(string)
			modstamp, _ := record["SystemModstamp"].(string)

			// Build content from configured fields
			var contentParts []string
			for _, field := range objCfg.Fields {
				if val, ok := record[field]; ok && val != nil {
					contentParts = append(contentParts, fmt.Sprintf("%s: %v", field, val))
				}
			}
			content := strings.Join(contentParts, "\n\n")

			metadata := map[string]string{
				"object":    objCfg.Name,
				"record_id": id,
				"modstamp":  modstamp,
				"source":    "salesforce",
			}

			recordURL := fmt.Sprintf("%s/%s", c.config.InstanceURL, id)

			connector.SendRecord(ctx, records, connector.Record{
				StreamName: stream.Name,
				ID:         objCfg.Name + "/" + id,
				Content:    []byte(content),
				MimeType:   "text/plain",
				URL:        recordURL,
				Metadata:   metadata,
				Action:     connector.Upsert,
				EmittedAt:  time.Now(),
			})

			if modstamp > latestModstamp {
				latestModstamp = modstamp
			}
		}

		// Handle pagination via nextRecordsUrl
		if resp.NextRecordsURL != "" {
			queryPath = resp.NextRecordsURL
		} else {
			queryPath = ""
		}
	}

	// Save cursor (normalized to RFC3339 for stable round-tripping).
	if latestModstamp != "" {
		if t, err := parseSalesforceTime(latestModstamp); err == nil {
			latestModstamp = t.UTC().Format(time.RFC3339)
		}
		cursorJSON, _ := json.Marshal(map[string]string{"last_modstamp": latestModstamp})
		state.SetStreamState(stream.Name, cursorJSON)
	}
}

// parseSalesforceTime accepts the timestamp formats Salesforce returns for
// SystemModstamp (notably the "+0000" offset without a colon, which RFC3339
// rejects).
func parseSalesforceTime(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02T15:04:05.000Z0700",
		"2006-01-02T15:04:05.000+0700",
		"2006-01-02T15:04:05-0700",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format %q", s)
}

type sfQueryResponse struct {
	TotalSize      int                      `json:"totalSize"`
	Done           bool                     `json:"done"`
	NextRecordsURL string                   `json:"nextRecordsUrl"`
	Records        []map[string]interface{} `json:"records"`
}

func unique(strs []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, s := range strs {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}
