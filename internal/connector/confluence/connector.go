// Package confluence implements a YASE connector for Atlassian Confluence.
// Supports both Cloud (OAuth2/API token) and Data Center (Basic auth) instances.
// Streams: pages, blog posts. Incremental sync via lastModified timestamp.
package confluence

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "confluence"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds Confluence-specific configuration.
type Config struct {
	BaseURL         string   `json:"base_url"`   // e.g., "https://company.atlassian.net/wiki"
	SpaceKeys       []string `json:"space_keys"` // filter to specific spaces (empty = all)
	IncludeArchived bool     `json:"include_archived"`
	PageSize        int      `json:"page_size"` // default 50
}

// Connector implements the YASE Connector interface for Confluence.
type Connector struct {
	id     string
	config Config
	http   *connector.HTTPConnectorBase
}

// NewConnector creates a Confluence connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, err := json.Marshal(cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse confluence config: %w", err)
	}

	if config.BaseURL == "" {
		return nil, fmt.Errorf("confluence: base_url is required")
	}
	config.BaseURL = strings.TrimRight(config.BaseURL, "/")
	if config.PageSize <= 0 {
		config.PageSize = 50
	}

	auth, err := connector.NewAuthenticator(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("confluence auth: %w", err)
	}

	httpBase := connector.NewHTTPConnectorBase(config.BaseURL, auth, 2) // 2 req/s default

	return &Connector{
		id:     cfg.Type,
		config: config,
		http:   httpBase,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "Confluence" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthBasic, connector.AuthOAuth2, connector.AuthBearer},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, _, err := c.http.DoRequest(ctx, "GET", "/api/v2/spaces?limit=1", nil)
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	return &connector.Catalog{
		Streams: []connector.Stream{
			{Name: "pages", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "lastModified"},
			{Name: "blogposts", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "lastModified"},
		},
	}, nil
}

func (c *Connector) Read(ctx context.Context, streams []connector.ConfiguredStream, state *connector.SyncState) (<-chan connector.Record, <-chan error) {
	records := make(chan connector.Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		for _, stream := range streams {
			switch stream.Name {
			case "pages":
				c.readPages(ctx, stream, state, records, errs)
			case "blogposts":
				c.readBlogPosts(ctx, stream, state, records, errs)
			default:
				errs <- fmt.Errorf("unknown stream: %s", stream.Name)
			}
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

// readPages fetches Confluence pages with optional space filtering and incremental sync.
func (c *Connector) readPages(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	path := "/api/v2/pages?body-format=storage&limit=" + fmt.Sprintf("%d", c.config.PageSize)
	incrementalSince := c.readCursor(stream, state)

	maxModified := incrementalSince

	// Read once per space, tracking the maximum cursor across all spaces so a
	// shared state key is not overwritten mid-loop (which would skip data).
	if len(c.config.SpaceKeys) > 0 {
		for _, spaceKey := range c.config.SpaceKeys {
			spaceID, err := c.resolveSpaceID(ctx, spaceKey)
			if err != nil {
				errs <- err
				continue
			}
			latest, err := c.fetchPagesWithCursor(ctx, path+"&space-id="+spaceID, incrementalSince, stream.Name, records, errs)
			if err == nil && latest > maxModified {
				maxModified = latest
			}
		}
	} else {
		latest, err := c.fetchPagesWithCursor(ctx, path, incrementalSince, stream.Name, records, errs)
		if err == nil && latest > maxModified {
			maxModified = latest
		}
	}

	c.saveCursor(stream, state, maxModified)
}

func (c *Connector) readCursor(stream connector.ConfiguredStream, state *connector.SyncState) string {
	if stream.SyncMode != connector.Incremental {
		return ""
	}
	raw := state.GetStreamState(stream.Name)
	if raw == nil {
		return ""
	}
	var cursorState struct {
		LastModified string `json:"last_modified"`
	}
	json.Unmarshal(raw, &cursorState)
	return cursorState.LastModified
}

func (c *Connector) saveCursor(stream connector.ConfiguredStream, state *connector.SyncState, latestModified string) {
	if latestModified == "" {
		return
	}
	cursorJSON, _ := json.Marshal(map[string]string{"last_modified": latestModified})
	state.SetStreamState(stream.Name, cursorJSON)
}

func (c *Connector) resolveSpaceID(ctx context.Context, spaceKey string) (string, error) {
	_, body, err := c.http.DoRequest(ctx, "GET", "/api/v2/spaces?keys="+url.QueryEscape(spaceKey)+"&limit=1", nil)
	if err != nil {
		return "", fmt.Errorf("fetch space %s: %w", spaceKey, err)
	}

	var spacesResp struct {
		Results []struct {
			ID string `json:"id"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &spacesResp); err != nil || len(spacesResp.Results) == 0 {
		return "", fmt.Errorf("space %s not found", spaceKey)
	}
	return spacesResp.Results[0].ID, nil
}

func (c *Connector) fetchPagesWithCursor(ctx context.Context, path string, incrementalSince string, streamName string, records chan<- connector.Record, errs chan<- error) (string, error) {
	cursor := ""
	latestModified := incrementalSince

	for {
		reqPath := path
		if cursor != "" {
			reqPath += "&cursor=" + url.QueryEscape(cursor)
		}

		_, body, err := c.http.DoRequest(ctx, "GET", reqPath, nil)
		if err != nil {
			return "", fmt.Errorf("fetch pages: %w", err)
		}

		var resp confluencePageResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return "", fmt.Errorf("parse pages: %w", err)
		}

		for _, page := range resp.Results {
			// Skip pages not modified since the last sync (strict <, so
			// same-second updates are re-fetched and idempotently overwritten).
			if incrementalSince != "" && page.Version.CreatedAt <= incrementalSince {
				continue
			}

			content := page.Body.Storage.Value
			if content == "" {
				continue
			}

			metadata := map[string]string{
				"title":         page.Title,
				"space_id":      page.SpaceID,
				"status":        page.Status,
				"version":       fmt.Sprintf("%d", page.Version.Number),
				"last_modified": page.Version.CreatedAt,
				"author":        page.Version.AuthorID,
				"source":        "confluence",
			}

			pageURL := c.config.BaseURL + "/pages/" + page.ID
			connector.SendRecord(ctx, records, connector.Record{
				StreamName: streamName,
				ID:         page.ID,
				Content:    []byte(content),
				MimeType:   "text/html",
				URL:        pageURL,
				Metadata:   metadata,
				Action:     connector.Upsert,
				EmittedAt:  time.Now(),
			})

			// Track latest modified
			if page.Version.CreatedAt > latestModified {
				latestModified = page.Version.CreatedAt
			}
		}

		// Check for next page
		if resp.Links.Next == "" {
			break
		}

		// Extract cursor from next link
		parsed, err := url.Parse(resp.Links.Next)
		if err != nil {
			break
		}
		cursor = parsed.Query().Get("cursor")
		if cursor == "" {
			break
		}
	}

	return latestModified, nil
}

func (c *Connector) readBlogPosts(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	// Blog posts use the same API pattern as pages
	path := "/api/v2/blogposts?body-format=storage&limit=" + fmt.Sprintf("%d", c.config.PageSize)
	incrementalSince := c.readCursor(stream, state)
	latest, err := c.fetchPagesWithCursor(ctx, path, incrementalSince, stream.Name, records, errs)
	if err != nil {
		errs <- err
	}
	c.saveCursor(stream, state, latest)
}

// --- Confluence API response types ---

type confluencePageResponse struct {
	Results []confluencePage `json:"results"`
	Links   struct {
		Next string `json:"next"`
	} `json:"_links"`
}

type confluencePage struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	SpaceID string `json:"spaceId"`
	Status  string `json:"status"`
	Body    struct {
		Storage struct {
			Value string `json:"value"`
		} `json:"storage"`
	} `json:"body"`
	Version struct {
		Number    int    `json:"number"`
		CreatedAt string `json:"createdAt"`
		AuthorID  string `json:"authorId"`
	} `json:"version"`
}
