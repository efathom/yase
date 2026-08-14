// Package jira implements a YASE connector for Atlassian Jira.
// Supports Cloud and Data Center via REST API v3.
// Streams: issues. Incremental sync via JQL updated timestamp.
package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "jira"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds Jira-specific configuration.
type Config struct {
	BaseURL     string   `json:"base_url"`     // e.g., "https://company.atlassian.net"
	ProjectKeys []string `json:"project_keys"` // filter to specific projects (empty = all)
	JQLFilter   string   `json:"jql_filter"`   // additional JQL filter
	PageSize    int      `json:"page_size"`    // default 50
}

// Connector implements the YASE Connector interface for Jira.
type Connector struct {
	id     string
	config Config
	http   *connector.HTTPConnectorBase
}

// NewConnector creates a Jira connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse jira config: %w", err)
	}

	if config.BaseURL == "" {
		return nil, fmt.Errorf("jira: base_url is required")
	}
	config.BaseURL = strings.TrimRight(config.BaseURL, "/")
	if config.PageSize <= 0 {
		config.PageSize = 50
	}

	auth, err := connector.NewAuthenticator(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("jira auth: %w", err)
	}

	httpBase := connector.NewHTTPConnectorBase(config.BaseURL, auth, 5) // 5 req/s

	return &Connector{
		id:     cfg.Type,
		config: config,
		http:   httpBase,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "Jira" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthBasic, connector.AuthOAuth2, connector.AuthBearer},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, _, err := c.http.DoRequest(ctx, "GET", "/rest/api/3/myself", nil)
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	return &connector.Catalog{
		Streams: []connector.Stream{
			{Name: "issues", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "updated"},
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
			if stream.Name == "issues" {
				c.readIssues(ctx, stream, state, records, errs)
			} else {
				errs <- fmt.Errorf("unknown stream: %s", stream.Name)
			}
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

func (c *Connector) readIssues(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	// Build JQL
	var jqlParts []string
	if len(c.config.ProjectKeys) > 0 {
		jqlParts = append(jqlParts, "project IN ("+strings.Join(c.config.ProjectKeys, ",")+")")
	}
	if c.config.JQLFilter != "" {
		jqlParts = append(jqlParts, c.config.JQLFilter)
	}

	// Incremental: filter by updated timestamp
	var lastUpdated string
	if stream.SyncMode == connector.Incremental {
		if raw := state.GetStreamState(stream.Name); raw != nil {
			var cursorState struct {
				LastUpdated string `json:"last_updated"`
			}
			_ = json.Unmarshal(raw, &cursorState)
			lastUpdated = cursorState.LastUpdated
			if lastUpdated != "" {
				// Validate the cursor is a timestamp before interpolating into
				// JQL, preventing injection from a corrupted/edited state file.
				if !validJiraTimestamp(lastUpdated) {
					errs <- fmt.Errorf("invalid jira cursor timestamp: %q", lastUpdated)
					return
				}
				jqlParts = append(jqlParts, fmt.Sprintf("updated >= '%s'", lastUpdated))
			}
		}
	}

	jqlParts = append(jqlParts, "ORDER BY updated ASC")
	jql := strings.Join(jqlParts, " AND ")

	startAt := 0
	var latestUpdated string

	for {
		params := url.Values{}
		params.Set("jql", jql)
		params.Set("startAt", fmt.Sprintf("%d", startAt))
		params.Set("maxResults", fmt.Sprintf("%d", c.config.PageSize))
		params.Set("fields", "summary,description,status,assignee,reporter,updated,comment")
		path := "/rest/api/3/search?" + params.Encode()

		_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
		if err != nil {
			errs <- fmt.Errorf("search issues: %w", err)
			return
		}

		var resp jiraSearchResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			errs <- fmt.Errorf("parse issues: %w", err)
			return
		}

		for _, issue := range resp.Issues {
			// Build document content from issue fields
			var contentParts []string
			contentParts = append(contentParts, "# "+issue.Fields.Summary)
			if issue.Fields.Description != nil {
				desc, _ := json.Marshal(issue.Fields.Description)
				contentParts = append(contentParts, string(desc))
			}

			// Add comments
			if issue.Fields.Comment != nil {
				for _, comment := range issue.Fields.Comment.Comments {
					if comment.Body != nil {
						commentBody, _ := json.Marshal(comment.Body)
						contentParts = append(contentParts, "## Comment\n"+string(commentBody))
					}
				}
			}

			content := strings.Join(contentParts, "\n\n")
			metadata := map[string]string{
				"issue_key": issue.Key,
				"summary":   issue.Fields.Summary,
				"updated":   issue.Fields.Updated,
				"source":    "jira",
			}
			if issue.Fields.Status != nil {
				metadata["status"] = issue.Fields.Status.Name
			}
			if issue.Fields.Assignee != nil {
				metadata["assignee"] = issue.Fields.Assignee.DisplayName
			}

			issueURL := c.config.BaseURL + "/browse/" + issue.Key
			connector.SendRecord(ctx, records, connector.Record{
				StreamName: stream.Name,
				ID:         issue.Key,
				Content:    []byte(content),
				MimeType:   "text/plain",
				URL:        issueURL,
				Metadata:   metadata,
				Action:     connector.Upsert,
				EmittedAt:  time.Now(),
			})

			if issue.Fields.Updated > latestUpdated {
				latestUpdated = issue.Fields.Updated
			}
		}

		startAt += len(resp.Issues)
		if startAt >= resp.Total || len(resp.Issues) == 0 {
			break
		}
	}

	// Save cursor
	if latestUpdated != "" {
		cursorJSON, _ := json.Marshal(map[string]string{"last_updated": latestUpdated})
		state.SetStreamState(stream.Name, cursorJSON)
	}
}

// --- Jira API response types ---

type jiraSearchResponse struct {
	Total  int         `json:"total"`
	Issues []jiraIssue `json:"issues"`
}

type jiraIssue struct {
	Key    string `json:"key"`
	Fields struct {
		Summary     string           `json:"summary"`
		Description json.RawMessage  `json:"description"` // ADF format
		Status      *jiraNamedField  `json:"status"`
		Assignee    *jiraUser        `json:"assignee"`
		Reporter    *jiraUser        `json:"reporter"`
		Updated     string           `json:"updated"`
		Comment     *jiraCommentPage `json:"comment"`
	} `json:"fields"`
}

type jiraNamedField struct {
	Name string `json:"name"`
}

type jiraUser struct {
	DisplayName string `json:"displayName"`
	AccountID   string `json:"accountId"`
}

type jiraCommentPage struct {
	Comments []jiraComment `json:"comments"`
	Total    int           `json:"total"`
}

type jiraComment struct {
	Body    json.RawMessage `json:"body"` // ADF format
	Author  *jiraUser       `json:"author"`
	Created string          `json:"created"`
}

// validJiraTimestamp reports whether s is a Jira-acceptable timestamp format.
func validJiraTimestamp(s string) bool {
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02 15:04",
		"2006-01-02",
	} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}
