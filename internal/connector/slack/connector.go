// Package slack implements a YASE connector for Slack workspaces.
// Uses bot token authentication to read channel messages and threads.
// Incremental sync via conversation history with oldest timestamp parameter.
package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "slack"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds Slack-specific configuration.
type Config struct {
	ChannelIDs     []string `json:"channel_ids"`     // specific channels (empty = all public)
	IncludeThreads bool     `json:"include_threads"` // fetch thread replies
	IncludeFiles   bool     `json:"include_files"`   // index file content
	PageSize       int      `json:"page_size"`       // default 100
}

// Connector implements the YASE Connector interface for Slack.
type Connector struct {
	id     string
	config Config
	http   *connector.HTTPConnectorBase
}

// NewConnector creates a Slack connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse slack config: %w", err)
	}

	if config.PageSize <= 0 {
		config.PageSize = 100
	}

	auth, err := connector.NewAuthenticator(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("slack auth: %w", err)
	}

	httpBase := connector.NewHTTPConnectorBase("https://slack.com/api", auth, 4) // Slack rate: ~4 req/s

	return &Connector{
		id:     cfg.Type,
		config: config,
		http:   httpBase,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "Slack" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthBearer, connector.AuthOAuth2},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, body, err := c.http.DoRequest(ctx, "GET", "/auth.test", nil)
	if err != nil {
		return err
	}
	var resp slackResponse
	json.Unmarshal(body, &resp)
	if !resp.OK {
		return fmt.Errorf("slack auth.test: %s", resp.Error)
	}
	return nil
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	return &connector.Catalog{
		Streams: []connector.Stream{
			{Name: "messages", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "ts"},
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
			if stream.Name == "messages" {
				c.readMessages(ctx, stream, state, records, errs)
			}
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

func (c *Connector) readMessages(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	channelIDs := c.config.ChannelIDs
	if len(channelIDs) == 0 {
		// List all public channels
		ids, err := c.listPublicChannels(ctx)
		if err != nil {
			errs <- err
			return
		}
		channelIDs = ids
	}

	for _, channelID := range channelIDs {
		c.readChannelHistory(ctx, channelID, stream, state, records, errs)
	}
}

func (c *Connector) listPublicChannels(ctx context.Context) ([]string, error) {
	var channelIDs []string
	cursor := ""

	for {
		path := fmt.Sprintf("/conversations.list?types=public_channel&limit=%d&exclude_archived=true", c.config.PageSize)
		if cursor != "" {
			path += "&cursor=" + cursor
		}

		_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
		if err != nil {
			return nil, fmt.Errorf("list channels: %w", err)
		}

		var resp struct {
			slackResponse
			Channels []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"channels"`
			ResponseMetadata struct {
				NextCursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		json.Unmarshal(body, &resp)
		if !resp.OK {
			return nil, fmt.Errorf("conversations.list: %s", resp.Error)
		}

		for _, ch := range resp.Channels {
			channelIDs = append(channelIDs, ch.ID)
		}

		cursor = resp.ResponseMetadata.NextCursor
		if cursor == "" {
			break
		}
	}

	return channelIDs, nil
}

func (c *Connector) readChannelHistory(ctx context.Context, channelID string, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	// Get oldest timestamp for incremental
	var oldest string
	stateKey := "messages:" + channelID
	if stream.SyncMode == connector.Incremental {
		if raw := state.GetStreamState(stateKey); raw != nil {
			var cursorState struct {
				Latest string `json:"latest"`
			}
			json.Unmarshal(raw, &cursorState)
			oldest = cursorState.Latest
		}
	}

	cursor := ""
	var latestTS string

	for {
		path := fmt.Sprintf("/conversations.history?channel=%s&limit=%d", channelID, c.config.PageSize)
		if oldest != "" {
			path += "&oldest=" + oldest
		}
		if cursor != "" {
			path += "&cursor=" + cursor
		}

		_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
		if err != nil {
			errs <- fmt.Errorf("channel %s history: %w", channelID, err)
			return
		}

		var resp struct {
			slackResponse
			Messages         []slackMessage `json:"messages"`
			ResponseMetadata struct {
				NextCursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		json.Unmarshal(body, &resp)
		if !resp.OK {
			errs <- fmt.Errorf("conversations.history %s: %s", channelID, resp.Error)
			return
		}

		for _, msg := range resp.Messages {
			if msg.SubType == "channel_join" || msg.SubType == "channel_leave" {
				continue // skip system messages
			}

			// Skip the boundary message: Slack's `oldest` is inclusive, so the
			// previous sync's newest message would otherwise be re-emitted.
			if oldest != "" && msg.TS == oldest {
				continue
			}

			content := msg.Text

			// Optionally fetch thread replies
			if c.config.IncludeThreads && msg.ThreadTS != "" && msg.ThreadTS == msg.TS {
				replies, err := c.fetchThreadReplies(ctx, channelID, msg.ThreadTS)
				if err == nil && len(replies) > 0 {
					var threadParts []string
					threadParts = append(threadParts, content)
					for _, reply := range replies {
						if reply.TS != msg.TS {
							threadParts = append(threadParts, reply.Text)
						}
					}
					content = strings.Join(threadParts, "\n---\n")
				}
			}

			metadata := map[string]string{
				"channel":   channelID,
				"user":      msg.User,
				"timestamp": msg.TS,
				"source":    "slack",
			}
			if msg.ThreadTS != "" {
				metadata["thread_ts"] = msg.ThreadTS
			}

			msgURL := fmt.Sprintf("https://slack.com/archives/%s/p%s", channelID, strings.Replace(msg.TS, ".", "", 1))

			connector.SendRecord(ctx, records, connector.Record{
				StreamName: stream.Name,
				ID:         channelID + "/" + msg.TS,
				Content:    []byte(content),
				MimeType:   "text/plain",
				URL:        msgURL,
				Metadata:   metadata,
				Action:     connector.Upsert,
				EmittedAt:  time.Now(),
			})

			if msg.TS > latestTS {
				latestTS = msg.TS
			}
		}

		cursor = resp.ResponseMetadata.NextCursor
		if cursor == "" {
			break
		}
	}

	// Save cursor
	if latestTS != "" {
		cursorJSON, _ := json.Marshal(map[string]string{"latest": latestTS})
		state.SetStreamState(stateKey, cursorJSON)
	}
}

func (c *Connector) fetchThreadReplies(ctx context.Context, channelID, threadTS string) ([]slackMessage, error) {
	path := fmt.Sprintf("/conversations.replies?channel=%s&ts=%s&limit=%d", channelID, threadTS, c.config.PageSize)
	_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}

	var resp struct {
		slackResponse
		Messages []slackMessage `json:"messages"`
	}
	json.Unmarshal(body, &resp)
	if !resp.OK {
		return nil, fmt.Errorf("conversations.replies: %s", resp.Error)
	}
	return resp.Messages, nil
}

// --- Slack API response types ---

type slackResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type slackMessage struct {
	Type     string `json:"type"`
	SubType  string `json:"subtype"`
	User     string `json:"user"`
	Text     string `json:"text"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts"`
}
