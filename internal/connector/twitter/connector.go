// Package twitter implements a YASE connector for Twitter/X.
// Uses the X API v2 with OAuth2 Bearer token authentication.
// Supports user timeline, search, and list timeline streams.
// Incremental sync via since_id parameter.
package twitter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "twitter"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds Twitter/X-specific configuration.
type Config struct {
	UserIDs       []string `json:"user_ids"`       // user IDs for timeline sync
	Usernames     []string `json:"usernames"`      // resolve to user IDs at runtime
	SearchQueries []string `json:"search_queries"` // search queries (e.g., "#golang", "from:user")
	ListIDs       []string `json:"list_ids"`       // list IDs to sync
	MaxResults    int      `json:"max_results"`    // per request (10-100, default 100)
	IncludeRTs    bool     `json:"include_rts"`    // include retweets
}

// Connector implements the YASE Connector interface for Twitter/X.
type Connector struct {
	id     string
	config Config
	http   *connector.HTTPConnectorBase
}

// NewConnector creates a Twitter connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse twitter config: %w", err)
	}

	if len(config.UserIDs) == 0 && len(config.Usernames) == 0 &&
		len(config.SearchQueries) == 0 && len(config.ListIDs) == 0 {
		return nil, fmt.Errorf("twitter: at least one of user_ids, usernames, search_queries, or list_ids is required")
	}
	if config.MaxResults <= 0 || config.MaxResults > 100 {
		config.MaxResults = 100
	}

	auth, err := connector.NewAuthenticator(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("twitter auth: %w", err)
	}

	// X API v2 base URL
	httpBase := connector.NewHTTPConnectorBase("https://api.twitter.com/2", auth, 1) // 1 req/s (X API is strict)

	return &Connector{
		id:     cfg.Type,
		config: config,
		http:   httpBase,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "Twitter/X" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthBearer, connector.AuthOAuth2},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	// Verify bearer token with a lightweight endpoint
	_, _, err := c.http.DoRequest(ctx, "GET", "/users/me", nil)
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	var streams []connector.Stream

	if len(c.config.UserIDs) > 0 || len(c.config.Usernames) > 0 {
		streams = append(streams, connector.Stream{
			Name:               "timelines",
			SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental},
			DefaultCursorField: "since_id",
		})
	}
	if len(c.config.SearchQueries) > 0 {
		streams = append(streams, connector.Stream{
			Name:               "search",
			SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental},
			DefaultCursorField: "since_id",
		})
	}
	if len(c.config.ListIDs) > 0 {
		streams = append(streams, connector.Stream{
			Name:               "lists",
			SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental},
			DefaultCursorField: "since_id",
		})
	}

	return &connector.Catalog{Streams: streams}, nil
}

func (c *Connector) Read(ctx context.Context, streams []connector.ConfiguredStream, state *connector.SyncState) (<-chan connector.Record, <-chan error) {
	records := make(chan connector.Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		// Resolve usernames to IDs if needed
		if len(c.config.Usernames) > 0 {
			ids, err := c.resolveUsernames(ctx)
			if err != nil {
				errs <- err
			} else {
				c.config.UserIDs = append(c.config.UserIDs, ids...)
			}
		}

		for _, stream := range streams {
			switch stream.Name {
			case "timelines":
				c.readTimelines(ctx, stream, state, records, errs)
			case "search":
				c.readSearch(ctx, stream, state, records, errs)
			case "lists":
				c.readLists(ctx, stream, state, records, errs)
			default:
				errs <- fmt.Errorf("unknown stream: %s", stream.Name)
			}
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

func (c *Connector) resolveUsernames(ctx context.Context) ([]string, error) {
	usernames := strings.Join(c.config.Usernames, ",")
	path := fmt.Sprintf("/users/by?usernames=%s", usernames)
	_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("resolve usernames: %w", err)
	}

	var resp struct {
		Data []struct {
			ID       string `json:"id"`
			Username string `json:"username"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &resp)

	var ids []string
	for _, u := range resp.Data {
		ids = append(ids, u.ID)
	}
	return ids, nil
}

func (c *Connector) readTimelines(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	for _, userID := range c.config.UserIDs {
		stateKey := "timeline:" + userID
		sinceID := c.getSinceID(state, stateKey, stream.SyncMode)

		path := fmt.Sprintf("/users/%s/tweets?max_results=%d&tweet.fields=created_at,author_id,conversation_id,public_metrics",
			userID, c.config.MaxResults)
		if sinceID != "" {
			path += "&since_id=" + sinceID
		}
		if !c.config.IncludeRTs {
			path += "&exclude=retweets"
		}

		latestID := c.fetchTweets(ctx, path, stateKey, records, errs)
		if latestID != "" {
			c.saveSinceID(state, stateKey, latestID)
		}
	}
}

func (c *Connector) readSearch(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	for _, query := range c.config.SearchQueries {
		stateKey := "search:" + query
		sinceID := c.getSinceID(state, stateKey, stream.SyncMode)

		path := fmt.Sprintf("/tweets/search/recent?query=%s&max_results=%d&tweet.fields=created_at,author_id,conversation_id,public_metrics",
			strings.ReplaceAll(query, " ", "%20"), c.config.MaxResults)
		if sinceID != "" {
			path += "&since_id=" + sinceID
		}

		latestID := c.fetchTweets(ctx, path, stateKey, records, errs)
		if latestID != "" {
			c.saveSinceID(state, stateKey, latestID)
		}
	}
}

func (c *Connector) readLists(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	for _, listID := range c.config.ListIDs {
		stateKey := "list:" + listID
		sinceID := c.getSinceID(state, stateKey, stream.SyncMode)

		path := fmt.Sprintf("/lists/%s/tweets?max_results=%d&tweet.fields=created_at,author_id,conversation_id,public_metrics",
			listID, c.config.MaxResults)
		if sinceID != "" {
			path += "&since_id=" + sinceID
		}

		latestID := c.fetchTweets(ctx, path, stateKey, records, errs)
		if latestID != "" {
			c.saveSinceID(state, stateKey, latestID)
		}
	}
}

func (c *Connector) fetchTweets(ctx context.Context, path, stateKey string, records chan<- connector.Record, errs chan<- error) string {
	var latestID string
	paginationToken := ""

	for {
		reqPath := path
		if paginationToken != "" {
			reqPath += "&pagination_token=" + paginationToken
		}

		_, body, err := c.http.DoRequest(ctx, "GET", reqPath, nil)
		if err != nil {
			errs <- fmt.Errorf("fetch tweets: %w", err)
			return latestID
		}

		var resp tweetResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			errs <- fmt.Errorf("parse tweets: %w", err)
			return latestID
		}

		for _, tweet := range resp.Data {
			metadata := map[string]string{
				"tweet_id":   tweet.ID,
				"author_id":  tweet.AuthorID,
				"created_at": tweet.CreatedAt,
				"source":     "twitter",
			}
			if tweet.PublicMetrics != nil {
				metadata["likes"] = fmt.Sprintf("%d", tweet.PublicMetrics.LikeCount)
				metadata["retweets"] = fmt.Sprintf("%d", tweet.PublicMetrics.RetweetCount)
				metadata["replies"] = fmt.Sprintf("%d", tweet.PublicMetrics.ReplyCount)
			}

			tweetURL := fmt.Sprintf("https://twitter.com/i/status/%s", tweet.ID)

			connector.SendRecord(ctx, records, connector.Record{
				StreamName: stateKey,
				ID:         tweet.ID,
				Content:    []byte(tweet.Text),
				MimeType:   "text/plain",
				URL:        tweetURL,
				Metadata:   metadata,
				Action:     connector.Upsert,
				EmittedAt:  time.Now(),
			})

			if numericIDGreater(tweet.ID, latestID) {
				latestID = tweet.ID
			}
		}

		// Check pagination
		if resp.Meta.NextToken == "" || len(resp.Data) == 0 {
			break
		}
		paginationToken = resp.Meta.NextToken
	}

	return latestID
}

func (c *Connector) getSinceID(state *connector.SyncState, stateKey string, mode connector.SyncMode) string {
	if mode != connector.Incremental {
		return ""
	}
	if raw := state.GetStreamState(stateKey); raw != nil {
		var cursorState struct {
			SinceID string `json:"since_id"`
		}
		_ = json.Unmarshal(raw, &cursorState)
		return cursorState.SinceID
	}
	return ""
}

func (c *Connector) saveSinceID(state *connector.SyncState, stateKey, sinceID string) {
	cursorJSON, _ := json.Marshal(map[string]string{"since_id": sinceID})
	state.SetStreamState(stateKey, cursorJSON)
}

// --- Twitter API v2 response types ---

type tweetResponse struct {
	Data []tweet   `json:"data"`
	Meta tweetMeta `json:"meta"`
}

type tweet struct {
	ID             string         `json:"id"`
	Text           string         `json:"text"`
	AuthorID       string         `json:"author_id"`
	CreatedAt      string         `json:"created_at"`
	ConversationID string         `json:"conversation_id"`
	PublicMetrics  *publicMetrics `json:"public_metrics"`
}

type publicMetrics struct {
	RetweetCount int `json:"retweet_count"`
	ReplyCount   int `json:"reply_count"`
	LikeCount    int `json:"like_count"`
	QuoteCount   int `json:"quote_count"`
}

type tweetMeta struct {
	ResultCount int    `json:"result_count"`
	NextToken   string `json:"next_token"`
	NewestID    string `json:"newest_id"`
	OldestID    string `json:"oldest_id"`
}

// numericIDGreater compares two numeric tweet ID strings numerically,
// falling back to lexicographic comparison on parse errors.
func numericIDGreater(a, b string) bool {
	au, aerr := strconv.ParseUint(a, 10, 64)
	bu, berr := strconv.ParseUint(b, 10, 64)
	if aerr == nil && berr == nil {
		return au > bu
	}
	return a > b
}
