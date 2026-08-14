package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/time/rate"
)

// BackoffConfig controls retry behavior for HTTP requests.
type BackoffConfig struct {
	MaxRetries int           // default 4
	BaseDelay  time.Duration // default 1s
}

// HTTPConnectorBase provides reusable HTTP functionality for REST API connectors.
// Handles authentication, rate limiting, pagination, and retry with backoff.
type HTTPConnectorBase struct {
	BaseURL     string
	Client      *http.Client
	Auth        Authenticator
	RateLimiter *rate.Limiter
	Backoff     BackoffConfig
}

// NewHTTPConnectorBase creates an HTTP connector base with sensible defaults.
func NewHTTPConnectorBase(baseURL string, auth Authenticator, requestsPerSecond float64) *HTTPConnectorBase {
	if requestsPerSecond <= 0 {
		requestsPerSecond = 10 // default: 10 req/s
	}

	client := &http.Client{Timeout: 30 * time.Second}

	// Apply transport-level TLS (mTLS) if the authenticator provides it.
	if tc, ok := auth.(TLSConfigurer); ok {
		if tlsCfg := tc.TLSConfig(); tlsCfg != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = tlsCfg
			client.Transport = transport
		}
	}

	return &HTTPConnectorBase{
		BaseURL:     baseURL,
		Client:      client,
		Auth:        auth,
		RateLimiter: rate.NewLimiter(rate.Limit(requestsPerSecond), int(requestsPerSecond)),
		Backoff:     BackoffConfig{MaxRetries: 4, BaseDelay: time.Second},
	}
}

// DoRequest executes an HTTP request with auth, rate limiting, and retry.
func (h *HTTPConnectorBase) DoRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, []byte, error) {
	fullURL := h.BaseURL + path

	// Buffer body for retries — io.Reader is consumed on first attempt
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, nil, fmt.Errorf("read request body: %w", err)
		}
	}

	var lastErr error
	for attempt := 0; attempt <= h.Backoff.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * h.Backoff.BaseDelay
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(backoff + jitter):
			}
		}

		// Rate limit
		if err := h.RateLimiter.Wait(ctx); err != nil {
			return nil, nil, fmt.Errorf("rate limiter: %w", err)
		}

		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
		if err != nil {
			return nil, nil, fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Accept", "application/json")

		// Apply authentication
		if h.Auth != nil {
			if err := h.Auth.Apply(req); err != nil {
				return nil, nil, fmt.Errorf("auth: %w", err)
			}
		}

		resp, err := h.Client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read body: %w", err)
			continue
		}

		// Refresh auth on 401/403 and retry
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			if h.Auth != nil {
				if err := h.Auth.Refresh(ctx); err != nil {
					lastErr = fmt.Errorf("auth refresh: %w", err)
					continue
				}
			}
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200))
			continue
		}

		// Retry on 429 or 5xx
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200))
			continue
		}

		if resp.StatusCode >= 400 {
			return resp, respBody, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200))
		}

		return resp, respBody, nil
	}
	return nil, nil, fmt.Errorf("exhausted retries: %w", lastErr)
}

// Paginator controls iteration over paginated API responses.
type Paginator interface {
	// HasNextPage returns true if there are more pages to fetch.
	HasNextPage() bool
	// NextPageParams returns query parameters for the next page request.
	NextPageParams() url.Values
	// ProcessResponse extracts pagination state from the response.
	ProcessResponse(body []byte) error
}

// FetchAllPages iterates through all pages of a paginated API endpoint.
// The parse function converts each response body into Records.
func (h *HTTPConnectorBase) FetchAllPages(
	ctx context.Context,
	path string,
	paginator Paginator,
	parse func(body []byte) ([]Record, error),
) (<-chan Record, <-chan error) {
	records := make(chan Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		for {
			// Build URL with pagination params
			reqPath := path
			if params := paginator.NextPageParams(); len(params) > 0 {
				reqPath = path + "?" + params.Encode()
			}

			_, body, err := h.DoRequest(ctx, "GET", reqPath, nil)
			if err != nil {
				errs <- fmt.Errorf("fetch page: %w", err)
				return
			}

			parsed, err := parse(body)
			if err != nil {
				errs <- fmt.Errorf("parse page: %w", err)
				return
			}

			for _, r := range parsed {
				select {
				case records <- r:
				case <-ctx.Done():
					return
				}
			}

			if err := paginator.ProcessResponse(body); err != nil {
				errs <- fmt.Errorf("process pagination: %w", err)
				return
			}

			if !paginator.HasNextPage() {
				return
			}
		}
	}()

	return records, errs
}

// --- Concrete Paginator Implementations ---

// CursorPaginator handles cursor/token-based pagination.
type CursorPaginator struct {
	cursor    string
	hasMore   bool
	limitKey  string // query param name for page size
	cursorKey string // query param name for cursor
	limit     int
	// extractCursor extracts the next cursor from response body
	ExtractCursor func(body []byte) (cursor string, hasMore bool, err error)
}

func NewCursorPaginator(cursorKey, limitKey string, limit int, extractFn func([]byte) (string, bool, error)) *CursorPaginator {
	return &CursorPaginator{
		hasMore:       true,
		cursorKey:     cursorKey,
		limitKey:      limitKey,
		limit:         limit,
		ExtractCursor: extractFn,
	}
}

func (p *CursorPaginator) HasNextPage() bool { return p.hasMore }

func (p *CursorPaginator) NextPageParams() url.Values {
	params := url.Values{}
	if p.limitKey != "" && p.limit > 0 {
		params.Set(p.limitKey, fmt.Sprintf("%d", p.limit))
	}
	if p.cursor != "" {
		params.Set(p.cursorKey, p.cursor)
	}
	return params
}

func (p *CursorPaginator) ProcessResponse(body []byte) error {
	cursor, hasMore, err := p.ExtractCursor(body)
	if err != nil {
		return err
	}
	p.cursor = cursor
	p.hasMore = hasMore
	return nil
}

// OffsetPaginator handles offset+limit pagination (e.g., Jira).
type OffsetPaginator struct {
	offset    int
	limit     int
	total     int
	offsetKey string
	limitKey  string
	// extractTotal extracts the total count from response body
	ExtractTotal func(body []byte) (total int, err error)
}

func NewOffsetPaginator(offsetKey, limitKey string, limit int, extractFn func([]byte) (int, error)) *OffsetPaginator {
	return &OffsetPaginator{
		offset:       0,
		limit:        limit,
		total:        -1, // unknown until first response
		offsetKey:    offsetKey,
		limitKey:     limitKey,
		ExtractTotal: extractFn,
	}
}

func (p *OffsetPaginator) HasNextPage() bool {
	if p.total < 0 {
		return true // haven't seen first response yet
	}
	return p.offset < p.total
}

func (p *OffsetPaginator) NextPageParams() url.Values {
	params := url.Values{}
	params.Set(p.offsetKey, fmt.Sprintf("%d", p.offset))
	params.Set(p.limitKey, fmt.Sprintf("%d", p.limit))
	return params
}

func (p *OffsetPaginator) ProcessResponse(body []byte) error {
	total, err := p.ExtractTotal(body)
	if err != nil {
		return err
	}
	p.total = total
	p.offset += p.limit
	return nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ReadLimited reads up to max bytes from r, returning an error if the content
// exceeds max (0 = unlimited). Guards against unbounded in-memory buffering of
// large files (PDFs, blobs, etc.).
func ReadLimited(r io.Reader, max int64) ([]byte, error) {
	if max <= 0 {
		return io.ReadAll(r)
	}
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("content exceeds maximum size of %d bytes", max)
	}
	return data, nil
}
