// Package client provides a Go SDK for the YASE search API.
// Handles authentication, retries, and JSON serialization.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a YASE search API client.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// New creates a YASE client.
func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SearchRequest is the search query parameters.
type SearchRequest struct {
	Query    string            `json:"query"`
	Filters  map[string]string `json:"filters,omitempty"`
	TopK     int               `json:"top_k,omitempty"`
	PageSize int               `json:"page_size,omitempty"`
	Offset   int               `json:"offset,omitempty"`
}

// SearchResponse contains search results.
type SearchResponse struct {
	Status     string         `json:"status"`
	Count      int            `json:"count"`
	Total      int            `json:"total"`
	Offset     int            `json:"offset"`
	DurationMs int64          `json:"duration_ms"`
	Results    []SearchResult `json:"results"`
}

// SearchResult is a single search hit.
type SearchResult struct {
	ID            uint32            `json:"id"`
	Text          string            `json:"text,omitempty"`
	FusedScore    float64           `json:"fused_score"`
	BM25Score     float64           `json:"bm25_score"`
	SemanticScore float32           `json:"semantic_score"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// RAGRequest is the RAG query parameters.
type RAGRequest struct {
	Query       string            `json:"query"`
	Filters     map[string]string `json:"filters,omitempty"`
	TopK        int               `json:"top_k,omitempty"`
	IncludeText bool              `json:"include_text"`
}

// RAGResponse contains RAG results with citations.
type RAGResponse struct {
	Status     string     `json:"status"`
	Query      string     `json:"query"`
	Citations  []Citation `json:"citations"`
	Context    string     `json:"context"`
	DurationMs int64      `json:"duration_ms"`
}

// Citation is a source passage with provenance.
type Citation struct {
	DocID          uint32            `json:"doc_id"`
	ChunkText      string            `json:"chunk_text,omitempty"`
	SourceURL      string            `json:"source_url,omitempty"`
	RelevanceScore float64           `json:"relevance_score"`
	BM25Score      float64           `json:"bm25_score"`
	SemanticScore  float32           `json:"semantic_score"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// Search executes a hybrid search query.
func (c *Client) Search(ctx context.Context, req SearchRequest) (*SearchResponse, error) {
	var resp SearchResponse
	if err := c.doJSON(ctx, "POST", "/search", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// RAG executes a RAG query with citation enrichment.
func (c *Client) RAG(ctx context.Context, req RAGRequest) (*RAGResponse, error) {
	var resp RAGResponse
	if err := c.doJSON(ctx, "POST", "/v1/rag", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Delete removes documents by ID.
func (c *Client) Delete(ctx context.Context, docIDs []uint32) (int, error) {
	var resp struct {
		DeletedCount int `json:"deleted_count"`
	}
	if err := c.doJSON(ctx, "DELETE", "/v1/documents", map[string]interface{}{"doc_ids": docIDs}, &resp); err != nil {
		return 0, err
	}
	return resp.DeletedCount, nil
}

// DeleteByFilter removes documents matching metadata filters.
func (c *Client) DeleteByFilter(ctx context.Context, filters map[string]string) (int, error) {
	var resp struct {
		DeletedCount int `json:"deleted_count"`
	}
	if err := c.doJSON(ctx, "DELETE", "/v1/documents/query", map[string]interface{}{"filters": filters}, &resp); err != nil {
		return 0, err
	}
	return resp.DeletedCount, nil
}

// Health checks if the service is alive.
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("health check failed: HTTP %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, body, result interface{}) error {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	const maxResponseSize = 10 * 1024 * 1024 // 10 MB
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}
