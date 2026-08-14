package reranker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"
)

// TEIReranker calls the HuggingFace Text Embeddings Inference /rerank endpoint.
type TEIReranker struct {
	baseURL    string
	model      string
	httpClient *http.Client
}

// TEIOption configures a TEIReranker.
type TEIOption func(*TEIReranker)

// WithTEIHTTPClient overrides the default HTTP client.
func WithTEIHTTPClient(c *http.Client) TEIOption {
	return func(r *TEIReranker) { r.httpClient = c }
}

// NewTEIReranker creates a reranker that calls TEI's /rerank endpoint.
func NewTEIReranker(baseURL, model string, opts ...TEIOption) *TEIReranker {
	r := &TEIReranker{
		baseURL: baseURL,
		model:   model,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type rerankRequest struct {
	Query      string   `json:"query"`
	Texts      []string `json:"texts"`
	RawScores  bool     `json:"raw_scores"`
	ReturnText bool     `json:"return_text"`
}

type rerankResponseItem struct {
	Index int     `json:"index"`
	Score float32 `json:"score"`
}

// Rerank sends texts to TEI for cross-encoder scoring against the query.
// Returns results sorted by score descending (most relevant first).
func (r *TEIReranker) Rerank(ctx context.Context, query string, texts []string) ([]RerankResult, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	reqBody := rerankRequest{
		Query: query,
		Texts: texts,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	const maxRetries = 4
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff + jitter):
			}
		}

		req, err := http.NewRequestWithContext(ctx, "POST", r.baseURL+"/rerank", bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.httpClient.Do(req)
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

		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
			continue
		}

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
		}

		var items []rerankResponseItem
		if err := json.Unmarshal(respBody, &items); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}

		results := make([]RerankResult, len(items))
		for i, item := range items {
			results[i] = RerankResult{Index: item.Index, Score: item.Score}
		}
		return results, nil
	}
	return nil, fmt.Errorf("exhausted retries: %w", lastErr)
}
