package embedder

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

// OpenAIEmbedder calls the OpenAI embeddings API with retry and batching.
type OpenAIEmbedder struct {
	apiKey     string
	model      string
	dim        int
	baseURL    string
	maxBatch   int
	httpClient *http.Client
}

// OpenAIOption configures an OpenAIEmbedder.
type OpenAIOption func(*OpenAIEmbedder)

// WithBaseURL overrides the default OpenAI API base URL (useful for testing).
func WithBaseURL(url string) OpenAIOption {
	return func(o *OpenAIEmbedder) { o.baseURL = url }
}

// WithHTTPClient overrides the default HTTP client.
func WithHTTPClient(c *http.Client) OpenAIOption {
	return func(o *OpenAIEmbedder) { o.httpClient = c }
}

// WithMaxBatch overrides the maximum number of texts per API call (default 2048).
func WithMaxBatch(n int) OpenAIOption {
	return func(o *OpenAIEmbedder) { o.maxBatch = n }
}

// NewOpenAIEmbedder creates an OpenAI embeddings client.
// Model examples: "text-embedding-3-small" (1536-dim), "text-embedding-3-large" (3072-dim).
func NewOpenAIEmbedder(apiKey, model string, dim int, opts ...OpenAIOption) *OpenAIEmbedder {
	e := &OpenAIEmbedder{
		apiKey:   apiKey,
		model:    model,
		dim:      dim,
		baseURL:  "https://api.openai.com",
		maxBatch: 2048,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Embed returns the vector for a single text.
func (o *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	vecs, err := o.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// EmbedBatch sends texts to the OpenAI embeddings API in batches of up to 2048.
// Retries on 429 and 5xx with exponential backoff + jitter.
func (o *OpenAIEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))

	for start := 0; start < len(texts); start += o.maxBatch {
		end := start + o.maxBatch
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[start:end]

		vecs, err := o.callAPI(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("batch [%d:%d]: %w", start, end, err)
		}

		for _, v := range vecs {
			result[start+v.index] = v.embedding
		}
	}
	return result, nil
}

type indexedVec struct {
	index     int
	embedding []float32
}

func (o *OpenAIEmbedder) callAPI(ctx context.Context, texts []string) ([]indexedVec, error) {
	reqBody := embeddingRequest{Model: o.model, Input: texts}
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

		req, err := http.NewRequestWithContext(ctx, "POST", o.baseURL+"/v1/embeddings", bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+o.apiKey)

		resp, err := o.httpClient.Do(req)
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

		var embResp embeddingResponse
		if err := json.Unmarshal(respBody, &embResp); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		if embResp.Error != nil {
			return nil, fmt.Errorf("API error: %s", embResp.Error.Message)
		}

		result := make([]indexedVec, len(embResp.Data))
		for i, d := range embResp.Data {
			result[i] = indexedVec{index: d.Index, embedding: d.Embedding}
		}
		return result, nil
	}
	return nil, fmt.Errorf("exhausted retries: %w", lastErr)
}

// Dimension returns the configured vector dimensionality.
func (o *OpenAIEmbedder) Dimension() int {
	return o.dim
}
