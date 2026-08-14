package embedder

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/efathom/yase/pkg/config"
)

// NewFromConfig creates an Embedder based on the provider configuration.
// It applies caching if cfg.CacheSize > 0.
func NewFromConfig(cfg config.EmbedderConfig) (Embedder, error) {
	var emb Embedder

	switch cfg.Provider {
	case "mock":
		emb = NewMockEmbedder(cfg.Dimension)

	case "ollama":
		baseURL := cfg.BaseURL
		if baseURL == "" || baseURL == "https://api.openai.com/v1" {
			baseURL = "http://localhost:11434"
		}
		baseURL = strings.TrimSuffix(baseURL, "/v1")

		var opts []OpenAIOption
		opts = append(opts, WithBaseURL(baseURL))
		opts = append(opts, WithMaxBatch(256))
		if cfg.Timeout > 0 {
			opts = append(opts, WithHTTPClient(&http.Client{Timeout: cfg.Timeout}))
		}
		apiKey := cfg.APIKey
		if apiKey == "" {
			apiKey = "ollama"
		}
		emb = NewOpenAIEmbedder(apiKey, cfg.Model, cfg.Dimension, opts...)

	case "tei":
		baseURL := cfg.BaseURL
		if baseURL == "" || baseURL == "https://api.openai.com/v1" {
			baseURL = "http://localhost:8888"
		}
		baseURL = strings.TrimSuffix(baseURL, "/v1")

		var opts []OpenAIOption
		opts = append(opts, WithBaseURL(baseURL))
		opts = append(opts, WithMaxBatch(256))
		if cfg.Timeout > 0 {
			opts = append(opts, WithHTTPClient(&http.Client{Timeout: cfg.Timeout}))
		}
		apiKey := cfg.APIKey
		if apiKey == "" {
			apiKey = "tei"
		}
		emb = NewOpenAIEmbedder(apiKey, cfg.Model, cfg.Dimension, opts...)

	case "openai", "":
		baseURL := strings.TrimSuffix(cfg.BaseURL, "/v1")
		var opts []OpenAIOption
		if baseURL != "" && baseURL != "https://api.openai.com" {
			opts = append(opts, WithBaseURL(baseURL))
		}
		if cfg.Timeout > 0 {
			opts = append(opts, WithHTTPClient(&http.Client{Timeout: cfg.Timeout}))
		}
		emb = NewOpenAIEmbedder(cfg.APIKey, cfg.Model, cfg.Dimension, opts...)

	default:
		return nil, fmt.Errorf("unknown embedder provider: %q", cfg.Provider)
	}

	if cfg.CacheSize > 0 {
		emb = NewCachedEmbedder(emb, cfg.CacheSize)
	}
	return emb, nil
}
