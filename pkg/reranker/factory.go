package reranker

import (
	"net/http"

	"github.com/efathom/yase/pkg/config"
)

// NewFromConfig creates a Reranker from configuration.
// Returns nil if reranking is disabled (Enabled=false or Provider="").
func NewFromConfig(cfg config.RerankerConfig) Reranker {
	if !cfg.Enabled {
		return nil
	}

	switch cfg.Provider {
	case "tei":
		var opts []TEIOption
		if cfg.Timeout > 0 {
			opts = append(opts, WithTEIHTTPClient(&http.Client{Timeout: cfg.Timeout}))
		}
		baseURL := cfg.BaseURL
		if baseURL == "" {
			baseURL = "http://localhost:8081"
		}
		return NewTEIReranker(baseURL, cfg.Model, opts...)

	case "mock":
		return NewMockReranker()

	default:
		return nil
	}
}
