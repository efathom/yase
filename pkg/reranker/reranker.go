package reranker

import "context"

// Reranker scores query-document pairs using a cross-encoder model.
// Unlike bi-encoder embeddings, cross-encoders process query and document
// jointly through all transformer layers for higher accuracy.
type Reranker interface {
	Rerank(ctx context.Context, query string, texts []string) ([]RerankResult, error)
}

// RerankResult holds the reranker's score for a single document.
// Index refers to the original position in the input texts slice.
type RerankResult struct {
	Index int
	Score float32
}
