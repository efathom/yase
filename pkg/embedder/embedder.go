package embedder

import "context"

// Embedder defines the interface for text embedding providers.
type Embedder interface {
	// Embed returns the vector representation of a single text.
	Embed(ctx context.Context, text string) ([]float32, error)

	// EmbedBatch returns vector representations for multiple texts.
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)

	// Dimension returns the dimensionality of output vectors.
	Dimension() int
}
