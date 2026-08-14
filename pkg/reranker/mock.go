package reranker

import "context"

// MockReranker returns results in original order with decreasing scores.
// Useful for testing the reranking integration without a real model.
type MockReranker struct{}

func NewMockReranker() *MockReranker {
	return &MockReranker{}
}

func (m *MockReranker) Rerank(_ context.Context, _ string, texts []string) ([]RerankResult, error) {
	results := make([]RerankResult, len(texts))
	for i := range texts {
		results[i] = RerankResult{
			Index: i,
			Score: 1.0 - float32(i)*0.01,
		}
	}
	return results, nil
}
