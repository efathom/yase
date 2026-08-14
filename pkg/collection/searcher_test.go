package collection

import (
	"context"
	"testing"

	"github.com/efathom/yase/pkg/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSearcher(t *testing.T, numCollections int) (*Searcher, *Manager) {
	t.Helper()
	m := setupManager(t)
	ctx := context.Background()

	require.NoError(t, m.RestoreAll(ctx))

	for i := 0; i < numCollections; i++ {
		id := "col-" + string(rune('a'+i))
		_, err := m.Create(ctx, "", id, "Collection "+id, CollectionConfig{})
		require.NoError(t, err)
	}

	return NewSearcher(m), m
}

func ingestTestDocs(t *testing.T, m *Manager, collectionID string, docs []index.Document) {
	t.Helper()
	eng, err := m.GetEngine(collectionID)
	require.NoError(t, err)
	ctx := context.Background()
	for _, doc := range docs {
		require.NoError(t, eng.Ingest(ctx, doc))
	}
}

func TestSearcher_SearchDefault(t *testing.T) {
	s, m := setupSearcher(t, 0)
	t.Cleanup(func() { m.Close() })

	// Ingest a doc into _default
	emb, err := m.GetEmbedder(DefaultCollectionID)
	require.NoError(t, err)

	ctx := context.Background()
	vec, err := emb.Embed(ctx, "test document")
	require.NoError(t, err)

	ingestTestDocs(t, m, DefaultCollectionID, []index.Document{
		{ID: 100, Text: "test document about search", Vector: vec, Metadata: map[string]string{}},
	})

	results, err := s.SearchDefault(ctx, "test", vec, nil, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.Equal(t, DefaultCollectionID, results[0].CollectionID)
}

func TestSearcher_SearchSingle(t *testing.T) {
	s, m := setupSearcher(t, 1)
	t.Cleanup(func() { m.Close() })

	emb, err := m.GetEmbedder("col-a")
	require.NoError(t, err)

	ctx := context.Background()
	vec, err := emb.Embed(ctx, "hello world")
	require.NoError(t, err)

	ingestTestDocs(t, m, "col-a", []index.Document{
		{ID: 200, Text: "hello world document", Vector: vec, Metadata: map[string]string{}},
	})

	results, err := s.SearchSingle(ctx, "col-a", "hello", vec, nil, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.Equal(t, "col-a", results[0].CollectionID)
}

func TestSearcher_CrossCollectionRRF(t *testing.T) {
	tests := []struct {
		name    string
		input   []SearchResult
		topK    int
		wantLen int
	}{
		{
			name:    "empty input",
			input:   nil,
			topK:    5,
			wantLen: 0,
		},
		{
			name: "single result",
			input: []SearchResult{
				{ScoredResult: index.ScoredResult{ID: 1, FusedScore: 0.9}, CollectionID: "a"},
			},
			topK:    5,
			wantLen: 1,
		},
		{
			name: "topK truncation",
			input: []SearchResult{
				{ScoredResult: index.ScoredResult{ID: 1, FusedScore: 0.9}, CollectionID: "a"},
				{ScoredResult: index.ScoredResult{ID: 2, FusedScore: 0.8}, CollectionID: "b"},
				{ScoredResult: index.ScoredResult{ID: 3, FusedScore: 0.7}, CollectionID: "a"},
			},
			topK:    2,
			wantLen: 2,
		},
		{
			name: "results sorted by fused score desc",
			input: []SearchResult{
				{ScoredResult: index.ScoredResult{ID: 3, FusedScore: 0.3}, CollectionID: "a"},
				{ScoredResult: index.ScoredResult{ID: 1, FusedScore: 0.9}, CollectionID: "b"},
				{ScoredResult: index.ScoredResult{ID: 2, FusedScore: 0.5}, CollectionID: "a"},
			},
			topK:    10,
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.input == nil {
				return
			}
			result := crossCollectionRRF(tt.input, tt.topK)
			assert.Len(t, result, tt.wantLen)

			// Verify descending order
			for i := 1; i < len(result); i++ {
				assert.GreaterOrEqual(t, result[i-1].FusedScore, result[i].FusedScore)
			}
		})
	}
}

func TestSearcher_GetEmbedder(t *testing.T) {
	s, m := setupSearcher(t, 1)
	t.Cleanup(func() { m.Close() })

	// Empty string should return _default embedder
	emb, err := s.GetEmbedder("")
	require.NoError(t, err)
	assert.NotNil(t, emb)

	// Specific collection
	emb, err = s.GetEmbedder("col-a")
	require.NoError(t, err)
	assert.NotNil(t, emb)

	// Nonexistent
	_, err = s.GetEmbedder("nonexistent")
	assert.Error(t, err)
}
