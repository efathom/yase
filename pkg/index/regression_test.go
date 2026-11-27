package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMemEngine(t *testing.T) *HybridEngine {
	t.Helper()
	he, err := NewHybridEngine(t.TempDir(), 256<<20, persistDim, 5)
	require.NoError(t, err)
	t.Cleanup(func() { _ = he.Close() })
	return he
}

// bulkIngest writes n documents in batches; doc i has text "doc i tokI" and
// tenant "t<i%tenants>".
func bulkIngest(t *testing.T, he *HybridEngine, n, tenants int) {
	t.Helper()
	ctx := context.Background()
	const batch = 500
	for start := 0; start < n; start += batch {
		var docs []Document
		for i := start; i < start+batch && i < n; i++ {
			text := fmt.Sprintf("doc %d tok%d", i, i)
			docs = append(docs, Document{ID: uint32(i), Text: text, Vector: deterministicVec(text),
				Metadata: map[string]string{"_tenant": fmt.Sprintf("t%d", i%tenants)}})
		}
		require.NoError(t, he.IngestBatch(ctx, nil, docs))
	}
}

func contains(res []ScoredResult, id uint32) bool {
	for _, r := range res {
		if r.ID == id {
			return true
		}
	}
	return false
}

// Regression: the pre-filter used a capped match-all query, so documents past
// the cap could never be returned.
func TestEveryDocumentIsReachable(t *testing.T) {
	he := newMemEngine(t)
	const n = 6000
	bulkIngest(t, he, n, 1)

	for _, id := range []uint32{0, 1, n / 2, n - 3, n - 2, n - 1} {
		text := fmt.Sprintf("doc %d tok%d", id, id)
		res, err := he.HybridSearch(context.Background(), text, deterministicVec(text), nil, 10)
		require.NoError(t, err)
		assert.True(t, contains(res, id), "doc %d must be findable by its own text+vector", id)
	}
}

// Regression: BM25 only re-ranked vector candidates, so a strong keyword
// match outside the nearest clusters was never returned.
func TestKeywordMatchFarFromQueryVectorIsReturned(t *testing.T) {
	he := newMemEngine(t)
	bulkIngest(t, he, 3000, 1)

	needle := uint32(1234)
	// Query vector of an unrelated document; text names only the needle.
	res, err := he.HybridSearch(context.Background(), "tok1234", deterministicVec("doc 7 tok7"), nil, 10)
	require.NoError(t, err)
	assert.True(t, contains(res, needle), "exact keyword hit must be a candidate regardless of vector distance")
}

func TestFilteredSearchPaths(t *testing.T) {
	tests := []struct {
		name      string
		threshold int // FlatSearchThreshold
	}{
		{"restrictive filter uses exact flat scan", 1 << 30},
		{"broad filter uses ANN with post-check", 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := FlatSearchThreshold
			FlatSearchThreshold = tt.threshold
			defer func() { FlatSearchThreshold = old }()

			he := newMemEngine(t)
			bulkIngest(t, he, 2000, 4)
			filters := map[string]string{"_tenant": "t2"}

			needle := uint32(1502) // 1502 % 4 == 2
			text := fmt.Sprintf("doc %d tok%d", needle, needle)
			res, err := he.HybridSearch(context.Background(), text, deterministicVec(text), filters, 20)
			require.NoError(t, err)
			require.NotEmpty(t, res)
			assert.True(t, contains(res, needle))
			for _, r := range res {
				assert.Equal(t, uint32(2), r.ID%4, "result %d violates the tenant filter", r.ID)
			}
		})
	}
}

func TestFilterWithNoMatchesReturnsNothing(t *testing.T) {
	he := newMemEngine(t)
	bulkIngest(t, he, 100, 2)
	res, err := he.HybridSearch(context.Background(), "doc", deterministicVec("x"), map[string]string{"_tenant": "nobody"}, 10)
	require.NoError(t, err)
	assert.Empty(t, res)
}

func TestQueryDimensionMismatchIsRejected(t *testing.T) {
	he := newMemEngine(t)
	bulkIngest(t, he, 10, 1)
	_, err := he.HybridSearch(context.Background(), "doc", make([]float32, persistDim+1), nil, 5)
	assert.ErrorIs(t, err, ErrDimensionMismatch)
}

func TestReservedMetadataCannotOverrideIdentity(t *testing.T) {
	he := newMemEngine(t)
	ctx := context.Background()
	require.NoError(t, he.Ingest(ctx, Document{ID: 1, Text: "real body", Vector: deterministicVec("a"),
		Metadata: map[string]string{"_id": "999", "content": "spoofed"}}))
	docs, err := he.BlugeStore.GetDocumentsByIDs(ctx, []uint32{1})
	require.NoError(t, err)
	assert.Equal(t, "real body", docs[1].Content)
	none, err := he.BlugeStore.GetDocumentsByIDs(ctx, []uint32{999})
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestIngestBatchReplacesAtomically(t *testing.T) {
	he := newMemEngine(t)
	ctx := context.Background()
	url := map[string]string{"url": "https://x/doc"}
	mk := func(id uint32, text string) Document {
		return Document{ID: id, Text: text, Vector: deterministicVec(text), Metadata: map[string]string{"url": "https://x/doc"}}
	}
	require.NoError(t, he.IngestBatch(ctx, url, []Document{mk(1, "v1 a"), mk(2, "v1 b"), mk(3, "v1 c")}))
	require.NoError(t, he.IngestBatch(ctx, url, []Document{mk(4, "v2 only")}))

	ids, _, err := he.BlugeStore.FilteredIDs(ctx, url, -1)
	require.NoError(t, err)
	assert.ElementsMatch(t, []uint32{4}, ids, "old chunks replaced, not accumulated")
	assert.Equal(t, 1, he.DocCount())
	assert.Positive(t, he.DeadBytes())
}

func TestDeleteRemovesFromVectorIndexInConstantTime(t *testing.T) {
	he := newMemEngine(t)
	bulkIngest(t, he, 500, 1)
	n, err := he.Delete(context.Background(), []uint32{7, 7, 8, 100000}, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "duplicates and unknown IDs are not counted")
	_, ok := he.VectorOffset(7)
	assert.False(t, ok)
	text := "doc 8 tok8"
	res, err := he.HybridSearch(context.Background(), text, deterministicVec(text), nil, 10)
	require.NoError(t, err)
	assert.False(t, contains(res, 8))
}

func TestDeleteByFilterRequiresFilter(t *testing.T) {
	he := newMemEngine(t)
	bulkIngest(t, he, 10, 1)
	_, err := he.DeleteByFilter(context.Background(), nil)
	assert.Error(t, err)
	assert.Equal(t, 10, he.DocCount())
}

func TestNextIDSkipsExplicitIDs(t *testing.T) {
	he := newMemEngine(t)
	require.NoError(t, he.Ingest(context.Background(), Document{ID: 41, Text: "x", Vector: deterministicVec("x")}))
	id, err := he.NextID()
	require.NoError(t, err)
	assert.Equal(t, uint32(42), id)
}
