package reranker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// teiServing returns a TEI stub that replies with the given raw JSON body.
func teiServing(t *testing.T, body string) *TEIReranker {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewTEIReranker(srv.URL, "test-model", WithTEIHTTPClient(srv.Client()))
}

// M-09: an index past the end of the input is used directly as a slice index
// by the search pipeline, so it must be rejected here rather than panicking
// two layers up — and in a gRPC handler that panic kills the process.
func TestRerankRejectsIndexBeyondInput(t *testing.T) {
	r := teiServing(t, `[{"index":7,"score":0.9},{"index":0,"score":0.1}]`)

	_, err := r.Rerank(context.Background(), "q", []string{"a", "b"})

	require.Error(t, err, "an out-of-range index must be an error, not a panic")
	assert.Contains(t, err.Error(), "index")
}

func TestRerankRejectsNegativeIndex(t *testing.T) {
	r := teiServing(t, `[{"index":-1,"score":0.9},{"index":0,"score":0.1}]`)

	_, err := r.Rerank(context.Background(), "q", []string{"a", "b"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "index")
}

// M-09: a short response silently truncated the result set, dropping documents
// the caller had already retrieved.
func TestRerankRejectsShortResponse(t *testing.T) {
	r := teiServing(t, `[{"index":0,"score":0.9}]`)

	_, err := r.Rerank(context.Background(), "q", []string{"a", "b", "c"})

	require.Error(t, err, "a response covering fewer documents than were sent must be rejected")
}

// M-09: a duplicated index would drop one document and double another.
func TestRerankRejectsDuplicateIndex(t *testing.T) {
	r := teiServing(t, `[{"index":0,"score":0.9},{"index":0,"score":0.5}]`)

	_, err := r.Rerank(context.Background(), "q", []string{"a", "b"})

	require.Error(t, err, "each input document must appear exactly once")
}

// A well-formed response is still passed through untouched.
func TestRerankAcceptsValidPermutation(t *testing.T) {
	r := teiServing(t, `[{"index":1,"score":0.9},{"index":0,"score":0.2}]`)

	got, err := r.Rerank(context.Background(), "q", []string{"a", "b"})

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, 1, got[0].Index)
	assert.Equal(t, 0, got[1].Index)
}
