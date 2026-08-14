package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/efathom/yase/pkg/index"
)

// ── Mock implementations ──

type mockResolver struct {
	aliases map[string]struct {
		shardIDs []uint32
		filters  map[string]string
	}
}

func (m *mockResolver) ResolveAlias(name string) ([]uint32, map[string]string, error) {
	a, ok := m.aliases[name]
	if !ok {
		return nil, nil, fmt.Errorf("alias %q not found", name)
	}
	return a.shardIDs, a.filters, nil
}

type mockSearcher struct {
	results map[uint32][]index.ScoredResult // shardID → results
	delay   map[uint32]time.Duration        // shardID → artificial delay
	err     map[uint32]error                // shardID → error
}

func (m *mockSearcher) SearchShard(ctx context.Context, shardID uint32, query string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	if d, ok := m.delay[shardID]; ok {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if e, ok := m.err[shardID]; ok {
		return nil, e
	}
	results := m.results[shardID]
	if topK > 0 && len(results) > topK {
		results = results[:topK]
	}
	return results, nil
}

// ── Tests ──

func TestCoordinatorBasicSearch(t *testing.T) {
	resolver := &mockResolver{
		aliases: map[string]struct {
			shardIDs []uint32
			filters  map[string]string
		}{
			"prod": {shardIDs: []uint32{1, 2}},
		},
	}
	searcher := &mockSearcher{
		results: map[uint32][]index.ScoredResult{
			1: {{ID: 10, BM25Score: 5}, {ID: 11, BM25Score: 3}},
			2: {{ID: 20, BM25Score: 8}, {ID: 21, BM25Score: 2}},
		},
	}

	coord := &Coordinator{
		Resolver: resolver,
		Searcher: searcher,
		Timeout:  5 * time.Second,
	}

	resp, err := coord.Search(context.Background(), &SearchRequest{
		Alias: "prod",
		Query: "test",
		TopK:  3,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.ShardCount != 2 {
		t.Errorf("shard count: got %d, want 2", resp.ShardCount)
	}
	if len(resp.Results) != 3 {
		t.Errorf("results: got %d, want 3", len(resp.Results))
	}

	t.Logf("Results:")
	for i, r := range resp.Results {
		t.Logf("  #%d: ID=%d, fused=%.6f", i+1, r.ID, r.FusedScore)
	}
}

func TestCoordinatorPartialFailure(t *testing.T) {
	resolver := &mockResolver{
		aliases: map[string]struct {
			shardIDs []uint32
			filters  map[string]string
		}{
			"prod": {shardIDs: []uint32{1, 2, 3}},
		},
	}
	searcher := &mockSearcher{
		results: map[uint32][]index.ScoredResult{
			1: {{ID: 10, BM25Score: 5}},
			3: {{ID: 30, BM25Score: 8}},
		},
		err: map[uint32]error{
			2: fmt.Errorf("shard offline"),
		},
	}

	coord := &Coordinator{
		Resolver: resolver,
		Searcher: searcher,
		Timeout:  5 * time.Second,
	}

	resp, err := coord.Search(context.Background(), &SearchRequest{
		Alias: "prod",
		Query: "test",
		TopK:  10,
	})
	if err != nil {
		t.Fatalf("Search should succeed with partial results: %v", err)
	}
	if resp.ShardCount != 2 {
		t.Errorf("shard count: got %d, want 2", resp.ShardCount)
	}
	if len(resp.Errors) != 1 {
		t.Errorf("errors: got %d, want 1", len(resp.Errors))
	}
	if len(resp.Results) != 2 {
		t.Errorf("results: got %d, want 2", len(resp.Results))
	}
}

func TestCoordinatorAllShardsFailure(t *testing.T) {
	resolver := &mockResolver{
		aliases: map[string]struct {
			shardIDs []uint32
			filters  map[string]string
		}{
			"prod": {shardIDs: []uint32{1, 2}},
		},
	}
	searcher := &mockSearcher{
		err: map[uint32]error{
			1: fmt.Errorf("offline"),
			2: fmt.Errorf("timeout"),
		},
	}

	coord := &Coordinator{
		Resolver: resolver,
		Searcher: searcher,
		Timeout:  5 * time.Second,
	}

	_, err := coord.Search(context.Background(), &SearchRequest{
		Alias: "prod",
		Query: "test",
		TopK:  10,
	})
	if err == nil {
		t.Fatal("expected error when all shards fail")
	}
	t.Logf("All-fail error (expected): %v", err)
}

func TestCoordinatorTimeout(t *testing.T) {
	resolver := &mockResolver{
		aliases: map[string]struct {
			shardIDs []uint32
			filters  map[string]string
		}{
			"prod": {shardIDs: []uint32{1, 2}},
		},
	}
	searcher := &mockSearcher{
		results: map[uint32][]index.ScoredResult{
			1: {{ID: 10, BM25Score: 5}},
		},
		delay: map[uint32]time.Duration{
			2: 5 * time.Second, // slow shard
		},
	}

	coord := &Coordinator{
		Resolver: resolver,
		Searcher: searcher,
		Timeout:  200 * time.Millisecond, // tight SLA
	}

	start := time.Now()
	resp, err := coord.Search(context.Background(), &SearchRequest{
		Alias: "prod",
		Query: "test",
		TopK:  10,
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Search should succeed with partial results: %v", err)
	}
	if resp.ShardCount != 1 {
		t.Errorf("shard count: got %d, want 1 (slow shard timed out)", resp.ShardCount)
	}
	if elapsed > 1*time.Second {
		t.Errorf("should complete within ~200ms, took %v", elapsed)
	}
	t.Logf("Completed in %v with %d results from %d shards (%d errors)",
		elapsed, len(resp.Results), resp.ShardCount, len(resp.Errors))
}

func TestCoordinatorMissingAlias(t *testing.T) {
	resolver := &mockResolver{
		aliases: map[string]struct {
			shardIDs []uint32
			filters  map[string]string
		}{},
	}
	coord := &Coordinator{
		Resolver: resolver,
		Searcher: &mockSearcher{},
	}

	_, err := coord.Search(context.Background(), &SearchRequest{
		Alias: "nonexistent",
		Query: "test",
	})
	if err == nil {
		t.Fatal("expected error for missing alias")
	}
}

func TestCoordinatorFilterMerge(t *testing.T) {
	resolver := &mockResolver{
		aliases: map[string]struct {
			shardIDs []uint32
			filters  map[string]string
		}{
			"tenant-a": {
				shardIDs: []uint32{1},
				filters:  map[string]string{"tenant": "a", "env": "prod"},
			},
		},
	}

	var capturedFilters map[string]string
	searcher := &mockSearcher{
		results: map[uint32][]index.ScoredResult{1: {{ID: 1}}},
	}
	// Wrap to capture filters
	wrappedSearcher := &filterCapture{inner: searcher, captured: &capturedFilters}

	coord := &Coordinator{
		Resolver: resolver,
		Searcher: wrappedSearcher,
		Timeout:  5 * time.Second,
	}

	coord.Search(context.Background(), &SearchRequest{
		Alias:   "tenant-a",
		Query:   "test",
		TopK:    10,
		Filters: map[string]string{"env": "staging"}, // overrides alias "env"
	})

	if capturedFilters["tenant"] != "a" {
		t.Errorf("alias filter 'tenant' should be 'a', got %q", capturedFilters["tenant"])
	}
	if capturedFilters["env"] != "staging" {
		t.Errorf("user filter should override: got %q, want 'staging'", capturedFilters["env"])
	}
}

// filterCapture wraps a ShardSearcher to capture the filters passed.
type filterCapture struct {
	inner    ShardSearcher
	captured *map[string]string
}

func (f *filterCapture) SearchShard(ctx context.Context, shardID uint32, query string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	*f.captured = filters
	return f.inner.SearchShard(ctx, shardID, query, queryVec, filters, topK)
}
