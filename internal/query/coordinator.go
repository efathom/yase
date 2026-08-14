package query

import (
	"context"
	"fmt"
	"time"

	"github.com/efathom/yase/pkg/index"
)

// SearchRequest represents a distributed search query.
type SearchRequest struct {
	Alias       string            // index alias to search (resolved to shards)
	Query       string            // text query for BM25
	QueryVector []float32         // embedding vector for semantic search
	Filters     map[string]string // user-supplied metadata filters
	TopK        int               // number of results to return
}

// SearchResponse holds the fused results from a distributed search.
type SearchResponse struct {
	Results    []index.ScoredResult
	ShardCount int // number of shards that responded
	Errors     []error
}

// ShardSearcher is the interface for executing a search on a single shard.
// Implementations include local (in-process) and remote (gRPC) searchers.
type ShardSearcher interface {
	SearchShard(ctx context.Context, shardID uint32, query string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error)
}

// AliasResolver resolves an alias name to shard IDs and embedded filters.
type AliasResolver interface {
	ResolveAlias(name string) (shardIDs []uint32, filters map[string]string, err error)
}

// Coordinator fans out queries to multiple shards and merges results.
// It is stateless and can run on any gateway node.
type Coordinator struct {
	Resolver AliasResolver
	Searcher ShardSearcher
	Timeout  time.Duration // per-query SLA (default 500ms)
}

// Search resolves the alias, scatters to all shards, gathers and fuses.
func (c *Coordinator) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	// 1. Resolve alias to shard list + embedded filters
	shardIDs, aliasFilters, err := c.Resolver.ResolveAlias(req.Alias)
	if err != nil {
		return nil, fmt.Errorf("resolve alias: %w", err)
	}
	if len(shardIDs) == 0 {
		return &SearchResponse{}, nil
	}

	// 2. Merge embedded alias filters with query filters
	mergedFilters := mergeFilters(aliasFilters, req.Filters)

	// 3. Scatter: fan-out to all shards with deadline
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 500 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type shardResult struct {
		ShardID uint32
		Results []index.ScoredResult
		Err     error
	}

	resultsCh := make(chan shardResult, len(shardIDs))
	for _, sid := range shardIDs {
		go func(shardID uint32) {
			results, err := c.Searcher.SearchShard(ctx, shardID, req.Query, req.QueryVector, mergedFilters, req.TopK)
			resultsCh <- shardResult{shardID, results, err}
		}(sid)
	}

	// 4. Gather: collect results, tolerate partial failures
	var allResults [][]index.ScoredResult
	var errors []error
	for range shardIDs {
		sr := <-resultsCh
		if sr.Err != nil {
			errors = append(errors, fmt.Errorf("shard %d: %w", sr.ShardID, sr.Err))
			continue
		}
		allResults = append(allResults, sr.Results)
	}

	// Fail if ALL shards failed; partial results are acceptable
	if len(allResults) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("all %d shards failed: %v", len(shardIDs), errors)
	}

	// 5. Merge: distributed RRF across shard-local ranked lists
	fused := MergeRRF(allResults, req.TopK)
	return &SearchResponse{
		Results:    fused,
		ShardCount: len(allResults),
		Errors:     errors,
	}, nil
}

// mergeFilters combines alias-embedded filters with user-supplied filters.
// User filters take precedence.
func mergeFilters(aliasFilters, queryFilters map[string]string) map[string]string {
	if len(aliasFilters) == 0 && len(queryFilters) == 0 {
		return nil
	}
	merged := make(map[string]string)
	for k, v := range aliasFilters {
		merged[k] = v
	}
	for k, v := range queryFilters {
		merged[k] = v // user overrides alias
	}
	return merged
}
