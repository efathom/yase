package shard

import (
	"context"
	"fmt"
	"sync"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
)

// SearchServer implements the ShardSearchService gRPC interface.
// Each cluster node hosts one SearchServer that serves its local shards.
type SearchServer struct {
	ingestionv1.UnimplementedShardSearchServiceServer
	mu      sync.RWMutex
	engines map[uint32]*index.HybridEngine // shardID → engine
}

// NewSearchServer creates a shard search gRPC server.
func NewSearchServer() *SearchServer {
	return &SearchServer{
		engines: make(map[uint32]*index.HybridEngine),
	}
}

// RegisterEngine adds a shard engine to this server.
func (s *SearchServer) RegisterEngine(shardID uint32, engine *index.HybridEngine) {
	s.mu.Lock()
	s.engines[shardID] = engine
	s.mu.Unlock()
}

// RemoveEngine removes a shard engine from this server.
func (s *SearchServer) RemoveEngine(shardID uint32) {
	s.mu.Lock()
	delete(s.engines, shardID)
	s.mu.Unlock()
}

// ShardIDs returns the IDs of all locally hosted shards.
func (s *SearchServer) ShardIDs() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]uint32, 0, len(s.engines))
	for id := range s.engines {
		ids = append(ids, id)
	}
	return ids
}

// SearchShard executes a hybrid search on a single local shard.
func (s *SearchServer) SearchShard(ctx context.Context, req *ingestionv1.ShardSearchRequest) (*ingestionv1.ShardSearchResponse, error) {
	s.mu.RLock()
	engine, ok := s.engines[req.ShardId]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("shard %d not hosted on this node", req.ShardId)
	}

	topK := int(req.TopK)
	if topK <= 0 {
		topK = 10
	}

	filters := auth.InjectTenantFilter(auth.FromContext(ctx), req.Filters)

	scored, err := engine.HybridSearch(ctx, req.Query, req.QueryVector, filters, topK)
	if err != nil {
		return nil, fmt.Errorf("shard %d search: %w", req.ShardId, err)
	}

	results := make([]*ingestionv1.ScoredResult, len(scored))
	for i, r := range scored {
		results[i] = &ingestionv1.ScoredResult{
			Id:            r.ID,
			FusedScore:    r.FusedScore,
			Bm25Score:     r.BM25Score,
			SemanticScore: r.SemanticScore,
		}
	}

	return &ingestionv1.ShardSearchResponse{
		Results: results,
		ShardId: req.ShardId,
	}, nil
}
