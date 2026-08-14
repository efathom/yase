// Package indexsvc implements the gRPC IndexService that owns the hybrid engine.
// The indexer process hosts this service; the gateway calls it remotely.
package indexsvc

import (
	"context"

	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
)

// Server implements the IndexServiceServer gRPC interface.
type Server struct {
	ingestionv1.UnimplementedIndexServiceServer
	engine *index.HybridEngine
}

// NewServer creates an IndexService gRPC server wrapping the given engine.
func NewServer(engine *index.HybridEngine) *Server {
	return &Server{engine: engine}
}

// Ingest adds a document to the hybrid index.
func (s *Server) Ingest(ctx context.Context, req *ingestionv1.IngestRequest) (*ingestionv1.IngestResponse, error) {
	doc := index.Document{
		ID:       req.DocId,
		Text:     req.Text,
		Vector:   req.Vector,
		Metadata: req.Metadata,
	}
	if err := s.engine.Ingest(ctx, doc); err != nil {
		return &ingestionv1.IngestResponse{Ok: false}, err
	}
	return &ingestionv1.IngestResponse{Ok: true}, nil
}

// Search executes a hybrid BM25 + semantic search with RRF fusion.
func (s *Server) Search(ctx context.Context, req *ingestionv1.SearchRequest) (*ingestionv1.SearchResponse, error) {
	topK := int(req.TopK)
	if topK <= 0 {
		topK = 10
	}

	scored, err := s.engine.HybridSearch(ctx, req.Query, req.QueryVector, req.Filters, topK)
	if err != nil {
		return nil, err
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

	return &ingestionv1.SearchResponse{Results: results}, nil
}

// Stats returns index health information.
func (s *Server) Stats(ctx context.Context, _ *ingestionv1.StatsRequest) (*ingestionv1.StatsResponse, error) {
	centroids, leaves := s.engine.InvertedFileStats()
	return &ingestionv1.StatsResponse{
		HnswNodes:      int64(s.engine.Graph.Len()),
		ArenaUsedBytes: int64(s.engine.Arena.UsedBytes()),
		Centroids:      int64(centroids),
		Leaves:         int64(leaves),
	}, nil
}
