// Package indexsvc implements the gRPC IndexService and CollectionService
// that own the hybrid engines. The indexer process hosts these services; the
// gateway calls them remotely.
package indexsvc

import (
	"context"
	"errors"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the IndexServiceServer gRPC interface.
type Server struct {
	ingestionv1.UnimplementedIndexServiceServer
	engine   *index.HybridEngine // legacy single-engine mode
	mgr      *collection.Manager
	searcher *collection.Searcher
}

// NewServer creates an IndexService over a single engine (no collections).
func NewServer(engine *index.HybridEngine) *Server {
	return &Server{engine: engine}
}

// NewCollectionServer creates a collection-aware IndexService.
func NewCollectionServer(mgr *collection.Manager) *Server {
	return &Server{mgr: mgr, searcher: collection.NewSearcher(mgr)}
}

// effectiveTenant returns the tenant a request acts for. A tenant-scoped
// caller is always confined to its own tenant; an unscoped caller (a service
// identity such as the gateway, or auth disabled) acts for the tenant it
// names on the request.
func effectiveTenant(ctx context.Context, claimed string) string {
	if ac := auth.FromContext(ctx); ac != nil && ac.TenantID != "" {
		return ac.TenantID
	}
	return claimed
}

func tenantFilters(tenant string, filters map[string]string) map[string]string {
	out := make(map[string]string, len(filters)+1)
	for k, v := range filters {
		out[k] = v
	}
	if tenant != "" {
		out[auth.TenantField] = tenant
	}
	return out
}

// engineFor resolves a collection's engine. The shared _default collection is
// open to every tenant (documents in it are scoped by the tenant filter);
// other collections require ownership.
func (s *Server) engineFor(ctx context.Context, tenant, collectionID string) (*index.HybridEngine, error) {
	if s.mgr == nil {
		if collectionID != "" && collectionID != collection.DefaultCollectionID {
			return nil, status.Error(codes.NotFound, "collections are not enabled on this server")
		}
		return s.engine, nil
	}
	if collectionID == "" {
		collectionID = collection.DefaultCollectionID
	}
	if collectionID != collection.DefaultCollectionID {
		if _, err := s.mgr.Get(ctx, tenant, collectionID); err != nil {
			return nil, toStatus(err)
		}
	}
	eng, err := s.mgr.GetEngine(collectionID)
	if err != nil {
		return nil, toStatus(err)
	}
	return eng, nil
}

// toStatus maps domain errors to gRPC status codes.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, collection.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, collection.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, collection.ErrDefaultDelete):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, collection.ErrInvalidConfig), errors.Is(err, index.ErrDimensionMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, index.ErrEngineClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// Ingest adds a document, tagging it with the effective tenant. Replacing an
// existing document ID owned by another tenant is refused.
func (s *Server) Ingest(ctx context.Context, req *ingestionv1.IngestRequest) (*ingestionv1.IngestResponse, error) {
	tenant := effectiveTenant(ctx, "")
	eng, err := s.engineFor(ctx, tenant, req.CollectionId)
	if err != nil {
		return nil, err
	}
	if tenant != "" {
		existing, err := eng.BlugeStore.GetDocumentsByIDs(ctx, []uint32{req.DocId})
		if err != nil {
			return nil, toStatus(err)
		}
		if d, ok := existing[req.DocId]; ok && d.Metadata[auth.TenantField] != tenant {
			return nil, status.Error(codes.PermissionDenied, "document id belongs to another tenant")
		}
	}
	doc := index.Document{
		ID:     req.DocId,
		Text:   req.Text,
		Vector: req.Vector,
		// Overwrites any caller-supplied "_tenant" with the authenticated one.
		Metadata: auth.InjectTenantMetadata(auth.FromContext(ctx), req.Metadata),
	}
	if err := eng.Ingest(ctx, doc); err != nil {
		return &ingestionv1.IngestResponse{Ok: false}, toStatus(err)
	}
	return &ingestionv1.IngestResponse{Ok: true}, nil
}

// Search executes a hybrid BM25 + semantic search on one collection, or on
// several when cross_collection is set.
func (s *Server) Search(ctx context.Context, req *ingestionv1.SearchRequest) (*ingestionv1.SearchResponse, error) {
	topK := int(req.TopK)
	if topK <= 0 {
		topK = 10
	}
	tenant := effectiveTenant(ctx, req.CallerTenant)
	filters := tenantFilters(tenant, req.Filters)

	cross := req.CrossCollection || len(req.CollectionIds) > 0
	if s.mgr != nil && (cross || (req.CollectionId != "" && req.CollectionId != collection.DefaultCollectionID)) {
		ids := req.CollectionIds
		if !cross {
			ids = []string{req.CollectionId}
		}
		qe := collection.NewQueryEmbedder(req.Query, req.QueryVector)
		res, err := s.searcher.SearchEmbedded(ctx, tenant, ids, req.Query, qe, filters, topK)
		if err != nil {
			return nil, toStatus(err)
		}
		out := make([]*ingestionv1.ScoredResult, len(res))
		for i, r := range res {
			out[i] = &ingestionv1.ScoredResult{Id: r.ID, FusedScore: r.FusedScore, Bm25Score: r.BM25Score,
				SemanticScore: r.SemanticScore, CollectionId: r.CollectionID}
		}
		return &ingestionv1.SearchResponse{Results: out}, nil
	}

	eng, err := s.engineFor(ctx, tenant, collection.DefaultCollectionID)
	if err != nil {
		return nil, err
	}
	vec := req.QueryVector
	if len(vec) == 0 {
		if s.mgr == nil {
			return nil, status.Error(codes.InvalidArgument, "query_vector is required")
		}
		emb, err := s.mgr.GetEmbedder(collection.DefaultCollectionID)
		if err != nil {
			return nil, toStatus(err)
		}
		if vec, err = emb.Embed(ctx, req.Query); err != nil {
			return nil, status.Error(codes.Unavailable, "embed query: "+err.Error())
		}
	}
	scored, err := eng.HybridSearch(ctx, req.Query, vec, filters, topK)
	if err != nil {
		return nil, toStatus(err)
	}
	results := make([]*ingestionv1.ScoredResult, len(scored))
	for i, r := range scored {
		results[i] = &ingestionv1.ScoredResult{
			Id:            r.ID,
			FusedScore:    r.FusedScore,
			Bm25Score:     r.BM25Score,
			SemanticScore: r.SemanticScore,
			CollectionId:  collection.DefaultCollectionID,
		}
	}
	return &ingestionv1.SearchResponse{Results: results}, nil
}

// GetDocuments returns stored content for documents the tenant may read.
func (s *Server) GetDocuments(ctx context.Context, req *ingestionv1.GetDocumentsRequest) (*ingestionv1.GetDocumentsResponse, error) {
	tenant := effectiveTenant(ctx, req.CallerTenant)
	eng, err := s.engineFor(ctx, tenant, req.CollectionId)
	if err != nil {
		return nil, err
	}
	hits, err := eng.BlugeStore.GetDocumentsByIDs(ctx, req.Ids)
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*ingestionv1.Document, 0, len(hits))
	for id, h := range hits {
		if tenant != "" && h.Metadata[auth.TenantField] != tenant {
			continue // other tenants' documents are invisible
		}
		out = append(out, &ingestionv1.Document{Id: id, Content: h.Content, Metadata: h.Metadata})
	}
	return &ingestionv1.GetDocumentsResponse{Documents: out}, nil
}

// DeleteDocuments removes documents by ID or filter, always constrained to
// the effective tenant.
func (s *Server) DeleteDocuments(ctx context.Context, req *ingestionv1.DeleteDocumentsRequest) (*ingestionv1.DeleteDocumentsResponse, error) {
	if ac := auth.FromContext(ctx); ac != nil && !ac.HasScope("delete") {
		return nil, status.Error(codes.PermissionDenied, "delete scope required")
	}
	tenant := effectiveTenant(ctx, req.CallerTenant)
	eng, err := s.engineFor(ctx, tenant, req.CollectionId)
	if err != nil {
		return nil, err
	}
	var n int
	if len(req.Ids) > 0 {
		var filters map[string]string
		if tenant != "" || len(req.Filters) > 0 {
			filters = tenantFilters(tenant, req.Filters)
		}
		n, err = eng.Delete(ctx, req.Ids, filters)
	} else {
		if len(req.Filters) == 0 {
			return nil, status.Error(codes.InvalidArgument, "ids or filters required")
		}
		n, err = eng.DeleteByFilter(ctx, tenantFilters(tenant, req.Filters))
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &ingestionv1.DeleteDocumentsResponse{Deleted: int64(n)}, nil
}

// Stats returns index health information for a collection.
func (s *Server) Stats(ctx context.Context, req *ingestionv1.StatsRequest) (*ingestionv1.StatsResponse, error) {
	eng, err := s.engineFor(ctx, effectiveTenant(ctx, ""), req.GetCollectionId())
	if err != nil {
		return nil, err
	}
	centroids, leaves := eng.InvertedFileStats()
	return &ingestionv1.StatsResponse{
		HnswNodes:      int64(eng.NodeCount()),
		ArenaUsedBytes: int64(eng.ArenaUsedBytes()),
		Centroids:      int64(centroids),
		Leaves:         int64(leaves),
		Generation:     eng.Generation(),
		Documents:      int64(eng.DocCount()),
	}, nil
}
