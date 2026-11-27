package indexsvc

import (
	"context"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CollectionServer implements the CollectionService over a Manager so a
// remote gateway can manage collections owned by the indexer.
type CollectionServer struct {
	ingestionv1.UnimplementedCollectionServiceServer
	mgr      *collection.Manager
	searcher *Server
}

// NewCollectionService creates the CollectionService server.
func NewCollectionService(mgr *collection.Manager) *CollectionServer {
	return &CollectionServer{mgr: mgr, searcher: NewCollectionServer(mgr)}
}

// requireAdmin enforces the admin scope for mutations when auth is on.
func requireAdmin(ctx context.Context) error {
	if ac := auth.FromContext(ctx); ac != nil && !ac.HasScope("admin") {
		return status.Error(codes.PermissionDenied, "admin scope required")
	}
	return nil
}

func resp(c *collection.Collection, err error) (*ingestionv1.CollectionResponse, error) {
	if err != nil {
		return nil, toStatus(err)
	}
	return &ingestionv1.CollectionResponse{Collection: collection.ToProto(c)}, nil
}

func (s *CollectionServer) CreateCollection(ctx context.Context, req *ingestionv1.CreateCollectionRequest) (*ingestionv1.CollectionResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	return resp(s.mgr.Create(ctx, effectiveTenant(ctx, req.TenantId), req.Id, req.Name, collection.ConfigFromProto(req.Config)))
}

func (s *CollectionServer) GetCollection(ctx context.Context, req *ingestionv1.GetCollectionRequest) (*ingestionv1.CollectionResponse, error) {
	return resp(s.mgr.Get(ctx, effectiveTenant(ctx, req.CallerTenant), req.Id))
}

func (s *CollectionServer) ListCollections(ctx context.Context, req *ingestionv1.ListCollectionsRequest) (*ingestionv1.ListCollectionsResponse, error) {
	cols, err := s.mgr.List(ctx, effectiveTenant(ctx, req.TenantId))
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*ingestionv1.Collection, len(cols))
	for i, c := range cols {
		out[i] = collection.ToProto(c)
	}
	return &ingestionv1.ListCollectionsResponse{Collections: out}, nil
}

func (s *CollectionServer) UpdateCollection(ctx context.Context, req *ingestionv1.UpdateCollectionRequest) (*ingestionv1.CollectionResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	return resp(s.mgr.Update(ctx, effectiveTenant(ctx, req.CallerTenant), req.Id, req.Name, req.Description))
}

func (s *CollectionServer) DeleteCollection(ctx context.Context, req *ingestionv1.DeleteCollectionRequest) (*ingestionv1.DeleteCollectionResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := s.mgr.Delete(ctx, effectiveTenant(ctx, req.CallerTenant), req.Id); err != nil {
		return nil, toStatus(err)
	}
	return &ingestionv1.DeleteCollectionResponse{Ok: true}, nil
}

func (s *CollectionServer) BindConnector(ctx context.Context, req *ingestionv1.BindConnectorRequest) (*ingestionv1.CollectionResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	tenant := effectiveTenant(ctx, req.CallerTenant)
	if err := s.mgr.BindConnector(ctx, tenant, req.Id, req.ConnectorJobId); err != nil {
		return nil, toStatus(err)
	}
	return resp(s.mgr.Get(ctx, tenant, req.Id))
}

func (s *CollectionServer) UnbindConnector(ctx context.Context, req *ingestionv1.BindConnectorRequest) (*ingestionv1.CollectionResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	tenant := effectiveTenant(ctx, req.CallerTenant)
	if err := s.mgr.UnbindConnector(ctx, tenant, req.Id, req.ConnectorJobId); err != nil {
		return nil, toStatus(err)
	}
	return resp(s.mgr.Get(ctx, tenant, req.Id))
}

func (s *CollectionServer) CollectionStats(ctx context.Context, req *ingestionv1.GetCollectionRequest) (*ingestionv1.CollectionStatsResponse, error) {
	st, err := s.mgr.Stats(ctx, effectiveTenant(ctx, req.CallerTenant), req.Id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &ingestionv1.CollectionStatsResponse{
		Id: st.ID, HnswNodes: int64(st.HNSWNodes), Centroids: int64(st.Centroids), Leaves: int64(st.Leaves),
		ArenaUsedBytes: int64(st.ArenaUsedBytes), Documents: int64(st.Documents),
	}, nil
}

// SearchCollection searches one or more collections (the indexer embeds the
// query per collection when no vector is given).
func (s *CollectionServer) SearchCollection(ctx context.Context, req *ingestionv1.CollectionSearchRequest) (*ingestionv1.CollectionSearchResponse, error) {
	r, err := s.searcher.Search(ctx, &ingestionv1.SearchRequest{
		Query: req.Query, QueryVector: req.QueryVector, Filters: req.Filters, TopK: req.TopK,
		CollectionIds: req.CollectionIds, CrossCollection: true,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*ingestionv1.CollectionScoredResult, len(r.Results))
	for i, x := range r.Results {
		out[i] = &ingestionv1.CollectionScoredResult{Id: x.Id, FusedScore: x.FusedScore, Bm25Score: x.Bm25Score,
			SemanticScore: x.SemanticScore, CollectionId: x.CollectionId}
	}
	return &ingestionv1.CollectionSearchResponse{Results: out}, nil
}
