package gateway

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CollectionAPI is the collection management surface the HTTP handlers use.
// *collection.Manager implements it in-process; RemoteCollections implements
// it over gRPC for a gateway running apart from the indexer.
type CollectionAPI interface {
	Create(ctx context.Context, tenantID, id, name string, cfg collection.CollectionConfig) (*collection.Collection, error)
	Get(ctx context.Context, callerTenant, id string) (*collection.Collection, error)
	List(ctx context.Context, tenantID string) ([]*collection.Collection, error)
	Update(ctx context.Context, callerTenant, id, name, description string) (*collection.Collection, error)
	Delete(ctx context.Context, callerTenant, id string) error
	BindConnector(ctx context.Context, callerTenant, collectionID, connectorJobID string) error
	UnbindConnector(ctx context.Context, callerTenant, collectionID, connectorJobID string) error
	Stats(ctx context.Context, callerTenant, id string) (*collection.Stats, error)
}

// CollectionSearchBackend runs searches against named collections. The
// backend embeds the query with each collection's own embedder.
type CollectionSearchBackend interface {
	SearchCollections(ctx context.Context, callerTenant string, ids []string, cross bool,
		query string, filters map[string]string, topK int) ([]collection.SearchResult, error)
}

// LocalCollectionSearch searches in-process collections.
type LocalCollectionSearch struct {
	Searcher *collection.Searcher
}

// SearchCollections implements CollectionSearchBackend.
func (l *LocalCollectionSearch) SearchCollections(ctx context.Context, callerTenant string, ids []string, _ bool,
	query string, filters map[string]string, topK int) ([]collection.SearchResult, error) {
	return l.Searcher.SearchEmbedded(ctx, callerTenant, ids, query, collection.NewQueryEmbedder(query, nil), filters, topK)
}

// tenantOf returns the authenticated tenant of an HTTP request context.
func tenantOf(ctx context.Context) string {
	if ac := auth.FromContext(ctx); ac != nil {
		return ac.TenantID
	}
	return ""
}

// fromStatus maps gRPC errors back to domain errors so handlers keep their
// status-code mapping in remote mode.
func fromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	var sentinel error
	switch st.Code() {
	case codes.NotFound:
		sentinel = collection.ErrNotFound
	case codes.AlreadyExists:
		sentinel = collection.ErrAlreadyExists
	case codes.FailedPrecondition:
		sentinel = collection.ErrDefaultDelete
	case codes.InvalidArgument:
		sentinel = collection.ErrInvalidConfig
	default:
		return err
	}
	return &remoteError{sentinel: sentinel, msg: st.Message()}
}

// remoteError carries the server's message while matching a domain sentinel.
type remoteError struct {
	sentinel error
	msg      string
}

func (e *remoteError) Error() string        { return e.msg }
func (e *remoteError) Is(target error) bool { return target == e.sentinel }

// RemoteCollections implements CollectionAPI via the CollectionService.
type RemoteCollections struct {
	Client ingestionv1.CollectionServiceClient
}

func fromResp(r *ingestionv1.CollectionResponse, err error) (*collection.Collection, error) {
	if err != nil {
		return nil, fromStatus(err)
	}
	return collection.FromProto(r.GetCollection())
}

func (rc *RemoteCollections) Create(ctx context.Context, tenantID, id, name string, cfg collection.CollectionConfig) (*collection.Collection, error) {
	return fromResp(rc.Client.CreateCollection(ctx, &ingestionv1.CreateCollectionRequest{
		Id: id, Name: name, TenantId: tenantID, Config: collection.ConfigToProto(cfg)}))
}

func (rc *RemoteCollections) Get(ctx context.Context, callerTenant, id string) (*collection.Collection, error) {
	return fromResp(rc.Client.GetCollection(ctx, &ingestionv1.GetCollectionRequest{Id: id, CallerTenant: callerTenant}))
}

func (rc *RemoteCollections) List(ctx context.Context, tenantID string) ([]*collection.Collection, error) {
	r, err := rc.Client.ListCollections(ctx, &ingestionv1.ListCollectionsRequest{TenantId: tenantID})
	if err != nil {
		return nil, fromStatus(err)
	}
	out := make([]*collection.Collection, 0, len(r.Collections))
	for _, pc := range r.Collections {
		c, err := collection.FromProto(pc)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func (rc *RemoteCollections) Update(ctx context.Context, callerTenant, id, name, description string) (*collection.Collection, error) {
	return fromResp(rc.Client.UpdateCollection(ctx, &ingestionv1.UpdateCollectionRequest{
		Id: id, Name: name, Description: description, CallerTenant: callerTenant}))
}

func (rc *RemoteCollections) Delete(ctx context.Context, callerTenant, id string) error {
	_, err := rc.Client.DeleteCollection(ctx, &ingestionv1.DeleteCollectionRequest{Id: id, CallerTenant: callerTenant})
	return fromStatus(err)
}

func (rc *RemoteCollections) BindConnector(ctx context.Context, callerTenant, collectionID, connectorJobID string) error {
	_, err := rc.Client.BindConnector(ctx, &ingestionv1.BindConnectorRequest{Id: collectionID, ConnectorJobId: connectorJobID, CallerTenant: callerTenant})
	return fromStatus(err)
}

func (rc *RemoteCollections) UnbindConnector(ctx context.Context, callerTenant, collectionID, connectorJobID string) error {
	_, err := rc.Client.UnbindConnector(ctx, &ingestionv1.BindConnectorRequest{Id: collectionID, ConnectorJobId: connectorJobID, CallerTenant: callerTenant})
	return fromStatus(err)
}

func (rc *RemoteCollections) Stats(ctx context.Context, callerTenant, id string) (*collection.Stats, error) {
	c, err := rc.Get(ctx, callerTenant, id)
	if err != nil {
		return nil, err
	}
	r, err := rc.Client.CollectionStats(ctx, &ingestionv1.GetCollectionRequest{Id: id, CallerTenant: callerTenant})
	if err != nil {
		return nil, fromStatus(err)
	}
	return &collection.Stats{
		ID: c.ID, Name: c.Name, Status: string(c.Status),
		HNSWNodes: int(r.HnswNodes), Centroids: int(r.Centroids), Leaves: int(r.Leaves),
		Documents: int(r.Documents), ArenaUsedBytes: uint64(r.ArenaUsedBytes), Connectors: len(c.Connectors),
	}, nil
}

// RemoteCollectionSearch implements CollectionSearchBackend via IndexService.
type RemoteCollectionSearch struct {
	Client ingestionv1.IndexServiceClient
}

// SearchCollections implements CollectionSearchBackend.
func (r *RemoteCollectionSearch) SearchCollections(ctx context.Context, callerTenant string, ids []string, cross bool,
	query string, filters map[string]string, topK int) ([]collection.SearchResult, error) {
	req := &ingestionv1.SearchRequest{Query: query, Filters: filters, TopK: int32(topK),
		CallerTenant: callerTenant, CollectionIds: ids, CrossCollection: cross}
	resp, err := r.Client.Search(ctx, req)
	if err != nil {
		return nil, fromStatus(err)
	}
	out := make([]collection.SearchResult, len(resp.Results))
	for i, x := range resp.Results {
		out[i] = collection.SearchResult{CollectionID: x.CollectionId, ScoredResult: index.ScoredResult{
			ID: x.Id, FusedScore: x.FusedScore, BM25Score: x.Bm25Score, SemanticScore: x.SemanticScore}}
	}
	return out, nil
}

// RemoteSearcher calls the IndexService gRPC server for the _default
// collection. It supports search, RAG enrichment and deletes, and tracks the
// index generation for cache invalidation.
type RemoteSearcher struct {
	Client ingestionv1.IndexServiceClient

	gen atomic.Uint64
}

// HybridSearch implements Searcher.
func (s *RemoteSearcher) HybridSearch(ctx context.Context, textQuery string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	resp, err := s.Client.Search(ctx, &ingestionv1.SearchRequest{
		Query:        textQuery,
		QueryVector:  queryVec,
		Filters:      filters,
		TopK:         int32(topK),
		CallerTenant: tenantOf(ctx),
	})
	if err != nil {
		return nil, err
	}
	results := make([]index.ScoredResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = index.ScoredResult{
			ID:            r.Id,
			FusedScore:    r.FusedScore,
			BM25Score:     r.Bm25Score,
			SemanticScore: r.SemanticScore,
		}
	}
	return results, nil
}

// NodeCount implements Searcher.
func (s *RemoteSearcher) NodeCount() int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := s.Client.Stats(ctx, &ingestionv1.StatsRequest{})
	if err != nil {
		return 0
	}
	return int(resp.HnswNodes)
}

// GetDocumentsByIDs implements RAGSearcher.
func (s *RemoteSearcher) GetDocumentsByIDs(ctx context.Context, ids []uint32) (map[uint32]index.SearchHit, error) {
	resp, err := s.Client.GetDocuments(ctx, &ingestionv1.GetDocumentsRequest{Ids: ids, CallerTenant: tenantOf(ctx)})
	if err != nil {
		return nil, err
	}
	out := make(map[uint32]index.SearchHit, len(resp.Documents))
	for _, d := range resp.Documents {
		out[d.Id] = index.SearchHit{Content: d.Content, Metadata: d.Metadata}
	}
	return out, nil
}

// Delete implements DeleteSearcher.
func (s *RemoteSearcher) Delete(ctx context.Context, docIDs []uint32, filters map[string]string) (int, error) {
	resp, err := s.Client.DeleteDocuments(ctx, &ingestionv1.DeleteDocumentsRequest{Ids: docIDs, Filters: filters, CallerTenant: tenantOf(ctx)})
	if err != nil {
		return 0, err
	}
	return int(resp.Deleted), nil
}

// DeleteByFilter implements DeleteSearcher.
func (s *RemoteSearcher) DeleteByFilter(ctx context.Context, filters map[string]string) (int, error) {
	resp, err := s.Client.DeleteDocuments(ctx, &ingestionv1.DeleteDocumentsRequest{Filters: filters, CallerTenant: tenantOf(ctx)})
	if err != nil {
		return 0, err
	}
	return int(resp.Deleted), nil
}

// Generation returns the last observed index generation.
func (s *RemoteSearcher) Generation() uint64 { return s.gen.Load() }

// WatchGeneration polls the index generation until ctx is done, so a gateway
// cache drops entries soon after the indexer ingests or deletes.
func (s *RemoteSearcher) WatchGeneration(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		c, cancel := context.WithTimeout(ctx, every)
		resp, err := s.Client.Stats(c, &ingestionv1.StatsRequest{})
		cancel()
		if err == nil {
			s.gen.Store(resp.Generation)
		} else if ctx.Err() == nil {
			// Unknown state: force misses by moving the generation.
			s.gen.Add(1)
			slog.Debug("gateway: generation poll failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
