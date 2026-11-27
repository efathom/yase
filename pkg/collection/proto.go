package collection

import (
	"context"
	"fmt"

	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Stats summarizes a collection's index.
type Stats struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Status         string `json:"status"`
	HNSWNodes      int    `json:"hnsw_nodes"`
	Centroids      int    `json:"centroids"`
	Leaves         int    `json:"leaves"`
	Documents      int    `json:"documents"`
	ArenaUsedBytes uint64 `json:"arena_used_bytes"`
	Connectors     int    `json:"connectors"`
}

// Stats returns index statistics for a collection the caller may access.
func (m *Manager) Stats(ctx context.Context, callerTenant, id string) (*Stats, error) {
	c, err := m.Get(ctx, callerTenant, id)
	if err != nil {
		return nil, err
	}
	eng, err := m.GetEngine(id)
	if err != nil {
		return nil, err
	}
	centroids, leaves := eng.InvertedFileStats()
	return &Stats{
		ID: c.ID, Name: c.Name, Status: string(c.Status),
		HNSWNodes: eng.NodeCount(), Centroids: centroids, Leaves: leaves,
		Documents: eng.DocCount(), ArenaUsedBytes: eng.ArenaUsedBytes(),
		Connectors: len(c.Connectors),
	}, nil
}

// ConfigToProto converts a CollectionConfig to its wire form.
func ConfigToProto(c CollectionConfig) *ingestionv1.CollectionConfig {
	pc := &ingestionv1.CollectionConfig{
		ArenaSizeBytes: c.ArenaSize,
		VecDim:         int32(c.VecDim),
		CentroidRate:   int32(c.CentroidRate),
		SyncSchedule:   c.SyncSchedule,
	}
	if e := c.Embedder; e != nil {
		pc.Embedder = &ingestionv1.EmbedderRef{Provider: e.Provider, Model: e.Model, BaseUrl: e.BaseURL, ApiKey: e.APIKey, Dimension: int32(e.Dimension)}
	}
	if ch := c.Chunking; ch != nil {
		pc.Chunking = &ingestionv1.ChunkConfig{Strategy: ch.Strategy, Threshold: ch.Threshold, MaxTokens: int32(ch.MaxTokens)}
	}
	return pc
}

// ConfigFromProto converts the wire form back to a CollectionConfig.
func ConfigFromProto(pc *ingestionv1.CollectionConfig) CollectionConfig {
	if pc == nil {
		return CollectionConfig{}
	}
	c := CollectionConfig{
		ArenaSize:    pc.ArenaSizeBytes,
		VecDim:       int(pc.VecDim),
		CentroidRate: int(pc.CentroidRate),
		SyncSchedule: pc.SyncSchedule,
	}
	if e := pc.Embedder; e != nil {
		c.Embedder = &EmbedderRef{Provider: e.Provider, Model: e.Model, BaseURL: e.BaseUrl, APIKey: e.ApiKey, Dimension: int(e.Dimension)}
	}
	if ch := pc.Chunking; ch != nil {
		c.Chunking = &ChunkConfig{Strategy: ch.Strategy, Threshold: ch.Threshold, MaxTokens: int(ch.MaxTokens)}
	}
	return c
}

// ToProto converts a collection (redacted) to its wire form.
func ToProto(c *Collection) *ingestionv1.Collection {
	r := c.Redacted()
	return &ingestionv1.Collection{
		Id: r.ID, Name: r.Name, TenantId: r.TenantID, Description: r.Description,
		Config: ConfigToProto(r.Config), Status: string(r.Status),
		CreatedAt: timestamppb.New(r.CreatedAt), UpdatedAt: timestamppb.New(r.UpdatedAt),
		Connectors: r.Connectors, ShardIds: r.ShardIDs,
	}
}

// FromProto converts the wire form to a Collection.
func FromProto(pc *ingestionv1.Collection) (*Collection, error) {
	if pc == nil {
		return nil, fmt.Errorf("nil collection")
	}
	return &Collection{
		ID: pc.Id, Name: pc.Name, TenantID: pc.TenantId, Description: pc.Description,
		Config: ConfigFromProto(pc.Config), Status: CollectionStatus(pc.Status),
		CreatedAt: pc.CreatedAt.AsTime(), UpdatedAt: pc.UpdatedAt.AsTime(),
		Connectors: append([]string{}, pc.Connectors...), ShardIDs: pc.ShardIds,
	}, nil
}
