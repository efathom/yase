package collection

import "time"

// DefaultCollectionID is the ID of the auto-created default collection.
const DefaultCollectionID = "_default"

// CollectionStatus represents the lifecycle state of a collection.
type CollectionStatus string

const (
	StatusCreating CollectionStatus = "creating"
	StatusReady    CollectionStatus = "ready"
	StatusDeleting CollectionStatus = "deleting"
)

// Collection is a logical container that groups related data sources into a
// single searchable unit. Each collection owns its own HybridEngine (Bluge +
// HNSW-IF + Arena) for physical data isolation.
type Collection struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	TenantID    string           `json:"tenant_id"`
	Description string           `json:"description,omitempty"`
	Config      CollectionConfig `json:"config"`
	Status      CollectionStatus `json:"status"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	Connectors  []string         `json:"connectors"`
	ShardIDs    []uint32         `json:"shard_ids,omitempty"`
}

// CollectionConfig holds per-collection overrides. Zero values mean "use global default".
type CollectionConfig struct {
	Embedder     *EmbedderRef `json:"embedder,omitempty"`
	Chunking     *ChunkConfig `json:"chunking,omitempty"`
	ArenaSize    uint64       `json:"arena_size_bytes,omitempty"`
	VecDim       int          `json:"vec_dim,omitempty"`
	CentroidRate int          `json:"centroid_rate,omitempty"`
	SyncSchedule string       `json:"sync_schedule,omitempty"`
}

// EmbedderRef identifies an embedder provider configuration.
type EmbedderRef struct {
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	BaseURL   string `json:"base_url,omitempty"`
	APIKey    string `json:"api_key,omitempty"`
	Dimension int    `json:"dimension,omitempty"`
}

// ChunkConfig controls text chunking behavior for a collection.
type ChunkConfig struct {
	Strategy  string  `json:"strategy"`   // "semantic", "fixed", "ast"
	Threshold float32 `json:"threshold"`  // for semantic chunking
	MaxTokens int     `json:"max_tokens"`
}
