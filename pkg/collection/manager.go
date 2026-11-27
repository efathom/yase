package collection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/reranker"
)

var (
	// ErrNotFound indicates a collection does not exist.
	ErrNotFound = errors.New("collection not found")
	// ErrAlreadyExists indicates a collection ID is already in use.
	ErrAlreadyExists = errors.New("collection already exists")
	// ErrDefaultDelete indicates an attempt to delete the _default collection.
	ErrDefaultDelete = errors.New("cannot delete the default collection")
	// ErrInvalidConfig indicates a rejected collection configuration.
	ErrInvalidConfig = errors.New("invalid collection config")
)

// Manager handles the full lifecycle of collections: CRUD, engine provisioning,
// connector binding, and startup recovery. It is the single owner of all
// HybridEngine instances.
type Manager struct {
	mu        sync.RWMutex
	cols      map[string]*Collection
	engines   map[string]*index.HybridEngine
	embedders map[string]embedder.Embedder

	store     Store
	basePath  string
	globalCfg *config.Config
	logger    *slog.Logger

	reranker           reranker.Reranker
	rerankerCandidates int
}

// NewManager creates a collection manager. Call RestoreAll() after construction
// to reload persisted collections and ensure the _default collection exists.
func NewManager(store Store, basePath string, globalCfg *config.Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cols:      make(map[string]*Collection),
		engines:   make(map[string]*index.HybridEngine),
		embedders: make(map[string]embedder.Embedder),
		store:     store,
		basePath:  basePath,
		globalCfg: globalCfg,
		logger:    logger,
	}
}

// RestoreAll reloads persisted collections and opens their engines. If the
// _default collection does not exist, it is auto-created with global defaults.
func (m *Manager) RestoreAll(ctx context.Context) error {
	persisted, err := m.store.List(ctx)
	if err != nil {
		return fmt.Errorf("list persisted collections: %w", err)
	}

	for _, c := range persisted {
		if c.Status == StatusDeleting {
			m.logger.Warn("removing collection left in deleting state", "id", c.ID)
			_ = m.store.Delete(ctx, c.ID)
			_ = os.RemoveAll(filepath.Join(m.basePath, c.ID))
			continue
		}
		if err := m.openCollection(c); err != nil {
			m.logger.Error("failed to restore collection", "id", c.ID, "error", err)
			continue
		}
		m.reconcile(ctx, c.ID, m.engines[c.ID], m.embedders[c.ID])
		m.logger.Info("restored collection", "id", c.ID, "name", c.Name, "documents", m.engines[c.ID].DocCount())
	}

	// Ensure _default exists
	if _, exists := m.cols[DefaultCollectionID]; !exists {
		_, err := m.Create(ctx, "", DefaultCollectionID, "Default Collection", CollectionConfig{})
		if err != nil {
			return fmt.Errorf("create default collection: %w", err)
		}
	}
	return nil
}

// ValidateCollectionID reports whether a collection ID is safe to use as a
// filesystem path component. Rejects empty, "." / "..", path separators, and
// any character outside [a-zA-Z0-9._-].
func ValidateCollectionID(id string) error {
	if id == "" {
		return fmt.Errorf("collection id must not be empty")
	}
	if id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("invalid collection id %q", id)
	}
	if len(id) > 64 {
		return fmt.Errorf("collection id too long (max 64): %q", id)
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid collection id %q: allowed characters are [a-zA-Z0-9._-]", id)
	}
	return nil
}

// Create provisions a new collection with its own HybridEngine.
func (m *Manager) Create(ctx context.Context, tenantID, id, name string, cfg CollectionConfig) (*Collection, error) {
	if err := ValidateCollectionID(id); err != nil {
		return nil, err
	}
	if err := m.validateEmbedderRef(cfg.Embedder); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.cols[id]; exists {
		return nil, fmt.Errorf("%w: %q", ErrAlreadyExists, id)
	}

	// Enforce the configured collection limit.
	maxCollections := 100
	if m.globalCfg != nil && m.globalCfg.Collections.MaxCollections > 0 {
		maxCollections = m.globalCfg.Collections.MaxCollections
	}
	if len(m.cols) >= maxCollections {
		return nil, fmt.Errorf("maximum number of collections reached (%d)", maxCollections)
	}

	now := time.Now().UTC()
	c := &Collection{
		ID:         id,
		Name:       name,
		TenantID:   tenantID,
		Config:     cfg,
		Status:     StatusCreating,
		CreatedAt:  now,
		UpdatedAt:  now,
		Connectors: []string{},
	}

	if err := m.store.Save(ctx, c); err != nil {
		return nil, fmt.Errorf("persist collection: %w", err)
	}

	if err := m.openCollection(c); err != nil {
		_ = m.store.Delete(ctx, id)
		return nil, fmt.Errorf("provision engine: %w", err)
	}

	c.Status = StatusReady
	c.UpdatedAt = time.Now().UTC()
	if err := m.store.Save(ctx, c); err != nil {
		return nil, fmt.Errorf("persist ready status: %w", err)
	}

	m.logger.Info("created collection", "id", id, "name", name, "tenant", tenantID)
	return c, nil
}

// accessible reports whether callerTenant may act on c.
//
// An empty callerTenant means the caller is unscoped — auth disabled, or a
// token carrying no tenant — and retains full access, which is what
// single-tenant deployments rely on.
func accessible(callerTenant string, c *Collection) bool {
	return callerTenant == "" || c.TenantID == callerTenant
}

// Get returns collection metadata for a collection the caller's tenant owns.
//
// Callers outside the owning tenant get ErrNotFound rather than a permission
// error, so the response does not confirm that the collection exists.
func (m *Manager) Get(ctx context.Context, callerTenant, id string) (*Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.cols[id]
	if !ok || !accessible(callerTenant, c) {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	cp := *c
	cp.Connectors = append([]string(nil), c.Connectors...)
	return &cp, nil
}

// List returns all collections, optionally filtered by tenant ID.
func (m *Manager) List(ctx context.Context, tenantID string) ([]*Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Collection
	for _, c := range m.cols {
		if tenantID == "" || c.TenantID == tenantID {
			cp := *c
			cp.Connectors = append([]string(nil), c.Connectors...)
			result = append(result, &cp)
		}
	}
	return result, nil
}

// Update modifies a collection's mutable fields (name, description, config).
func (m *Manager) Update(ctx context.Context, callerTenant, id, name, description string) (*Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.cols[id]
	if !ok || !accessible(callerTenant, c) {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}

	if name != "" {
		c.Name = name
	}
	if description != "" {
		c.Description = description
	}
	c.UpdatedAt = time.Now().UTC()

	if err := m.store.Save(ctx, c); err != nil {
		return nil, fmt.Errorf("persist update: %w", err)
	}
	return c, nil
}

// Delete tears down a collection: closes the engine, removes data, and deletes
// metadata. The _default collection cannot be deleted.
func (m *Manager) Delete(ctx context.Context, callerTenant, id string) error {
	if id == DefaultCollectionID {
		return fmt.Errorf("%w: %q", ErrDefaultDelete, DefaultCollectionID)
	}

	m.mu.Lock()

	c, ok := m.cols[id]
	if !ok || !accessible(callerTenant, c) {
		m.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}

	c.Status = StatusDeleting
	c.UpdatedAt = time.Now().UTC()
	if err := m.store.Save(ctx, c); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("persist deleting status: %w", err)
	}
	eng := m.engines[id]
	delete(m.engines, id)
	delete(m.embedders, id)
	delete(m.cols, id)
	// Closing waits for in-flight searches and removing data can be slow:
	// do both without holding the manager lock, which every request takes.
	m.mu.Unlock()

	if eng != nil {
		if err := eng.Close(); err != nil {
			m.logger.Error("error closing engine", "id", id, "error", err)
		}
	}
	if err := os.RemoveAll(filepath.Join(m.basePath, id)); err != nil {
		m.logger.Error("error removing collection data", "id", id, "error", err)
	}
	if err := m.store.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete metadata: %w", err)
	}

	m.logger.Info("deleted collection", "id", id)
	return nil
}

// GetEngine returns the HybridEngine for a collection.
func (m *Manager) GetEngine(id string) (*index.HybridEngine, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	eng, ok := m.engines[id]
	if !ok {
		return nil, fmt.Errorf("%w: engine for %q", ErrNotFound, id)
	}
	return eng, nil
}

// GetEmbedder returns the embedder for a collection. If the collection has no
// per-collection embedder override, the global embedder is returned.
func (m *Manager) GetEmbedder(id string) (embedder.Embedder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if emb, ok := m.embedders[id]; ok {
		return emb, nil
	}
	return nil, fmt.Errorf("%w: embedder for %q", ErrNotFound, id)
}

// AllEngines returns a snapshot of all collection IDs to their engines.
func (m *Manager) AllEngines() map[string]*index.HybridEngine {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*index.HybridEngine, len(m.engines))
	for id, eng := range m.engines {
		result[id] = eng
	}
	return result
}

// EnginesForTenant returns the engines the caller's tenant may search.
//
// With ids empty it returns every collection the tenant owns; otherwise it
// returns the named ones it owns, silently dropping the rest — naming another
// tenant's collection must not confirm that it exists.
//
// An empty callerTenant is unscoped and reaches everything, matching Get and
// List.
func (m *Manager) EnginesForTenant(callerTenant string, ids []string) map[string]*index.HybridEngine {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*index.HybridEngine)

	if len(ids) == 0 {
		for id, eng := range m.engines {
			if c, ok := m.cols[id]; ok && accessible(callerTenant, c) {
				result[id] = eng
			}
		}
		return result
	}

	for _, id := range ids {
		c, ok := m.cols[id]
		if !ok || !accessible(callerTenant, c) {
			continue
		}
		if eng, ok := m.engines[id]; ok {
			result[id] = eng
		}
	}
	return result
}

// BindConnector associates a connector job ID with a collection.
func (m *Manager) BindConnector(ctx context.Context, callerTenant, collectionID, connectorJobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.cols[collectionID]
	if !ok || !accessible(callerTenant, c) {
		return fmt.Errorf("%w: %q", ErrNotFound, collectionID)
	}

	for _, id := range c.Connectors {
		if id == connectorJobID {
			return nil // already bound
		}
	}
	c.Connectors = append(c.Connectors, connectorJobID)
	c.UpdatedAt = time.Now().UTC()
	return m.store.Save(ctx, c)
}

// UnbindConnector removes a connector job association from a collection.
func (m *Manager) UnbindConnector(ctx context.Context, callerTenant, collectionID, connectorJobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.cols[collectionID]
	if !ok || !accessible(callerTenant, c) {
		return fmt.Errorf("%w: %q", ErrNotFound, collectionID)
	}

	filtered := c.Connectors[:0]
	for _, id := range c.Connectors {
		if id != connectorJobID {
			filtered = append(filtered, id)
		}
	}
	c.Connectors = filtered
	c.UpdatedAt = time.Now().UTC()
	return m.store.Save(ctx, c)
}

// Close shuts down all engines gracefully.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for id, eng := range m.engines {
		if err := eng.Close(); err != nil {
			m.logger.Error("error closing engine", "id", id, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	m.engines = make(map[string]*index.HybridEngine)
	m.embedders = make(map[string]embedder.Embedder)
	m.cols = make(map[string]*Collection)
	return firstErr
}

// openCollection provisions a durable HybridEngine and embedder for a
// collection and registers them in the in-memory maps. Must be called with
// m.mu held or during single-threaded startup.
func (m *Manager) openCollection(c *Collection) error {
	vecDim := m.resolveVecDim(c.Config)
	emb, err := m.resolveEmbedder(c.Config)
	if err != nil {
		return fmt.Errorf("resolve embedder: %w", err)
	}
	if d := emb.Dimension(); d > 0 && d != vecDim {
		return fmt.Errorf("embedder dimension %d does not match collection vec_dim %d", d, vecDim)
	}

	var hnswCfg config.HNSWConfig
	if m.globalCfg != nil {
		hnswCfg = m.globalCfg.HNSW
	}
	eng, err := index.OpenHybridEngine(index.EngineConfig{
		Dir:          filepath.Join(m.basePath, c.ID),
		ArenaSize:    m.resolveArenaSize(c.Config),
		HNSW:         index.HNswConfig(hnswCfg, vecDim),
		CentroidRate: m.resolveCentroidRate(c.Config),
	})
	if err != nil {
		return fmt.Errorf("open hybrid engine: %w", err)
	}
	eng.Reranker = m.reranker
	eng.RerankerCandidates = m.rerankerCandidates

	m.cols[c.ID] = c
	m.engines[c.ID] = eng
	m.embedders[c.ID] = emb
	return nil
}

// reconcile brings a freshly opened engine's vector index in line with its
// text index, re-embedding documents whose vectors were not checkpointed.
func (m *Manager) reconcile(ctx context.Context, id string, eng *index.HybridEngine, emb embedder.Embedder) {
	start := time.Now()
	st, err := eng.Reconcile(ctx, emb.EmbedBatch, 64)
	if err != nil {
		m.logger.Error("collection reconcile failed", "id", id, "error", err)
		return
	}
	if st.ReEmbedded > 0 || st.DroppedVectors > 0 || st.MissingVectors > 0 {
		m.logger.Warn("collection reconciled", "id", id, "documents", st.Documents,
			"re_embedded", st.ReEmbedded, "dropped_vectors", st.DroppedVectors,
			"missing_vectors", st.MissingVectors, "duration", time.Since(start))
	}
}

// embedderKey identifies the embedding model a collection uses.
func embedderKey(c *Collection) string {
	if e := c.Config.Embedder; e != nil {
		return e.Provider + "|" + e.Model + "|" + e.BaseURL + "|" + strconv.Itoa(e.Dimension)
	}
	return "global"
}

// SameEmbedder reports whether all the given collections embed with the same
// model, i.e. whether their semantic scores are directly comparable.
func (m *Manager) SameEmbedder(ids []string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := ""
	for i, id := range ids {
		c, ok := m.cols[id]
		if !ok {
			return false
		}
		k := embedderKey(c)
		if i == 0 {
			key = k
		} else if k != key {
			return false
		}
	}
	return true
}

// SetReranker attaches a cross-encoder reranker to every collection (current
// and future). Pass nil to disable.
func (m *Manager) SetReranker(r reranker.Reranker, candidates int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reranker, m.rerankerCandidates = r, candidates
	for _, eng := range m.engines {
		eng.Reranker = r
		eng.RerankerCandidates = candidates
	}
}

// Checkpoint durably persists every collection's vector index. The indexing
// daemon calls it before committing Kafka offsets.
func (m *Manager) Checkpoint(ctx context.Context) error {
	var errs []error
	for id, eng := range m.AllEngines() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := eng.Checkpoint(); err != nil && !errors.Is(err, index.ErrEngineClosed) {
			metrics.EngineCheckpointsTotal.WithLabelValues("error").Inc()
			errs = append(errs, fmt.Errorf("collection %s: %w", id, err))
			continue
		}
		metrics.EngineCheckpointsTotal.WithLabelValues("ok").Inc()
	}
	return errors.Join(errs...)
}

// MaintenanceConfig controls the background maintenance loop.
type MaintenanceConfig struct {
	CheckpointInterval time.Duration // periodic checkpoint for writes not driven by Kafka (default 1m)
	CompactDeadRatio   float64       // compact when dead bytes exceed this share of the arena (default 0.3)
	TTL                index.TTLConfig
}

// StartMaintenance runs periodic checkpoints, compaction and TTL expiry for
// every collection until ctx is cancelled.
func (m *Manager) StartMaintenance(ctx context.Context, mc MaintenanceConfig) {
	if mc.CheckpointInterval <= 0 {
		mc.CheckpointInterval = time.Minute
	}
	if mc.CompactDeadRatio <= 0 {
		mc.CompactDeadRatio = 0.3
	}
	if mc.TTL.SweepInterval <= 0 {
		mc.TTL.SweepInterval = 10 * time.Minute
	}
	go func() {
		ckpt := time.NewTicker(mc.CheckpointInterval)
		ttl := time.NewTicker(mc.TTL.SweepInterval)
		defer ckpt.Stop()
		defer ttl.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ckpt.C:
				if err := m.Checkpoint(ctx); err != nil {
					m.logger.Error("periodic checkpoint failed", "error", err)
				}
				for id, eng := range m.AllEngines() {
					ran, err := eng.MaybeCompact(mc.CompactDeadRatio)
					switch {
					case err != nil && !errors.Is(err, index.ErrEngineClosed):
						metrics.EngineCompactionsTotal.WithLabelValues("error").Inc()
						m.logger.Error("compaction failed", "id", id, "error", err)
					case ran:
						metrics.EngineCompactionsTotal.WithLabelValues("ok").Inc()
						m.logger.Info("compacted collection", "id", id)
					}
				}
			case <-ttl.C:
				for id, eng := range m.AllEngines() {
					if n := eng.SweepExpired(ctx, mc.TTL.DefaultTTL); n > 0 {
						m.logger.Info("ttl expired documents", "id", id, "count", n)
					}
				}
			}
		}
	}()
}

// ChunkOptions returns the chunking options for a collection.
func (m *Manager) ChunkOptions(id string) (threshold float32, maxChars int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.cols[id]
	if !ok || c.Config.Chunking == nil {
		return 0, 0
	}
	return c.Config.Chunking.Threshold, c.Config.Chunking.MaxTokens * 4
}

// validateEmbedderRef rejects per-collection embedder endpoints that are not
// explicitly allowed. Without this, anyone able to create a collection could
// make the server issue requests to arbitrary (internal) URLs.
func (m *Manager) validateEmbedderRef(ref *EmbedderRef) error {
	if ref == nil || ref.BaseURL == "" {
		return nil
	}
	u, err := url.Parse(ref.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%w: invalid embedder base_url %q", ErrInvalidConfig, ref.BaseURL)
	}
	var allowed []string
	if m.globalCfg != nil {
		allowed = m.globalCfg.Collections.AllowedEmbedderHosts
	}
	for _, h := range allowed {
		if strings.EqualFold(h, u.Host) || strings.EqualFold(h, u.Hostname()) {
			return nil
		}
	}
	return fmt.Errorf("%w: embedder host %q is not in collections.allowed_embedder_hosts", ErrInvalidConfig, u.Host)
}

func (m *Manager) resolveArenaSize(cfg CollectionConfig) uint64 {
	if cfg.ArenaSize > 0 {
		return cfg.ArenaSize
	}
	if m.globalCfg != nil {
		if m.globalCfg.Collections.DefaultArena > 0 {
			return m.globalCfg.Collections.DefaultArena
		}
		if m.globalCfg.Index.ArenaSize > 0 {
			return m.globalCfg.Index.ArenaSize
		}
	}
	return 64 << 30 // upper bound; arenas grow on demand
}

func (m *Manager) resolveVecDim(cfg CollectionConfig) int {
	if cfg.VecDim > 0 {
		return cfg.VecDim
	}
	if cfg.Embedder != nil && cfg.Embedder.Dimension > 0 {
		return cfg.Embedder.Dimension
	}
	if m.globalCfg != nil && m.globalCfg.Embedder.Dimension > 0 {
		return m.globalCfg.Embedder.Dimension
	}
	return 768
}

func (m *Manager) resolveCentroidRate(cfg CollectionConfig) int {
	if cfg.CentroidRate > 0 {
		return cfg.CentroidRate
	}
	if m.globalCfg != nil && m.globalCfg.Index.CentroidRate > 0 {
		return m.globalCfg.Index.CentroidRate
	}
	return 5
}

func (m *Manager) resolveEmbedder(cfg CollectionConfig) (embedder.Embedder, error) {
	if cfg.Embedder != nil {
		embCfg := config.EmbedderConfig{
			Provider:  cfg.Embedder.Provider,
			Model:     cfg.Embedder.Model,
			BaseURL:   cfg.Embedder.BaseURL,
			APIKey:    cfg.Embedder.APIKey,
			Dimension: cfg.Embedder.Dimension,
		}
		return embedder.NewFromConfig(embCfg)
	}
	if m.globalCfg != nil {
		return embedder.NewFromConfig(m.globalCfg.Embedder)
	}
	return embedder.NewFromConfig(config.EmbedderConfig{
		Provider:  "mock",
		Dimension: 768,
	})
}
