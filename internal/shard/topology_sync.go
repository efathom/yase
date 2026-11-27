package shard

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/efathom/yase/internal/consensus"
	"github.com/efathom/yase/internal/query"
)

// TopologySync periodically polls a cluster node for topology updates
// and keeps the RemoteShardSearcher and alias resolver in sync.
type TopologySync struct {
	clusterHTTPAddr string
	token           string
	searcher        *RemoteShardSearcher
	interval        time.Duration

	mu      sync.RWMutex
	aliases map[string]*consensus.AliasInfo
	nodes   map[string]*consensus.NodeInfo
	shards  map[uint32]*consensus.ShardInfo
	version uint64
}

// NewTopologySync creates a topology synchronizer.
func NewTopologySync(clusterHTTPAddr string, searcher *RemoteShardSearcher, interval time.Duration) *TopologySync {
	if interval == 0 {
		interval = 2 * time.Second
	}
	return &TopologySync{
		clusterHTTPAddr: clusterHTTPAddr,
		searcher:        searcher,
		interval:        interval,
		aliases:         make(map[string]*consensus.AliasInfo),
		nodes:           make(map[string]*consensus.NodeInfo),
		shards:          make(map[uint32]*consensus.ShardInfo),
	}
}

// SetToken sets the cluster management token presented when polling.
func (ts *TopologySync) SetToken(token string) *TopologySync {
	ts.token = token
	return ts
}

// Start begins periodic topology polling. Blocks until ctx is cancelled.
func (ts *TopologySync) Start(ctx context.Context) {
	// Initial sync
	ts.sync(ctx)

	ticker := time.NewTicker(ts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts.sync(ctx)
		}
	}
}

func (ts *TopologySync) sync(ctx context.Context) {
	topo, err := consensus.FetchTopologyWithToken(ctx, ts.clusterHTTPAddr, ts.token)
	if err != nil {
		slog.Warn("topology: fetch failed", "error", err)
		return
	}

	ts.mu.Lock()
	if topo.Version <= ts.version {
		ts.mu.Unlock()
		return // no changes
	}

	ts.version = topo.Version
	ts.aliases = topo.Aliases
	ts.nodes = topo.Nodes
	ts.shards = topo.Shards
	ts.mu.Unlock()

	// Build shard → active replica addresses
	shardReplicas := make(map[uint32][]string)
	for shardID, shard := range topo.Shards {
		for _, nodeID := range shard.Replicas {
			if node, ok := topo.Nodes[nodeID]; ok && node.State == consensus.NodeActive {
				shardReplicas[shardID] = append(shardReplicas[shardID], node.Address)
			}
		}
	}

	ts.searcher.UpdateTopology(shardReplicas)
	slog.Info("topology: synced",
		"version", topo.Version, "nodes", len(topo.Nodes), "shards", len(topo.Shards), "aliases", len(topo.Aliases))
}

// ResolveAlias implements query.AliasResolver using the synced topology.
func (ts *TopologySync) ResolveAlias(name string) ([]uint32, map[string]string, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	alias, ok := ts.aliases[name]
	if !ok {
		return nil, nil, &AliasNotFoundError{Name: name}
	}
	return alias.ShardIDs, alias.Filters, nil
}

// Version returns the last synced topology version.
func (ts *TopologySync) Version() uint64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.version
}

// AliasNotFoundError indicates an alias doesn't exist.
type AliasNotFoundError struct {
	Name string
}

func (e *AliasNotFoundError) Error() string {
	return "alias not found: " + e.Name
}

// Verify interface compliance
var _ query.AliasResolver = (*TopologySync)(nil)
