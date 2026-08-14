package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// ClusterService exposes HTTP endpoints for cluster management:
// join, leave, topology, alias CRUD. Runs on each cluster node;
// write operations are forwarded to the Raft leader.
type ClusterService struct {
	raft *RaftNode
}

// NewClusterService creates a cluster management service.
func NewClusterService(raft *RaftNode) *ClusterService {
	return &ClusterService{raft: raft}
}

// RegisterRoutes mounts all cluster management endpoints.
func (cs *ClusterService) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /cluster/join", cs.handleJoin)
	mux.HandleFunc("POST /cluster/leave", cs.handleLeave)
	mux.HandleFunc("GET /cluster/topology", cs.handleTopology)
	mux.HandleFunc("POST /cluster/alias", cs.handleCreateAlias)
	mux.HandleFunc("DELETE /cluster/alias", cs.handleDeleteAlias)
	mux.HandleFunc("POST /cluster/shard/assign", cs.handleAssignShard)
	mux.HandleFunc("GET /cluster/status", cs.handleStatus)
}

// ── Join: new node asks leader to add it as a Raft voter ──

type joinRequest struct {
	NodeID    string `json:"node_id"`
	RaftAddr  string `json:"raft_addr"`
	ShardAddr string `json:"shard_addr"`
	HTTPAddr  string `json:"http_addr"`
}

// redirectIfNotLeader writes a 307 redirect to the leader's HTTP management
// address when this node is not the leader. Returns true if a redirect was
// written (caller should return immediately).
func (cs *ClusterService) redirectIfNotLeader(w http.ResponseWriter) bool {
	if cs.raft.IsLeader() {
		return false
	}
	leader := cs.raft.LeaderHTTPAddr()
	if leader == "" {
		leader = cs.raft.LeaderAddress()
	}
	writeJSONCS(w, 307, map[string]string{"error": "not leader", "leader": leader})
	return true
}

func (cs *ClusterService) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONCS(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.NodeID == "" || req.RaftAddr == "" {
		writeJSONCS(w, 400, map[string]string{"error": "node_id and raft_addr required"})
		return
	}

	if cs.redirectIfNotLeader(w) {
		return
	}

	// Add as Raft voter
	if err := cs.raft.AddVoter(req.NodeID, req.RaftAddr, 10*time.Second); err != nil {
		writeJSONCS(w, 500, map[string]string{"error": fmt.Sprintf("add voter: %v", err)})
		return
	}

	// Register in FSM
	shardAddr := req.ShardAddr
	if shardAddr == "" {
		shardAddr = req.RaftAddr // fallback
	}
	err := cs.raft.Apply(&Command{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNode{ID: req.NodeID, Address: shardAddr, HTTPAddr: req.HTTPAddr},
	}, 5*time.Second)
	if err != nil {
		// Roll back the voter so the node isn't a quorum member without a
		// topology entry.
		_ = cs.raft.RemoveServer(req.NodeID, 5*time.Second)
		writeJSONCS(w, 500, map[string]string{"error": fmt.Sprintf("register node: %v", err)})
		return
	}

	slog.Info("cluster: node joined", "node_id", req.NodeID, "raft_addr", req.RaftAddr, "shard_addr", shardAddr)
	writeJSONCS(w, 200, map[string]string{"status": "joined", "node_id": req.NodeID})
}

// ── Leave: remove node from cluster ──

type leaveRequest struct {
	NodeID string `json:"node_id"`
}

func (cs *ClusterService) handleLeave(w http.ResponseWriter, r *http.Request) {
	var req leaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONCS(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}

	if cs.redirectIfNotLeader(w) {
		return
	}

	// Snapshot shards hosted by the leaving node (deep-copied) before removal.
	state := cs.raft.State()
	type shardReplicas struct {
		id       uint32
		replicas []string
	}
	var affected []shardReplicas
	state.mu.RLock()
	for id, shard := range state.Shards {
		for _, rep := range shard.Replicas {
			if rep == req.NodeID {
				affected = append(affected, shardReplicas{id: id, replicas: append([]string(nil), shard.Replicas...)})
				break
			}
		}
	}
	state.mu.RUnlock()

	if err := cs.raft.RemoveServer(req.NodeID, 10*time.Second); err != nil {
		writeJSONCS(w, 500, map[string]string{"error": fmt.Sprintf("remove server: %v", err)})
		return
	}

	if err := cs.raft.Apply(&Command{
		Type:       CmdRemoveNode,
		RemoveNode: &RemoveNode{ID: req.NodeID},
	}, 5*time.Second); err != nil {
		slog.Error("cluster: remove node FSM apply failed", "node_id", req.NodeID, "error", err)
		writeJSONCS(w, 500, map[string]string{"error": fmt.Sprintf("remove node: %v", err)})
		return
	}

	// Reassign shards the leaving node hosted: drop it from the replica list.
	for _, shard := range affected {
		var replicas []string
		for _, rep := range shard.replicas {
			if rep != req.NodeID {
				replicas = append(replicas, rep)
			}
		}
		if len(replicas) == 0 {
			slog.Warn("cluster: shard has no remaining replicas after node leave", "shard_id", shard.id)
			continue
		}
		if err := cs.raft.Apply(&Command{
			Type:        CmdAssignShard,
			AssignShard: &AssignShard{ShardID: shard.id, Replicas: replicas},
		}, 5*time.Second); err != nil {
			slog.Error("cluster: shard reassignment failed", "shard_id", shard.id, "error", err)
		}
	}

	slog.Info("cluster: node removed", "node_id", req.NodeID)
	writeJSONCS(w, 200, map[string]string{"status": "removed", "node_id": req.NodeID})
}

// ── Topology: full cluster state for gateway discovery ──

type topologyResponse struct {
	Nodes   map[string]*NodeInfo  `json:"nodes"`
	Shards  map[uint32]*ShardInfo `json:"shards"`
	Aliases map[string]*AliasInfo `json:"aliases"`
	Version uint64                `json:"version"`
	Leader  bool                  `json:"is_leader"`
}

func (cs *ClusterService) handleTopology(w http.ResponseWriter, _ *http.Request) {
	state := cs.raft.State()
	leader := cs.raft.IsLeader()

	state.mu.RLock()
	// Deep-copy the maps so JSON encoding happens without holding the lock
	// and never races with a concurrent Raft Apply mutating the live maps.
	resp := topologyResponse{
		Nodes:   copyNodes(state.Nodes),
		Shards:  copyShards(state.Shards),
		Aliases: copyAliases(state.Aliases),
		Version: state.Version,
		Leader:  leader,
	}
	state.mu.RUnlock()
	writeJSONCS(w, 200, resp)
}

func copyNodes(src map[string]*NodeInfo) map[string]*NodeInfo {
	if src == nil {
		return nil
	}
	dst := make(map[string]*NodeInfo, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		nv := *v
		nv.Shards = append([]uint32(nil), v.Shards...)
		dst[k] = &nv
	}
	return dst
}

func copyShards(src map[uint32]*ShardInfo) map[uint32]*ShardInfo {
	if src == nil {
		return nil
	}
	dst := make(map[uint32]*ShardInfo, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		sv := *v
		sv.Replicas = append([]string(nil), v.Replicas...)
		dst[k] = &sv
	}
	return dst
}

func copyAliases(src map[string]*AliasInfo) map[string]*AliasInfo {
	if src == nil {
		return nil
	}
	dst := make(map[string]*AliasInfo, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		av := *v
		av.ShardIDs = append([]uint32(nil), v.ShardIDs...)
		if v.Filters != nil {
			av.Filters = make(map[string]string, len(v.Filters))
			for fk, fv := range v.Filters {
				av.Filters[fk] = fv
			}
		}
		dst[k] = &av
	}
	return dst
}

// ── Alias management ──

type aliasRequest struct {
	Name     string            `json:"name"`
	ShardIDs []uint32          `json:"shard_ids"`
	Filters  map[string]string `json:"filters,omitempty"`
}

func (cs *ClusterService) handleCreateAlias(w http.ResponseWriter, r *http.Request) {
	var req aliasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONCS(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}

	if cs.redirectIfNotLeader(w) {
		return
	}

	err := cs.raft.Apply(&Command{
		Type: CmdUpdateAlias,
		UpdateAlias: &UpdateAlias{
			Name:     req.Name,
			ShardIDs: req.ShardIDs,
			Filters:  req.Filters,
		},
	}, 5*time.Second)
	if err != nil {
		writeJSONCS(w, 500, map[string]string{"error": err.Error()})
		return
	}

	writeJSONCS(w, 200, map[string]string{"status": "created", "alias": req.Name})
}

func (cs *ClusterService) handleDeleteAlias(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeJSONCS(w, 400, map[string]string{"error": "name query param required"})
		return
	}

	if cs.redirectIfNotLeader(w) {
		return
	}

	if err := cs.raft.Apply(&Command{
		Type:        CmdRemoveAlias,
		RemoveAlias: &RemoveAlias{Name: name},
	}, 5*time.Second); err != nil {
		writeJSONCS(w, 500, map[string]string{"error": err.Error()})
		return
	}

	writeJSONCS(w, 200, map[string]string{"status": "deleted", "alias": name})
}

// ── Shard assignment ──

type assignShardRequest struct {
	ShardID  uint32   `json:"shard_id"`
	Replicas []string `json:"replicas"`
}

func (cs *ClusterService) handleAssignShard(w http.ResponseWriter, r *http.Request) {
	var req assignShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONCS(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}

	if cs.redirectIfNotLeader(w) {
		return
	}

	err := cs.raft.Apply(&Command{
		Type:        CmdAssignShard,
		AssignShard: &AssignShard{ShardID: req.ShardID, Replicas: req.Replicas},
	}, 5*time.Second)
	if err != nil {
		writeJSONCS(w, 500, map[string]string{"error": err.Error()})
		return
	}

	writeJSONCS(w, 200, map[string]string{"status": "assigned"})
}

// ── Status ──

func (cs *ClusterService) handleStatus(w http.ResponseWriter, _ *http.Request) {
	state := cs.raft.State()
	writeJSONCS(w, 200, map[string]any{
		"leader":  cs.raft.IsLeader(),
		"nodes":   state.NodeCount(),
		"shards":  state.ShardCount(),
		"version": state.Version,
	})
}

// ── Helpers ──

// FetchTopology calls a cluster node's /cluster/topology endpoint.
// Used by the distributed gateway to discover shard-to-node mappings.
func FetchTopology(ctx context.Context, clusterHTTPAddr string) (*topologyResponse, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", clusterHTTPAddr+"/cluster/topology", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("topology fetch: %w", err)
	}
	defer resp.Body.Close()

	var topo topologyResponse
	if err := json.NewDecoder(resp.Body).Decode(&topo); err != nil {
		return nil, fmt.Errorf("topology decode: %w", err)
	}
	return &topo, nil
}

func writeJSONCS(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
