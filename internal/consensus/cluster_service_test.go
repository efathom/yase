package consensus

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestClusterServiceJoinAndTopology(t *testing.T) {
	// Bootstrap a single-node Raft cluster
	_, trans := raft.NewInmemTransport("")
	node, err := NewRaftNodeWithTransport(RaftNodeConfig{
		NodeID:    "node-0",
		Bootstrap: true,
	}, trans)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	defer node.Shutdown()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader")
		default:
			if node.IsLeader() {
				goto ready
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
ready:

	// Register the bootstrap node
	node.Apply(&Command{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNode{ID: "node-0", Address: "localhost:50053"},
	}, 5*time.Second)

	svc := NewClusterService(node)
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)

	// ── Test status ──
	t.Run("status", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/cluster/status", nil)
		mux.ServeHTTP(w, r)

		if w.Code != 200 {
			t.Fatalf("status: %d", w.Code)
		}
		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["leader"] != true {
			t.Error("expected leader=true")
		}
		t.Logf("Status: %s", w.Body.String())
	})

	// ── Test topology ──
	t.Run("topology", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/cluster/topology", nil)
		mux.ServeHTTP(w, r)

		if w.Code != 200 {
			t.Fatalf("topology: %d", w.Code)
		}
		var resp topologyResponse
		json.Unmarshal(w.Body.Bytes(), &resp)
		if len(resp.Nodes) != 1 {
			t.Errorf("expected 1 node, got %d", len(resp.Nodes))
		}
		t.Logf("Topology: %d nodes, %d shards, %d aliases, v%d",
			len(resp.Nodes), len(resp.Shards), len(resp.Aliases), resp.Version)
	})

	// ── Test create alias ──
	t.Run("create alias", func(t *testing.T) {
		body, _ := json.Marshal(aliasRequest{
			Name:     "prod",
			ShardIDs: []uint32{0, 1},
			Filters:  map[string]string{"tenant": "acme"},
		})
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/cluster/alias", bytes.NewReader(body))
		mux.ServeHTTP(w, r)

		if w.Code != 200 {
			t.Fatalf("create alias: %d %s", w.Code, w.Body.String())
		}

		// Verify via topology
		alias, ok := node.State().GetAlias("prod")
		if !ok {
			t.Fatal("alias not found")
		}
		if len(alias.ShardIDs) != 2 {
			t.Errorf("alias shards: %d", len(alias.ShardIDs))
		}
	})

	// ── Test assign shard ──
	t.Run("assign shard", func(t *testing.T) {
		body, _ := json.Marshal(assignShardRequest{
			ShardID:  0,
			Replicas: []string{"node-0"},
		})
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/cluster/shard/assign", bytes.NewReader(body))
		mux.ServeHTTP(w, r)

		if w.Code != 200 {
			t.Fatalf("assign shard: %d", w.Code)
		}

		shard, ok := node.State().GetShard(0)
		if !ok {
			t.Fatal("shard 0 not found")
		}
		if shard.Replicas[0] != "node-0" {
			t.Errorf("primary: %s", shard.Replicas[0])
		}
	})

	// ── Test delete alias ──
	t.Run("delete alias", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("DELETE", "/cluster/alias?name=prod", nil)
		mux.ServeHTTP(w, r)

		if w.Code != 200 {
			t.Fatalf("delete alias: %d", w.Code)
		}

		_, ok := node.State().GetAlias("prod")
		if ok {
			t.Error("alias should be deleted")
		}
	})
}

func TestClusterServiceFetchTopology(t *testing.T) {
	_, trans := raft.NewInmemTransport("")
	node, _ := NewRaftNodeWithTransport(RaftNodeConfig{
		NodeID:    "node-0",
		Bootstrap: true,
	}, trans)
	defer node.Shutdown()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout")
		default:
			if node.IsLeader() {
				goto ready
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
ready:

	node.Apply(&Command{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNode{ID: "n1", Address: "a:1"},
	}, 5*time.Second)
	node.Apply(&Command{
		Type:        CmdUpdateAlias,
		UpdateAlias: &UpdateAlias{Name: "test", ShardIDs: []uint32{0}},
	}, 5*time.Second)

	svc := NewClusterService(node)
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	// FetchTopology via HTTP client
	topo, err := FetchTopology(t.Context(), server.URL)
	if err != nil {
		t.Fatalf("FetchTopology: %v", err)
	}

	if len(topo.Nodes) != 1 {
		t.Errorf("nodes: %d", len(topo.Nodes))
	}
	if _, ok := topo.Aliases["test"]; !ok {
		t.Error("alias 'test' not found")
	}
	t.Logf("Remote topology: v%d, %d nodes, %d aliases", topo.Version, len(topo.Nodes), len(topo.Aliases))
}
