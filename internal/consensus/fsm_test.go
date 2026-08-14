package consensus

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func TestFSMRegisterNode(t *testing.T) {
	cs := NewClusterState()

	cmd := &Command{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNode{ID: "node-1", Address: "10.0.0.1:50052"},
	}
	data, _ := cmd.Marshal()
	cs.Apply(&raft.Log{Data: data})

	if cs.NodeCount() != 1 {
		t.Fatalf("expected 1 node, got %d", cs.NodeCount())
	}

	node, ok := cs.GetNode("node-1")
	if !ok {
		t.Fatal("node-1 not found")
	}
	if node.Address != "10.0.0.1:50052" {
		t.Errorf("address: got %q, want %q", node.Address, "10.0.0.1:50052")
	}
	if node.State != NodeActive {
		t.Errorf("state: got %d, want NodeActive", node.State)
	}
}

func TestFSMRemoveNode(t *testing.T) {
	cs := NewClusterState()

	// Register then remove
	applyCmd(t, cs, &Command{Type: CmdRegisterNode, RegisterNode: &RegisterNode{ID: "n1", Address: "a:1"}})
	applyCmd(t, cs, &Command{Type: CmdRemoveNode, RemoveNode: &RemoveNode{ID: "n1"}})

	node, ok := cs.GetNode("n1")
	if !ok {
		t.Fatal("node should still exist with Dead state")
	}
	if node.State != NodeDead {
		t.Errorf("state: got %d, want NodeDead", node.State)
	}
}

func TestFSMAssignShard(t *testing.T) {
	cs := NewClusterState()

	applyCmd(t, cs, &Command{Type: CmdRegisterNode, RegisterNode: &RegisterNode{ID: "n1", Address: "a:1"}})
	applyCmd(t, cs, &Command{Type: CmdRegisterNode, RegisterNode: &RegisterNode{ID: "n2", Address: "a:2"}})
	applyCmd(t, cs, &Command{Type: CmdAssignShard, AssignShard: &AssignShard{
		ShardID:  1,
		Replicas: []string{"n1", "n2"},
	}})

	if cs.ShardCount() != 1 {
		t.Fatalf("expected 1 shard, got %d", cs.ShardCount())
	}

	shard, ok := cs.GetShard(1)
	if !ok {
		t.Fatal("shard 1 not found")
	}
	if len(shard.Replicas) != 2 {
		t.Errorf("replicas: got %d, want 2", len(shard.Replicas))
	}
	if shard.Replicas[0] != "n1" {
		t.Errorf("primary: got %q, want n1", shard.Replicas[0])
	}

	// Node should have shard in its list
	node, _ := cs.GetNode("n1")
	if !containsUint32(node.Shards, 1) {
		t.Error("n1 should have shard 1")
	}
}

func TestFSMUpdateAlias(t *testing.T) {
	cs := NewClusterState()

	applyCmd(t, cs, &Command{Type: CmdUpdateAlias, UpdateAlias: &UpdateAlias{
		Name:     "production",
		ShardIDs: []uint32{1, 2, 3},
		Filters:  map[string]string{"tenant": "acme"},
	}})

	alias, ok := cs.GetAlias("production")
	if !ok {
		t.Fatal("alias not found")
	}
	if len(alias.ShardIDs) != 3 {
		t.Errorf("shard_ids: got %d, want 3", len(alias.ShardIDs))
	}
	if alias.Filters["tenant"] != "acme" {
		t.Errorf("filter: got %q, want acme", alias.Filters["tenant"])
	}

	// Update (overwrite)
	applyCmd(t, cs, &Command{Type: CmdUpdateAlias, UpdateAlias: &UpdateAlias{
		Name:     "production",
		ShardIDs: []uint32{4, 5},
	}})

	alias, _ = cs.GetAlias("production")
	if len(alias.ShardIDs) != 2 {
		t.Errorf("after update: got %d shards, want 2", len(alias.ShardIDs))
	}
}

func TestFSMRemoveAlias(t *testing.T) {
	cs := NewClusterState()

	applyCmd(t, cs, &Command{Type: CmdUpdateAlias, UpdateAlias: &UpdateAlias{
		Name: "staging", ShardIDs: []uint32{1},
	}})
	applyCmd(t, cs, &Command{Type: CmdRemoveAlias, RemoveAlias: &RemoveAlias{Name: "staging"}})

	_, ok := cs.GetAlias("staging")
	if ok {
		t.Error("alias should be removed")
	}
}

func TestFSMVersionIncrementing(t *testing.T) {
	cs := NewClusterState()

	for i := 0; i < 5; i++ {
		applyCmd(t, cs, &Command{Type: CmdRegisterNode, RegisterNode: &RegisterNode{
			ID: "n", Address: "a",
		}})
	}

	cs.mu.RLock()
	v := cs.Version
	cs.mu.RUnlock()

	if v != 5 {
		t.Errorf("version: got %d, want 5", v)
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	cs := NewClusterState()

	// Build up some state
	applyCmd(t, cs, &Command{Type: CmdRegisterNode, RegisterNode: &RegisterNode{ID: "n1", Address: "a:1"}})
	applyCmd(t, cs, &Command{Type: CmdRegisterNode, RegisterNode: &RegisterNode{ID: "n2", Address: "a:2"}})
	applyCmd(t, cs, &Command{Type: CmdAssignShard, AssignShard: &AssignShard{ShardID: 10, Replicas: []string{"n1"}}})
	applyCmd(t, cs, &Command{Type: CmdUpdateAlias, UpdateAlias: &UpdateAlias{Name: "prod", ShardIDs: []uint32{10}}})

	// Snapshot
	snap, err := cs.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var buf bytes.Buffer
	sink := &testSnapshotSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into a fresh state
	cs2 := NewClusterState()
	if err := cs2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify
	if cs2.NodeCount() != 2 {
		t.Errorf("restored nodes: got %d, want 2", cs2.NodeCount())
	}
	if cs2.ShardCount() != 1 {
		t.Errorf("restored shards: got %d, want 1", cs2.ShardCount())
	}
	alias, ok := cs2.GetAlias("prod")
	if !ok {
		t.Fatal("alias prod not found after restore")
	}
	if len(alias.ShardIDs) != 1 || alias.ShardIDs[0] != 10 {
		t.Errorf("alias shards: got %v, want [10]", alias.ShardIDs)
	}

	cs2.mu.RLock()
	v := cs2.Version
	cs2.mu.RUnlock()
	if v != 4 {
		t.Errorf("restored version: got %d, want 4", v)
	}
}

// ── Helpers ──

func applyCmd(t *testing.T, cs *ClusterState, cmd *Command) {
	t.Helper()
	data, err := cmd.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp := cs.Apply(&raft.Log{Data: data})
	if err, ok := resp.(error); ok {
		t.Fatalf("apply: %v", err)
	}
}

// testSnapshotSink implements raft.SnapshotSink for testing.
type testSnapshotSink struct {
	buf *bytes.Buffer
}

func (s *testSnapshotSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSnapshotSink) Close() error                { return nil }
func (s *testSnapshotSink) ID() string                  { return "test" }
func (s *testSnapshotSink) Cancel() error               { return nil }
