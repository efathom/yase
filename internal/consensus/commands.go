package consensus

import (
	"encoding/json"
	"time"

	"github.com/efathom/yase/pkg/collection"
)

// CommandType identifies the type of FSM command.
type CommandType uint8

const (
	CmdRegisterNode CommandType = iota + 1
	CmdRemoveNode
	CmdAssignShard
	CmdUpdateAlias
	CmdRemoveAlias
	CmdCreateCollection
	CmdUpdateCollection
	CmdDeleteCollection
)

// Command is a serializable FSM mutation. Exactly one payload field is set.
// Timestamp is set by the leader before replication so that all replicas apply
// the command with the same wall-clock time (deterministic FSM).
type Command struct {
	Type             CommandType       `json:"type"`
	Timestamp        time.Time         `json:"timestamp,omitempty"`
	RegisterNode     *RegisterNode     `json:"register_node,omitempty"`
	RemoveNode       *RemoveNode       `json:"remove_node,omitempty"`
	AssignShard      *AssignShard      `json:"assign_shard,omitempty"`
	UpdateAlias      *UpdateAlias      `json:"update_alias,omitempty"`
	RemoveAlias      *RemoveAlias      `json:"remove_alias,omitempty"`
	CreateCollection *CreateCollection `json:"create_collection,omitempty"`
	UpdateCollection *UpdateCollection `json:"update_collection,omitempty"`
	DeleteCollection *DeleteCollection `json:"delete_collection,omitempty"`
}

type RegisterNode struct {
	ID       string `json:"id"`
	Address  string `json:"address"`             // shard gRPC address
	HTTPAddr string `json:"http_addr,omitempty"` // cluster management HTTP address
}

type RemoveNode struct {
	ID string `json:"id"`
}

type AssignShard struct {
	ShardID  uint32   `json:"shard_id"`
	Replicas []string `json:"replicas"` // nodeIDs: first is primary
}

type UpdateAlias struct {
	Name     string            `json:"name"`
	ShardIDs []uint32          `json:"shard_ids"`
	Filters  map[string]string `json:"filters,omitempty"`
}

type RemoveAlias struct {
	Name string `json:"name"`
}

type CreateCollection struct {
	ID       string                      `json:"id"`
	TenantID string                      `json:"tenant_id"`
	Name     string                      `json:"name"`
	Config   collection.CollectionConfig `json:"config"`
}

type UpdateCollection struct {
	ID          string                       `json:"id"`
	Name        string                       `json:"name,omitempty"`
	Description string                       `json:"description,omitempty"`
	Config      *collection.CollectionConfig `json:"config,omitempty"`
	ShardIDs    []uint32                     `json:"shard_ids,omitempty"`
	Status      collection.CollectionStatus  `json:"status,omitempty"`
}

type DeleteCollection struct {
	ID string `json:"id"`
}

// Marshal encodes a command to JSON bytes for Raft log entry.
func (c *Command) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// UnmarshalCommand decodes a command from JSON bytes.
func UnmarshalCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}
