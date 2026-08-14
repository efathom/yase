// yase-ctl is the CLI for managing a YASE distributed cluster.
//
// Usage:
//
//	yase-ctl --addr http://localhost:9100 status
//	yase-ctl --addr http://localhost:9100 topology
//	yase-ctl --addr http://localhost:9100 alias create --name prod --shards 0,1,2
//	yase-ctl --addr http://localhost:9100 alias delete --name prod
//	yase-ctl --addr http://localhost:9100 shard assign --shard 0 --replicas node-0,node-1
//	yase-ctl --addr http://localhost:9100 node join --node-id node-1 --raft-addr localhost:7001 --shard-addr localhost:50054
//	yase-ctl --addr http://localhost:9100 node remove --node-id node-1
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	addr := "http://localhost:9100"
	args := os.Args[1:]

	// Parse --addr flag
	if args[0] == "--addr" {
		if len(args) < 3 {
			usage()
			os.Exit(1)
		}
		addr = args[1]
		args = args[2:]
	}

	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	cmd := args[0]
	args = args[1:]

	var err error
	switch cmd {
	case "status":
		err = doGet(addr + "/cluster/status")
	case "topology":
		err = doGet(addr + "/cluster/topology")
	case "alias":
		err = handleAlias(addr, args)
	case "shard":
		err = handleShard(addr, args)
	case "node":
		err = handleNode(addr, args)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func handleAlias(addr string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("alias subcommand required: create, delete")
	}
	switch args[0] {
	case "create":
		name, shards := "", ""
		filters := map[string]string{}
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			switch args[i] {
			case "--name":
				name = args[i+1]
			case "--shards":
				shards = args[i+1]
			case "--filter":
				parts := strings.SplitN(args[i+1], "=", 2)
				if len(parts) == 2 {
					filters[parts[0]] = parts[1]
				}
			}
		}
		if name == "" || shards == "" {
			return fmt.Errorf("--name and --shards required")
		}
		shardIDs, err := parseUint32List(shards)
		if err != nil {
			return err
		}
		body := map[string]any{"name": name, "shard_ids": shardIDs}
		if len(filters) > 0 {
			body["filters"] = filters
		}
		return doPost(addr+"/cluster/alias", body)

	case "delete":
		name := ""
		for i := 1; i < len(args); i += 2 {
			if args[i] == "--name" && i+1 < len(args) {
				name = args[i+1]
			}
		}
		if name == "" {
			return fmt.Errorf("--name required")
		}
		return doDelete(addr + "/cluster/alias?name=" + name)

	default:
		return fmt.Errorf("unknown alias subcommand: %s", args[0])
	}
}

func handleShard(addr string, args []string) error {
	if len(args) == 0 || args[0] != "assign" {
		return fmt.Errorf("usage: shard assign --shard ID --replicas node-0,node-1")
	}
	shardID, replicas := "", ""
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		switch args[i] {
		case "--shard":
			shardID = args[i+1]
		case "--replicas":
			replicas = args[i+1]
		}
	}
	if shardID == "" || replicas == "" {
		return fmt.Errorf("--shard and --replicas required")
	}
	sid, err := strconv.ParseUint(shardID, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid shard ID: %v", err)
	}
	return doPost(addr+"/cluster/shard/assign", map[string]any{
		"shard_id": uint32(sid),
		"replicas": strings.Split(replicas, ","),
	})
}

func handleNode(addr string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("node subcommand required: join, remove")
	}
	switch args[0] {
	case "join":
		nodeID, raftAddr, shardAddr := "", "", ""
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			switch args[i] {
			case "--node-id":
				nodeID = args[i+1]
			case "--raft-addr":
				raftAddr = args[i+1]
			case "--shard-addr":
				shardAddr = args[i+1]
			}
		}
		if nodeID == "" || raftAddr == "" {
			return fmt.Errorf("--node-id and --raft-addr required")
		}
		return doPost(addr+"/cluster/join", map[string]string{
			"node_id": nodeID, "raft_addr": raftAddr, "shard_addr": shardAddr,
		})

	case "remove":
		nodeID := ""
		for i := 1; i < len(args); i += 2 {
			if args[i] == "--node-id" && i+1 < len(args) {
				nodeID = args[i+1]
			}
		}
		if nodeID == "" {
			return fmt.Errorf("--node-id required")
		}
		return doPost(addr+"/cluster/leave", map[string]string{"node_id": nodeID})

	default:
		return fmt.Errorf("unknown node subcommand: %s", args[0])
	}
}

// ── HTTP helpers ──

func doGet(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return printJSON(resp.Body)
}

func doPost(url string, body any) error {
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return printJSON(resp.Body)
}

func doDelete(url string) error {
	req, _ := http.NewRequest("DELETE", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return printJSON(resp.Body)
}

func printJSON(r io.Reader) error {
	data, _ := io.ReadAll(r)
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		fmt.Println(string(data))
		return nil
	}
	fmt.Println(pretty.String())
	return nil
}

func parseUint32List(s string) ([]uint32, error) {
	parts := strings.Split(s, ",")
	result := make([]uint32, len(parts))
	for i, p := range parts {
		v, err := strconv.ParseUint(strings.TrimSpace(p), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid shard ID %q: %v", p, err)
		}
		result[i] = uint32(v)
	}
	return result, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `yase-ctl — YASE cluster management CLI

Usage:
  yase-ctl [--addr http://host:port] <command> [args]

Commands:
  status                              Show cluster status
  topology                            Show full cluster topology
  alias create --name NAME --shards 0,1,2 [--filter key=val]
  alias delete --name NAME
  shard assign --shard ID --replicas node-0,node-1
  node join --node-id ID --raft-addr HOST:PORT [--shard-addr HOST:PORT]
  node remove --node-id ID

Defaults:
  --addr  http://localhost:9100`)
}
