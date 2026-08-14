// Plugin support for external connectors using HashiCorp go-plugin.
// External connectors run as separate processes and communicate via gRPC.
// This provides crash isolation and cross-language support.
//
// To create an external connector plugin:
//  1. Implement the Connector interface in a separate binary
//  2. Use ConnectorPlugin to serve it via go-plugin
//  3. Name the binary "yase-connector-<type>" (e.g., "yase-connector-notion")
//  4. Place it in the plugin directory
//
// The connector manager discovers plugins via glob pattern and registers them
// in the DefaultRegistry.

package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	goplugin "github.com/hashicorp/go-plugin"
)

// PluginHandshake is used to verify that the plugin and host are compatible.
var PluginHandshake = goplugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "YASE_CONNECTOR_PLUGIN",
	MagicCookieValue: "yase-connector-v1",
}

// ConnectorPlugin implements go-plugin's Plugin interface for connectors.
type ConnectorPlugin struct {
	Impl Connector // only set on the plugin (server) side
}

func (p *ConnectorPlugin) Server(*goplugin.MuxBroker) (interface{}, error) {
	return &ConnectorRPCServer{Impl: p.Impl}, nil
}

func (p *ConnectorPlugin) Client(b *goplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &ConnectorRPCClient{client: c}, nil
}

// --- RPC Server (plugin side) ---

type ConnectorRPCServer struct {
	Impl Connector
}

func (s *ConnectorRPCServer) ID(_ struct{}, resp *string) error {
	*resp = s.Impl.ID()
	return nil
}

func (s *ConnectorRPCServer) DisplayName(_ struct{}, resp *string) error {
	*resp = s.Impl.DisplayName()
	return nil
}

func (s *ConnectorRPCServer) Validate(_ struct{}, resp *string) error {
	err := s.Impl.Validate(context.Background())
	if err != nil {
		*resp = err.Error()
	}
	return nil
}

type DiscoverResp struct {
	CatalogJSON []byte
	Error       string
}

func (s *ConnectorRPCServer) Discover(_ struct{}, resp *DiscoverResp) error {
	catalog, err := s.Impl.Discover(context.Background())
	if err != nil {
		resp.Error = err.Error()
		return nil
	}
	resp.CatalogJSON, _ = json.Marshal(catalog)
	return nil
}

type ReadArgs struct {
	StreamsJSON []byte
	StateJSON   []byte
}

type ReadResp struct {
	RecordsJSON []byte // JSON array of Record
	StateJSON   []byte
	Error       string
}

func (s *ConnectorRPCServer) Read(args ReadArgs, resp *ReadResp) error {
	var streams []ConfiguredStream
	if err := json.Unmarshal(args.StreamsJSON, &streams); err != nil {
		resp.Error = fmt.Sprintf("unmarshal streams: %v", err)
		return nil
	}

	var state SyncState
	if len(args.StateJSON) > 0 {
		if err := json.Unmarshal(args.StateJSON, &state); err != nil {
			resp.Error = fmt.Sprintf("unmarshal state: %v", err)
			return nil
		}
	} else {
		state = *NewSyncState()
	}

	records, errs := s.Impl.Read(context.Background(), streams, &state)

	var allRecords []Record
	for r := range records {
		allRecords = append(allRecords, r)
	}
	for err := range errs {
		if err != nil {
			resp.Error = err.Error()
		}
	}

	resp.RecordsJSON, _ = json.Marshal(allRecords)
	resp.StateJSON, _ = json.Marshal(&state)
	return nil
}

// --- RPC Client (host side) ---

type ConnectorRPCClient struct {
	client       *rpc.Client
	pluginClient *goplugin.Client // retained to kill subprocess on Close
}

func (c *ConnectorRPCClient) ID() string {
	var resp string
	_ = c.client.Call("Plugin.ID", struct{}{}, &resp)
	return resp
}

func (c *ConnectorRPCClient) DisplayName() string {
	var resp string
	_ = c.client.Call("Plugin.DisplayName", struct{}{}, &resp)
	return resp
}

func (c *ConnectorRPCClient) Spec() *ConnectorSpec {
	return &ConnectorSpec{} // spec is handled by the host registration
}

func (c *ConnectorRPCClient) Validate(ctx context.Context) error {
	var resp string
	if err := c.client.Call("Plugin.Validate", struct{}{}, &resp); err != nil {
		return err
	}
	if resp != "" {
		return fmt.Errorf("%s", resp)
	}
	return nil
}

func (c *ConnectorRPCClient) Discover(ctx context.Context) (*Catalog, error) {
	var resp DiscoverResp
	if err := c.client.Call("Plugin.Discover", struct{}{}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	var catalog Catalog
	_ = json.Unmarshal(resp.CatalogJSON, &catalog)
	return &catalog, nil
}

func (c *ConnectorRPCClient) Read(ctx context.Context, streams []ConfiguredStream, state *SyncState) (<-chan Record, <-chan error) {
	records := make(chan Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		streamsJSON, _ := json.Marshal(streams)
		stateJSON, _ := json.Marshal(state)

		var resp ReadResp
		if err := c.client.Call("Plugin.Read", ReadArgs{
			StreamsJSON: streamsJSON,
			StateJSON:   stateJSON,
		}, &resp); err != nil {
			errs <- err
			return
		}
		if resp.Error != "" {
			errs <- fmt.Errorf("%s", resp.Error)
		}

		// Update state from plugin
		if len(resp.StateJSON) > 0 {
			_ = json.Unmarshal(resp.StateJSON, state)
		}

		var allRecords []Record
		_ = json.Unmarshal(resp.RecordsJSON, &allRecords)
		for _, r := range allRecords {
			records <- r
		}
	}()

	return records, errs
}

func (c *ConnectorRPCClient) Close() error {
	rpcErr := c.client.Close()
	if c.pluginClient != nil {
		c.pluginClient.Kill()
	}
	return rpcErr
}

// --- Plugin Discovery ---

// DiscoverPlugins scans a directory for connector plugin binaries
// matching the pattern "yase-connector-*" and registers them in the registry.
func DiscoverPlugins(registry *Registry, pluginDir string) error {
	pattern := filepath.Join(pluginDir, "yase-connector-*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob plugins: %w", err)
	}

	for _, pluginPath := range matches {
		info, err := os.Stat(pluginPath)
		if err != nil || info.IsDir() {
			continue
		}

		// Extract connector type from binary name: "yase-connector-notion" → "notion"
		base := filepath.Base(pluginPath)
		connType := base[len("yase-connector-"):]

		path := pluginPath // capture for closure
		registry.Register(connType, func(cfg ConnectorConfig) (Connector, error) {
			return launchPlugin(path, cfg)
		})

		slog.Info("plugin: registered external connector", "type", connType, "path", pluginPath)
	}

	return nil
}

func launchPlugin(pluginPath string, cfg ConnectorConfig) (Connector, error) {
	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: PluginHandshake,
		Plugins: map[string]goplugin.Plugin{
			"connector": &ConnectorPlugin{},
		},
		Cmd:              exec.Command(pluginPath),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolNetRPC},
		StartTimeout:     30 * time.Second,
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("launch plugin %s: %w", pluginPath, err)
	}

	raw, err := rpcClient.Dispense("connector")
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("dispense plugin %s: %w", pluginPath, err)
	}

	conn := raw.(*ConnectorRPCClient)
	conn.pluginClient = client
	return conn, nil
}

// ServePlugin is called from a plugin binary's main() to serve a connector
// implementation over go-plugin's RPC protocol.
func ServePlugin(impl Connector) {
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: PluginHandshake,
		Plugins: map[string]goplugin.Plugin{
			"connector": &ConnectorPlugin{Impl: impl},
		},
	})
}
