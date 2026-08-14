package connector

import (
	"fmt"
	"sync"
)

// Factory creates a Connector instance from configuration.
type Factory func(cfg ConnectorConfig) (Connector, error)

// ConnectorConfig holds the configuration needed to create a connector.
type ConnectorConfig struct {
	Type   string                 `json:"type" yaml:"type"`     // "confluence", "jira", "s3", "postgres"
	Config map[string]interface{} `json:"config" yaml:"config"` // source-specific config
	Auth   *AuthConfig            `json:"auth" yaml:"auth"`     // credentials
}

// Registry maps connector type names to their factory functions.
// Connectors register themselves at init time or via explicit registration.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry creates an empty connector registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]Factory),
	}
}

// Register adds a connector factory for the given type name.
func (r *Registry) Register(connectorType string, factory Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[connectorType] = factory
}

// Create instantiates a connector from configuration.
func (r *Registry) Create(cfg ConnectorConfig) (Connector, error) {
	r.mu.RLock()
	factory, ok := r.factories[cfg.Type]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown connector type: %q (registered: %v)", cfg.Type, r.List())
	}
	return factory(cfg)
}

// List returns all registered connector type names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, t)
	}
	return types
}

// DefaultRegistry is the global connector registry.
// Connector packages register themselves here via init().
var DefaultRegistry = NewRegistry()
