// Package connector defines the core interfaces and types for YASE's
// enterprise connector system. Connectors pull data from external sources
// (SaaS apps, databases, file stores) and produce Records that flow into
// the existing YASE ingestion pipeline (Kafka → chunk → embed → index).
package connector

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// Connector defines the interface that all data source connectors must implement.
// Inspired by Airbyte's protocol: Spec → Validate → Discover → Read.
type Connector interface {
	// ID returns a unique identifier for this connector instance.
	ID() string
	// DisplayName returns a human-readable name for this connector type.
	DisplayName() string
	// Spec describes what configuration and auth methods this connector supports.
	Spec() *ConnectorSpec
	// Validate checks connectivity and permissions with the configured credentials.
	Validate(ctx context.Context) error
	// Discover detects available data streams and their schemas.
	Discover(ctx context.Context) (*Catalog, error)
	// Read extracts records from the configured streams. Supports incremental sync
	// via the SyncState parameter. Returns channels for records and errors.
	Read(ctx context.Context, streams []ConfiguredStream, state *SyncState) (<-chan Record, <-chan error)
	// Close releases any resources held by the connector.
	Close() error
}

// SendRecord sends a record to the output channel, aborting if ctx is done.
// Connectors should use this instead of a bare channel send so a cancelled
// context (e.g., scheduler shutdown) never leaks a goroutine blocked on send.
func SendRecord(ctx context.Context, records chan<- Record, r Record) {
	select {
	case records <- r:
	case <-ctx.Done():
	}
}

// Record represents a single document/item extracted from a source.
type Record struct {
	StreamName string            // which stream this record belongs to
	ID         string            // unique doc ID within the source
	Content    []byte            // raw content (HTML, JSON, text, PDF bytes)
	MimeType   string            // for parser routing (e.g., "text/html", "application/pdf")
	URL        string            // source URL for citations
	Metadata   map[string]string // arbitrary key-value pairs (author, date, tags, etc.)
	Action     RecordAction      // Upsert or Delete
	EmittedAt  time.Time
}

// RecordAction indicates whether a record should be added/updated or removed.
type RecordAction int

const (
	Upsert RecordAction = iota
	Delete
)

// SyncState tracks incremental sync progress with per-stream opaque cursors.
// Each connector stores its own cursor format (timestamps, page tokens, LSNs, etc.).
// Thread-safe via mutex — connectors may write from multiple goroutines.
type SyncState struct {
	mu           sync.Mutex
	StreamStates map[string]json.RawMessage `json:"stream_states"`
}

// NewSyncState creates an empty sync state.
func NewSyncState() *SyncState {
	return &SyncState{StreamStates: make(map[string]json.RawMessage)}
}

// GetStreamState returns the cursor for a specific stream.
func (s *SyncState) GetStreamState(stream string) json.RawMessage {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.StreamStates == nil {
		return nil
	}
	return s.StreamStates[stream]
}

// SetStreamState stores the cursor for a specific stream.
func (s *SyncState) SetStreamState(stream string, state json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.StreamStates == nil {
		s.StreamStates = make(map[string]json.RawMessage)
	}
	s.StreamStates[stream] = state
}

// ConnectorSpec describes a connector's configuration requirements.
type ConnectorSpec struct {
	ConfigSchema json.RawMessage // JSON Schema for source-specific configuration
	AuthMethods  []AuthMethod    // supported authentication types
	SyncModes    []SyncMode      // supported sync modes
}

// SyncMode defines how data is extracted from the source.
type SyncMode string

const (
	FullRefresh SyncMode = "full_refresh" // re-read all data every sync
	Incremental SyncMode = "incremental"  // read only changes since last sync
)

// Catalog describes the available data streams from a source.
type Catalog struct {
	Streams []Stream `json:"streams"`
}

// Stream describes a single data stream (e.g., "pages", "issues", "files").
type Stream struct {
	Name               string     `json:"name"`
	SupportedSyncModes []SyncMode `json:"supported_sync_modes"`
	DefaultCursorField string     `json:"default_cursor_field,omitempty"`
}

// ConfiguredStream specifies how a stream should be read.
type ConfiguredStream struct {
	Name        string   `json:"name"`
	SyncMode    SyncMode `json:"sync_mode"`
	CursorField string   `json:"cursor_field,omitempty"`
}
