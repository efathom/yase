// Package audit provides structured audit event logging for compliance.
// Events are written as JSON lines to a configurable output (file, stderr, etc.).
package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// EventType classifies audit events.
type EventType string

const (
	EventSearch        EventType = "search"
	EventIngest        EventType = "ingest"
	EventDelete        EventType = "delete"
	EventAdminCluster  EventType = "admin.cluster"
	EventAdminAlias    EventType = "admin.alias"
	EventAuthFailure   EventType = "auth.failure"
	EventConnectorSync EventType = "connector.sync"
)

// Event represents a single audit event.
type Event struct {
	Timestamp  time.Time         `json:"timestamp"`
	Type       EventType         `json:"type"`
	TenantID   string            `json:"tenant_id,omitempty"`
	UserID     string            `json:"user_id,omitempty"`
	Action     string            `json:"action"`
	Resource   string            `json:"resource,omitempty"`
	Detail     map[string]string `json:"detail,omitempty"`
	StatusCode int               `json:"status_code,omitempty"`
	DurationMs int64             `json:"duration_ms,omitempty"`
	ClientIP   string            `json:"client_ip,omitempty"`
}

// Logger writes audit events to a configured output.
type Logger struct {
	mu     sync.Mutex
	writer io.Writer
	enc    *json.Encoder
	closer io.Closer // non-nil when writing to a file
}

// NewLogger creates an audit logger writing to the given output.
// Pass "stdout", "stderr", or a file path.
func NewLogger(output string) *Logger {
	var w io.Writer
	var closer io.Closer
	switch output {
	case "stdout":
		w = os.Stdout
	case "", "stderr":
		w = os.Stderr
	default:
		f, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			slog.Error("audit: failed to open output, falling back to stderr", "output", output, "error", err)
			w = os.Stderr
		} else {
			w = f
			closer = f
		}
	}

	return &Logger{
		writer: w,
		enc:    json.NewEncoder(w),
		closer: closer,
	}
}

// Log writes an audit event.
func (l *Logger) Log(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enc.Encode(event) // errors are non-fatal for audit
}

// LogSearch records a search event.
func (l *Logger) LogSearch(tenantID, userID, query string, resultCount int, durationMs int64, clientIP string) {
	l.Log(Event{
		Type:       EventSearch,
		TenantID:   tenantID,
		UserID:     userID,
		Action:     "search",
		Detail:     map[string]string{"query": query, "result_count": itoa(resultCount)},
		DurationMs: durationMs,
		ClientIP:   clientIP,
	})
}

// LogDelete records a document deletion event.
func (l *Logger) LogDelete(tenantID, userID string, deletedCount int, clientIP string) {
	l.Log(Event{
		Type:     EventDelete,
		TenantID: tenantID,
		UserID:   userID,
		Action:   "delete",
		Detail:   map[string]string{"deleted_count": itoa(deletedCount)},
		ClientIP: clientIP,
	})
}

// LogAuthFailure records a failed authentication attempt.
func (l *Logger) LogAuthFailure(clientIP, reason string) {
	l.Log(Event{
		Type:     EventAuthFailure,
		Action:   "auth_failed",
		Detail:   map[string]string{"reason": reason},
		ClientIP: clientIP,
	})
}

// Close releases any file handle held by the audit logger.
func (l *Logger) Close() error {
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
