package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SyncJob represents a scheduled connector sync operation.
type SyncJob struct {
	ID           string             `json:"id"`
	CollectionID string             `json:"collection_id,omitempty"` // target collection (empty = _default)
	ConnectorCfg ConnectorConfig    `json:"connector_cfg"`
	Streams      []ConfiguredStream `json:"streams"`
	Schedule     string             `json:"schedule"` // cron expression
	Mode         SyncMode           `json:"mode"`
	Status       JobStatus          `json:"status"`
	LastSyncAt   time.Time          `json:"last_sync_at"`
	LastError    string             `json:"last_error,omitempty"`
	RecordsRead  int64              `json:"records_read"`
	State        *SyncState         `json:"state"`
}

// JobStatus tracks the lifecycle of a sync job.
type JobStatus string

const (
	JobIdle      JobStatus = "idle"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
)

// StateStore persists sync job state and cursors.
type StateStore interface {
	SaveState(ctx context.Context, jobID string, state *SyncState) error
	LoadState(ctx context.Context, jobID string) (*SyncState, error)
	SaveJob(ctx context.Context, job *SyncJob) error
	LoadJob(ctx context.Context, jobID string) (*SyncJob, error)
	ListJobs(ctx context.Context) ([]*SyncJob, error)
	DeleteJob(ctx context.Context, jobID string) error
}

// FileStateStore implements StateStore using JSON files on the local filesystem.
// Suitable for development and single-node deployments.
type FileStateStore struct {
	dir string
	mu  sync.RWMutex
}

// NewFileStateStore creates a file-backed state store in the given directory.
func NewFileStateStore(dir string) (*FileStateStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &FileStateStore{dir: dir}, nil
}

// sensitiveConfigKeys are connector config keys that must never be persisted.
var sensitiveConfigKeys = map[string]bool{
	"connection_string":    true,
	"dsn":                  true,
	"secret_key":           true,
	"secret_access_key":    true,
	"password":             true,
	"api_key":              true,
	"api_token":            true,
	"refresh_token":        true,
	"client_secret":        true,
	"service_account_json": true,
	"token":                true,
	"access_token":         true,
	"private_key":          true,
	"username":             true,
}

// redactJob returns a copy of the job safe for persistence: Auth is dropped
// entirely and sensitive Config keys are masked. The scheduler retains the
// in-memory job (with credentials) and reloads config from connectors.json
// on restart, so nothing is lost.
func redactJob(job *SyncJob) *SyncJob {
	if job == nil {
		return nil
	}
	c := *job
	c.ConnectorCfg = ConnectorConfig{
		Type:   job.ConnectorCfg.Type,
		Config: redactConfig(job.ConnectorCfg.Config),
	}
	return &c
}

func redactConfig(cfg map[string]interface{}) map[string]interface{} {
	if cfg == nil {
		return nil
	}
	out := make(map[string]interface{}, len(cfg))
	for k, v := range cfg {
		if sensitiveConfigKeys[strings.ToLower(k)] {
			out[k] = "[REDACTED]"
			continue
		}
		out[k] = v
	}
	return out
}

func (f *FileStateStore) SaveState(ctx context.Context, jobID string, state *SyncState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeJSON(f.statePath(jobID), state)
}

func (f *FileStateStore) LoadState(ctx context.Context, jobID string) (*SyncState, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var state SyncState
	if err := f.readJSON(f.statePath(jobID), &state); err != nil {
		if os.IsNotExist(err) {
			return NewSyncState(), nil
		}
		return nil, err
	}
	return &state, nil
}

func (f *FileStateStore) SaveJob(ctx context.Context, job *SyncJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeJSON(f.jobPath(job.ID), redactJob(job))
}

func (f *FileStateStore) LoadJob(ctx context.Context, jobID string) (*SyncJob, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var job SyncJob
	if err := f.readJSON(f.jobPath(jobID), &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (f *FileStateStore) ListJobs(ctx context.Context) ([]*SyncJob, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	pattern := filepath.Join(f.dir, "job-*.json")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var jobs []*SyncJob
	for _, path := range matches {
		var job SyncJob
		if err := f.readJSON(path, &job); err != nil {
			continue
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func (f *FileStateStore) DeleteJob(ctx context.Context, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = os.Remove(f.statePath(jobID))
	return os.Remove(f.jobPath(jobID))
}

func (f *FileStateStore) statePath(jobID string) string {
	return filepath.Join(f.dir, "state-"+jobID+".json")
}

func (f *FileStateStore) jobPath(jobID string) string {
	return filepath.Join(f.dir, "job-"+jobID+".json")
}

func (f *FileStateStore) writeJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func (f *FileStateStore) readJSON(path string, v interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
