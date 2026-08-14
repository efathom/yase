package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// RecordSink receives records from connectors for further processing.
type RecordSink func(ctx context.Context, records <-chan Record, errors <-chan error) error

// Scheduler manages periodic sync jobs for connectors.
// Each job runs on a cron schedule, tracks state for incremental sync,
// and feeds records into a RecordSink (typically the connector-to-pipeline bridge).
type Scheduler struct {
	registry    *Registry
	stateStore  StateStore
	sink        RecordSink
	cron        *cron.Cron
	jobs        map[string]*SyncJob
	cronIDs     map[string]cron.EntryID
	cancelFuncs map[string]context.CancelFunc
	mu          sync.RWMutex
	wg          sync.WaitGroup // tracks in-flight executeJob goroutines
}

// NewScheduler creates a connector sync scheduler.
func NewScheduler(registry *Registry, stateStore StateStore, sink RecordSink) *Scheduler {
	return &Scheduler{
		registry:   registry,
		stateStore: stateStore,
		sink:       sink,
		cron:       cron.New(),
		jobs:       make(map[string]*SyncJob),
		cronIDs:    make(map[string]cron.EntryID),
	}
}

// Add registers a sync job with the scheduler. If the job has a cron schedule,
// it will be executed automatically. CDC/streaming jobs (empty schedule) run continuously.
func (s *Scheduler) Add(job *SyncJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.State == nil {
		// Try to restore state from store
		state, err := s.stateStore.LoadState(context.Background(), job.ID)
		if err == nil {
			job.State = state
		} else {
			job.State = NewSyncState()
		}
	}

	s.jobs[job.ID] = job

	if job.Schedule != "" {
		id, err := s.cron.AddFunc(job.Schedule, func() {
			s.executeJob(job.ID)
		})
		if err != nil {
			return err
		}
		s.cronIDs[job.ID] = id
	}

	// Persist job
	_ = s.stateStore.SaveJob(context.Background(), job)

	return nil
}

// ErrJobRunning is returned when Trigger is called for a job already running.
var ErrJobRunning = fmt.Errorf("job is already running")

// Trigger manually starts a sync job immediately.
func (s *Scheduler) Trigger(jobID string) error {
	s.mu.RLock()
	job, ok := s.jobs[jobID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("job %q not found", jobID)
	}
	if job.Status == JobRunning {
		return ErrJobRunning
	}
	go s.executeJob(jobID)
	return nil
}

// Remove stops and removes a sync job.
func (s *Scheduler) Remove(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cronID, ok := s.cronIDs[jobID]; ok {
		s.cron.Remove(cronID)
		delete(s.cronIDs, jobID)
	}
	delete(s.jobs, jobID)

	return s.stateStore.DeleteJob(context.Background(), jobID)
}

// Status returns a snapshot of the current state of a sync job.
func (s *Scheduler) Status(jobID string) (*SyncJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, nil
	}
	copy := *job
	return &copy, nil
}

// ListJobs returns snapshots of all registered sync jobs.
func (s *Scheduler) ListJobs() []SyncJob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]SyncJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, *j)
	}
	return jobs
}

// Start begins the cron scheduler. Call after adding all jobs.
func (s *Scheduler) Start(ctx context.Context) error {
	s.cron.Start()

	// Start CDC/streaming jobs (empty schedule) immediately
	s.mu.RLock()
	for id, job := range s.jobs {
		if job.Schedule == "" {
			go s.executeJob(id)
		}
	}
	s.mu.RUnlock()

	<-ctx.Done()
	s.Stop()
	return nil
}

// Stop shuts down the cron scheduler, cancels running jobs, and waits for
// in-flight jobs to finish before returning.
func (s *Scheduler) Stop() {
	// Cancel all running jobs
	s.mu.Lock()
	for _, cancel := range s.cancelFuncs {
		cancel()
	}
	s.mu.Unlock()

	ctx := s.cron.Stop()
	<-ctx.Done()

	// Wait for in-flight jobs to unwind (with panic recovery + cleanup).
	s.wg.Wait()
}

func (s *Scheduler) executeJob(jobID string) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return
	}
	if job.Status == JobRunning {
		s.mu.Unlock()
		return
	}
	job.Status = JobRunning
	job.LastError = ""
	job.RecordsRead = 0
	s.mu.Unlock()

	s.wg.Add(1)
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			SyncStatus.WithLabelValues(jobID).Set(2) // failed
			RecordsErrorsTotal.WithLabelValues(jobID, "all").Inc()
			slog.Error("connector: job panic recovered", "job_id", jobID, "panic", r)
			s.mu.Lock()
			if j, ok := s.jobs[jobID]; ok {
				j.Status = JobFailed
				j.LastError = fmt.Sprintf("panic: %v", r)
				j.LastSyncAt = time.Now()
			}
			s.mu.Unlock()
		}
	}()

	slog.Info("connector: starting sync job", "job_id", job.ID, "type", job.ConnectorCfg.Type, "mode", job.Mode)
	startTime := time.Now()
	SyncStatus.WithLabelValues(job.ID).Set(1) // running

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Store cancel func for graceful shutdown
	s.mu.Lock()
	if s.cancelFuncs == nil {
		s.cancelFuncs = make(map[string]context.CancelFunc)
	}
	s.cancelFuncs[jobID] = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.cancelFuncs, jobID)
		s.mu.Unlock()
	}()

	// Create connector
	conn, err := s.registry.Create(job.ConnectorCfg)
	if err != nil {
		s.failJob(job, fmt.Errorf("create connector: %w", err))
		return
	}
	defer conn.Close()

	// Validate connectivity
	if err := conn.Validate(ctx); err != nil {
		s.failJob(job, fmt.Errorf("validate: %w", err))
		return
	}

	// Read records
	records, errs := conn.Read(ctx, job.Streams, job.State)

	// Count records and inject collection ID as they flow through
	countedRecords := make(chan Record, 100)
	go func() {
		defer close(countedRecords)
		for r := range records {
			// Inject collection routing metadata
			if job.CollectionID != "" {
				if r.Metadata == nil {
					r.Metadata = make(map[string]string)
				}
				r.Metadata["_collection_id"] = job.CollectionID
			}
			s.mu.Lock()
			job.RecordsRead++
			s.mu.Unlock()
			countedRecords <- r
		}
	}()

	// Process through sink
	if err := s.sink(ctx, countedRecords, errs); err != nil {
		s.failJob(job, fmt.Errorf("sink: %w", err))
		return
	}

	// Persist state
	if err := s.stateStore.SaveState(ctx, job.ID, job.State); err != nil {
		slog.Error("connector: failed to save state", "job_id", job.ID, "error", err)
	}

	elapsed := time.Since(startTime)
	s.mu.Lock()
	job.Status = JobCompleted
	job.LastSyncAt = time.Now()
	s.mu.Unlock()

	_ = s.stateStore.SaveJob(ctx, job)

	SyncDuration.WithLabelValues(job.ID, string(job.Mode)).Observe(elapsed.Seconds())
	SyncStatus.WithLabelValues(job.ID).Set(3) // completed
	LastSyncTimestamp.WithLabelValues(job.ID).SetToCurrentTime()
	RecordsReadTotal.WithLabelValues(job.ID, "all").Add(float64(job.RecordsRead))
	slog.Info("connector: job completed", "job_id", job.ID, "records", job.RecordsRead, "duration", elapsed)
}

func (s *Scheduler) failJob(job *SyncJob, err error) {
	SyncStatus.WithLabelValues(job.ID).Set(2) // failed
	RecordsErrorsTotal.WithLabelValues(job.ID, "all").Inc()
	slog.Error("connector: job failed", "job_id", job.ID, "error", err)
	s.mu.Lock()
	job.Status = JobFailed
	job.LastError = err.Error()
	job.LastSyncAt = time.Now()
	s.mu.Unlock()

	_ = s.stateStore.SaveJob(context.Background(), job)
}
