package worker

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"time"

	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	poolQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "yase",
		Subsystem: "worker_pool",
		Name:      "queue_depth",
		Help:      "Current number of jobs in the worker pool queue.",
	})

	poolJobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "yase",
		Subsystem: "worker_pool",
		Name:      "jobs_processed_total",
		Help:      "Total number of jobs processed by the worker pool.",
	}, []string{"status"})
)

// EventBroker abstracts message production (e.g., Kafka).
type EventBroker interface {
	Produce(ctx context.Context, record *ingestionv1.CrawlRecord) error
	Close() error
}

// Job represents a unit of work for the pool.
type Job struct {
	Ctx    context.Context
	Record *ingestionv1.CrawlRecord
	AckCh  chan<- Ack // nil for batch (fire-and-forget); non-nil for streaming
}

// Ack carries per-record acknowledgment back to the streaming sender goroutine.
type Ack struct {
	URL    string
	Status string
	Err    error
}

// Pool is a bounded worker pool that provides physical backpressure.
// When the jobs channel is full, Submit blocks, propagating TCP-level
// backpressure all the way to the gRPC caller.
type Pool struct {
	jobs    chan Job
	wg      sync.WaitGroup
	broker  EventBroker
	workers int
}

// NewPool creates a worker pool with the given queue capacity and broker.
// If workers <= 0, defaults to runtime.NumCPU() * 2.
func NewPool(workers, queueSize int, broker EventBroker) *Pool {
	if workers <= 0 {
		workers = runtime.NumCPU() * 2
	}
	if queueSize <= 0 {
		queueSize = 1024
	}
	return &Pool{
		jobs:    make(chan Job, queueSize),
		broker:  broker,
		workers: workers,
	}
}

// Start spawns worker goroutines that consume from the jobs channel.
func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic recovered", "panic", r)
			poolJobsProcessed.WithLabelValues("panic").Inc()
		}
	}()

	for job := range p.jobs {
		poolQueueDepth.Dec()

		// Use a detached context with a deadline for producing — the original
		// request context may be canceled after the gRPC response is sent, but
		// we still need to deliver the message to Kafka within a bounded time.
		produceCtx, cancel := context.WithTimeout(context.WithoutCancel(job.Ctx), 30*time.Second)

		// Skip expired jobs only for streaming (AckCh != nil) where the
		// caller is still waiting. Fire-and-forget batch jobs should always
		// be produced even if the gRPC context is done.
		if job.AckCh != nil && job.Ctx.Err() != nil {
			cancel()
			poolJobsProcessed.WithLabelValues("skipped").Inc()
			job.AckCh <- Ack{URL: job.Record.GetUrl(), Status: "SKIPPED", Err: job.Ctx.Err()}
			continue
		}

		err := p.broker.Produce(produceCtx, job.Record)
		cancel()
		if err != nil {
			poolJobsProcessed.WithLabelValues("error").Inc()
			if job.AckCh != nil {
				job.AckCh <- Ack{URL: job.Record.GetUrl(), Status: "ERROR", Err: err}
			}
			continue
		}

		poolJobsProcessed.WithLabelValues("success").Inc()
		if job.AckCh != nil {
			job.AckCh <- Ack{URL: job.Record.GetUrl(), Status: "OK"}
		}
	}
}

// Submit enqueues a job. Blocks when the channel is full, providing backpressure.
// Returns ctx.Err() if the context expires before the job can be enqueued.
func (p *Pool) Submit(ctx context.Context, record *ingestionv1.CrawlRecord, ackCh chan<- Ack) error {
	job := Job{Ctx: ctx, Record: record, AckCh: ackCh}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job:
		poolQueueDepth.Inc()
		return nil
	}
}

// Stop closes the jobs channel, waits for all workers to drain, then closes the broker.
func (p *Pool) Stop() error {
	close(p.jobs)
	p.wg.Wait()
	return p.broker.Close()
}
