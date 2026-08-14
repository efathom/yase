package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ingestionv1 "github.com/efathom/yase/proto/v1"
)

// mockBroker implements EventBroker for testing.
type mockBroker struct {
	mu      sync.Mutex
	records []*ingestionv1.CrawlRecord
	err     error
	closed  bool
	delay   time.Duration
	callCnt atomic.Int32
}

func (m *mockBroker) Produce(_ context.Context, record *ingestionv1.CrawlRecord) error {
	m.callCnt.Add(1)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	m.records = append(m.records, record)
	m.mu.Unlock()
	return nil
}

func (m *mockBroker) Close() error {
	m.closed = true
	return nil
}

func TestPoolProcessesJobs(t *testing.T) {
	broker := &mockBroker{}
	pool := NewPool(2, 10, broker)
	pool.Start()

	records := []*ingestionv1.CrawlRecord{
		{Url: "https://example.com/1", RawContent: []byte("content1")},
		{Url: "https://example.com/2", RawContent: []byte("content2")},
		{Url: "https://example.com/3", RawContent: []byte("content3")},
	}

	ctx := context.Background()
	for _, r := range records {
		if err := pool.Submit(ctx, r, nil); err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}

	if err := pool.Stop(); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.records) != 3 {
		t.Errorf("expected 3 records, got %d", len(broker.records))
	}
	if !broker.closed {
		t.Error("expected broker to be closed")
	}
}

func TestPoolBackpressure(t *testing.T) {
	broker := &mockBroker{delay: 50 * time.Millisecond}
	pool := NewPool(1, 1, broker) // tiny queue
	pool.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// First submit should succeed
	err := pool.Submit(context.Background(), &ingestionv1.CrawlRecord{Url: "https://a.com"}, nil)
	if err != nil {
		t.Fatalf("first submit should succeed: %v", err)
	}

	// Second submit with short timeout should hit backpressure
	err = pool.Submit(ctx, &ingestionv1.CrawlRecord{Url: "https://b.com"}, nil)
	if err == nil {
		// Might succeed if worker consumed fast enough, that's acceptable
		t.Log("submit succeeded (worker consumed quickly)")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	pool.Stop()
}

func TestPoolStreamingAcks(t *testing.T) {
	broker := &mockBroker{}
	pool := NewPool(2, 10, broker)
	pool.Start()

	ackCh := make(chan Ack, 10)
	records := []*ingestionv1.CrawlRecord{
		{Url: "https://example.com/1"},
		{Url: "https://example.com/2"},
	}

	ctx := context.Background()
	for _, r := range records {
		if err := pool.Submit(ctx, r, ackCh); err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}

	pool.Stop()

	close(ackCh)
	var acks []Ack
	for a := range ackCh {
		acks = append(acks, a)
	}

	if len(acks) != 2 {
		t.Fatalf("expected 2 acks, got %d", len(acks))
	}
	for _, a := range acks {
		if a.Status != "OK" {
			t.Errorf("expected OK status, got %q", a.Status)
		}
		if a.Err != nil {
			t.Errorf("unexpected error: %v", a.Err)
		}
	}
}

func TestPoolBrokerError(t *testing.T) {
	brokerErr := errors.New("kafka unavailable")
	broker := &mockBroker{err: brokerErr}
	pool := NewPool(1, 10, broker)
	pool.Start()

	ackCh := make(chan Ack, 1)
	err := pool.Submit(context.Background(), &ingestionv1.CrawlRecord{Url: "https://fail.com"}, ackCh)
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}

	pool.Stop()

	close(ackCh)
	ack := <-ackCh
	if ack.Status != "ERROR" {
		t.Errorf("expected ERROR status, got %q", ack.Status)
	}
	if ack.Err == nil {
		t.Error("expected non-nil error in ack")
	}
}

func TestPoolSkipsExpiredJobs(t *testing.T) {
	broker := &mockBroker{}
	pool := NewPool(1, 10, broker)
	pool.Start()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Already canceled

	ackCh := make(chan Ack, 5)

	// Submit with an already-canceled context — the Submit itself uses
	// context.Background() to send to the channel, but the Job.Ctx is canceled
	// so the worker will skip it.
	pool.Submit(context.Background(), &ingestionv1.CrawlRecord{Url: "https://good.com"}, nil)

	// Directly enqueue a job whose context is already canceled
	pool.jobs <- Job{
		Ctx:    canceledCtx,
		Record: &ingestionv1.CrawlRecord{Url: "https://expired.com"},
		AckCh:  ackCh,
	}

	// Also submit a valid job with ack to confirm pool is still alive
	pool.Submit(context.Background(), &ingestionv1.CrawlRecord{Url: "https://ok.com"}, ackCh)

	pool.Stop()

	close(ackCh)
	var skipped, ok int
	for a := range ackCh {
		switch a.Status {
		case "SKIPPED":
			skipped++
		case "OK":
			ok++
		}
	}

	if skipped < 1 {
		t.Errorf("expected at least 1 SKIPPED ack, got %d", skipped)
	}
	if ok < 1 {
		t.Errorf("expected at least 1 OK ack, got %d", ok)
	}
}

func TestPoolDefaultWorkers(t *testing.T) {
	broker := &mockBroker{}
	pool := NewPool(0, 0, broker)
	if pool.workers <= 0 {
		t.Errorf("expected positive default workers, got %d", pool.workers)
	}
	if cap(pool.jobs) <= 0 {
		t.Errorf("expected positive default queue, got %d", cap(pool.jobs))
	}
}
