package server

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/efathom/yase/internal/idempotency"
	"github.com/efathom/yase/internal/worker"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/metadata"
)

// testBroker collects produced records for assertions.
type testBroker struct {
	mu      sync.Mutex
	records []*ingestionv1.CrawlRecord
	closed  bool
}

func (b *testBroker) Produce(_ context.Context, record *ingestionv1.CrawlRecord) error {
	b.mu.Lock()
	b.records = append(b.records, record)
	b.mu.Unlock()
	return nil
}

func (b *testBroker) Close() error {
	b.closed = true
	return nil
}

func TestIngestBatch_Success(t *testing.T) {
	broker := &testBroker{}
	pool := worker.NewPool(2, 100, broker)
	pool.Start()

	handler := NewIngestionHandler(pool, nil)

	req := &ingestionv1.BatchIngestRequest{
		Records: []*ingestionv1.CrawlRecord{
			{Url: "https://example.com/1", RawContent: []byte("hello")},
			{Url: "https://example.com/2", RawContent: []byte("world")},
		},
	}

	resp, err := handler.IngestBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("IngestBatch error: %v", err)
	}

	pool.Stop()

	if resp.ProcessedCount != 2 {
		t.Errorf("expected 2, got %d", resp.ProcessedCount)
	}
	if resp.Status != "SUCCESS" {
		t.Errorf("expected SUCCESS, got %q", resp.Status)
	}
}

func TestIngestBatch_Empty(t *testing.T) {
	broker := &testBroker{}
	pool := worker.NewPool(1, 10, broker)
	pool.Start()
	defer pool.Stop()

	handler := NewIngestionHandler(pool, nil)

	resp, err := handler.IngestBatch(context.Background(), &ingestionv1.BatchIngestRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "EMPTY" {
		t.Errorf("expected EMPTY, got %q", resp.Status)
	}
}

func TestIngestBatch_NilRequest(t *testing.T) {
	broker := &testBroker{}
	pool := worker.NewPool(1, 10, broker)
	pool.Start()
	defer pool.Stop()

	handler := NewIngestionHandler(pool, nil)

	resp, err := handler.IngestBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "EMPTY" {
		t.Errorf("expected EMPTY, got %q", resp.Status)
	}
}

func TestIngestBatch_Idempotency(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	idemp := idempotency.NewManager(client)

	broker := &testBroker{}
	pool := worker.NewPool(2, 100, broker)
	pool.Start()

	handler := NewIngestionHandler(pool, idemp)

	md := metadata.Pairs("x-idempotency-key", "batch-001")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	req := &ingestionv1.BatchIngestRequest{
		Records: []*ingestionv1.CrawlRecord{
			{Url: "https://example.com/1"},
		},
	}

	// First call — should process
	resp1, err := handler.IngestBatch(ctx, req)
	if err != nil {
		t.Fatalf("first call error: %v", err)
	}
	if resp1.Status != "SUCCESS" {
		t.Errorf("first call: expected SUCCESS, got %q", resp1.Status)
	}

	// Second call with same key — should be duplicate
	resp2, err := handler.IngestBatch(ctx, req)
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}
	if resp2.Status != "DUPLICATE" {
		t.Errorf("second call: expected DUPLICATE, got %q", resp2.Status)
	}

	pool.Stop()

	// Verify only one set of records was produced
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.records) != 1 {
		t.Errorf("expected 1 record produced, got %d", len(broker.records))
	}
}

func TestIngestBatch_ProducedRecords(t *testing.T) {
	broker := &testBroker{}
	pool := worker.NewPool(2, 100, broker)
	pool.Start()

	handler := NewIngestionHandler(pool, nil)

	req := &ingestionv1.BatchIngestRequest{
		Records: []*ingestionv1.CrawlRecord{
			{Url: "https://a.com", RawContent: []byte("a")},
			{Url: "https://b.com", RawContent: []byte("b")},
			{Url: "https://c.com", RawContent: []byte("c")},
		},
	}

	_, err := handler.IngestBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	pool.Stop()

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(broker.records))
	}

	urls := map[string]bool{}
	for _, r := range broker.records {
		urls[r.Url] = true
	}
	for _, expected := range []string{"https://a.com", "https://b.com", "https://c.com"} {
		if !urls[expected] {
			t.Errorf("missing URL %s in produced records", expected)
		}
	}
}
