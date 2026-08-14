package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/efathom/yase/pkg/crawler"
	"github.com/redis/go-redis/v9"
)

func TestWorkerFetchSuccess(t *testing.T) {
	// Mock HTTP server returning HTML
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body>
			<p>Hello from test server</p>
			<a href="https://example.com/link1">Link 1</a>
			<a href="https://example.com/link2">Link 2</a>
		</body></html>`)
	}))
	defer server.Close()

	// Rate limiter that always allows
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	rl := crawler.NewGlobalRateLimiter(client)

	jobs := make(chan string, 1)
	results := make(chan CrawlResult, 1)

	worker := NewWorkerNode("test-worker", "http://localhost:9999", rl, jobs, results)
	// Use a simple HTTP client for the test (no heartbeat server)
	worker.HTTPClient = server.Client()

	// Process a single job directly
	worker.processJob(context.Background(), server.URL)

	select {
	case result := <-results:
		if result.Error != nil {
			t.Fatalf("unexpected error: %v", result.Error)
		}
		if result.SourceURL != server.URL {
			t.Errorf("source URL: got %q, want %q", result.SourceURL, server.URL)
		}
		if len(result.Outlinks) != 2 {
			t.Errorf("expected 2 outlinks, got %d", len(result.Outlinks))
		}
	default:
		t.Fatal("expected a result")
	}
}

func TestWorkerPanicRecovery(t *testing.T) {
	// Mock server that returns data that could cause issues
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		// Write valid HTML — the panic recovery test is about processJob not crashing
		fmt.Fprint(w, `<html><body><p>test</p></body></html>`)
	}))
	defer server.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	rl := crawler.NewGlobalRateLimiter(client)

	results := make(chan CrawlResult, 10)
	worker := NewWorkerNode("test-panic", "http://localhost:9999", rl, nil, results)
	worker.HTTPClient = server.Client()

	// This should not panic even with unusual input
	worker.processJob(context.Background(), "://invalid-url")
	worker.processJob(context.Background(), server.URL)

	// The second call (valid URL) should produce a result
	select {
	case r := <-results:
		if r.Error != nil {
			t.Logf("got error result (acceptable): %v", r.Error)
		}
	default:
		// It's OK if no result — the point is no panic
	}
}

func TestHeartbeatSent(t *testing.T) {
	var heartbeatCount atomic.Int32

	// Mock master heartbeat endpoint
	masterServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/heartbeat" {
			heartbeatCount.Add(1)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer masterServer.Close()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	rl := crawler.NewGlobalRateLimiter(client)

	jobs := make(chan string)
	results := make(chan CrawlResult, 10)

	worker := NewWorkerNode("hb-test", masterServer.URL, rl, jobs, results)

	ctx, cancel := context.WithCancel(context.Background())
	go worker.sendHeartbeats(ctx)

	// Wait a bit for heartbeats
	time.Sleep(4 * time.Second)
	cancel()

	count := heartbeatCount.Load()
	if count < 1 {
		t.Errorf("expected at least 1 heartbeat, got %d", count)
	}
	t.Logf("heartbeats sent: %d", count)
}
