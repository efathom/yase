package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/efathom/yase/pkg/crawler"
	"github.com/efathom/yase/pkg/metrics"
)

// CrawlResult holds the output of processing a single URL.
type CrawlResult struct {
	SourceURL string
	Markdown  string
	Outlinks  []string
	Error     error
}

// WorkerNode represents a crawler worker that fetches URLs, extracts content,
// and reports results. It runs a bounded goroutine pool with panic recovery,
// exponential backoff retry, and periodic heartbeats to the master.
type WorkerNode struct {
	ID          string
	MasterURL   string
	RateLimiter *crawler.GlobalRateLimiter
	HTTPClient  *http.Client
	DNSCache    *crawler.DNSCache
	JobQueue    chan string
	ResultsChan chan<- CrawlResult
	RateLimit   int           // requests per RateWindow per domain (default 1)
	RateWindow  time.Duration // sliding window duration (default 1s)
	MaxRetries  int           // rate-limit retry attempts (default 3)
	Heartbeat   time.Duration // heartbeat interval (default 3s)
}

// NewWorkerNode creates a worker node with a tuned HTTP client.
func NewWorkerNode(id, masterURL string, rl *crawler.GlobalRateLimiter, jobs chan string, results chan<- CrawlResult) *WorkerNode {
	httpClient, dnsCache := crawler.NewTunedCrawlerClient(5 * time.Minute)
	return &WorkerNode{
		ID:          id,
		MasterURL:   masterURL,
		RateLimiter: rl,
		HTTPClient:  httpClient,
		DNSCache:    dnsCache,
		JobQueue:    jobs,
		ResultsChan: results,
		RateLimit:   1,
		RateWindow:  time.Second,
		MaxRetries:  3,
		Heartbeat:   3 * time.Second,
	}
}

// Close releases worker-owned resources (DNS cache sweeper, idle connections).
func (w *WorkerNode) Close() {
	if w.DNSCache != nil {
		w.DNSCache.Close()
	}
	if w.HTTPClient != nil {
		if tr, ok := w.HTTPClient.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
}

// sendResult sends a crawl result, aborting if the worker context is canceled
// so workers never block forever on a full results channel during shutdown.
func (w *WorkerNode) sendResult(ctx context.Context, r CrawlResult) {
	select {
	case w.ResultsChan <- r:
	case <-ctx.Done():
	}
}

// Start initiates the bounded worker pool with the given concurrency level.
// Each goroutine pulls URLs from the job queue, fetches them, and pushes
// results. A heartbeat daemon and URL polling loop run concurrently.
func (w *WorkerNode) Start(ctx context.Context, concurrency int) {
	var wg sync.WaitGroup

	// Heartbeat daemon
	go w.sendHeartbeats(ctx)

	// Poll master for URL assignments
	go w.pollForURLs(ctx)

	// Fan-out: bounded goroutines
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case jobURL, ok := <-w.JobQueue:
					if !ok {
						return
					}
					metrics.CrawlerActiveGoroutines.Inc()
					w.processJob(ctx, jobURL)
					metrics.CrawlerActiveGoroutines.Dec()
				}
			}
		}()
	}

	// Fan-in: close results after all workers drain
	go func() {
		wg.Wait()
		close(w.ResultsChan)
	}()
}

// processJob fetches a URL with rate limiting, exponential backoff, and panic
// recovery. Malformed HTML must never crash the fleet.
func (w *WorkerNode) processJob(ctx context.Context, targetURL string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic recovered", "worker_id", w.ID, "url", targetURL, "panic", r)
		}
	}()

	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return
	}
	domain := parsedURL.Host

	// Rate limit with exponential backoff
	maxRetries := w.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		if w.RateLimiter.AllowRequest(ctx, domain, w.RateLimit, w.RateWindow) {
			break
		}
		if attempt == maxRetries-1 {
			return // Rate limit exhausted — drop
		}
		backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36")

	start := time.Now()
	resp, err := w.HTTPClient.Do(req)
	metrics.CrawlerFetchDuration.WithLabelValues(domain).Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.CrawlerURLsFetched.WithLabelValues("GET", "error", domain).Inc()
		w.sendResult(ctx, CrawlResult{Error: fmt.Errorf("fetching %s: %w", domain, err)})
		return
	}
	defer resp.Body.Close()

	statusStr := fmt.Sprintf("%d", resp.StatusCode)
	metrics.CrawlerURLsFetched.WithLabelValues("GET", statusStr, domain).Inc()

	if resp.StatusCode != http.StatusOK {
		return
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	doc, err := crawler.ExtractHTMLToMarkdownWithBase(bodyBytes, targetURL)
	if err != nil {
		return
	}

	// Report discovered outlinks to master for deduplication and frontier enqueue
	if len(doc.Outlinks) > 0 {
		go w.reportOutlinks(ctx, doc.Outlinks)
	}

	w.sendResult(ctx, CrawlResult{
		SourceURL: targetURL,
		Markdown:  doc.Markdown,
		Outlinks:  doc.Outlinks,
	})
}

// sendHeartbeats posts to the master at the configured interval.
func (w *WorkerNode) sendHeartbeats(ctx context.Context) {
	interval := w.Heartbeat
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hbURL := fmt.Sprintf("%s/heartbeat?worker_id=%s", w.MasterURL, w.ID)
			req, err := http.NewRequestWithContext(ctx, "POST", hbURL, nil)
			if err != nil {
				continue
			}
			if resp, err := w.HTTPClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
	}
}

// pollForURLs fetches URL assignments from the master's /assign endpoint
// and feeds them into the local job queue.
func (w *WorkerNode) pollForURLs(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			assignURL := fmt.Sprintf("%s/assign?worker_id=%s", w.MasterURL, w.ID)
			req, err := http.NewRequestWithContext(ctx, "GET", assignURL, nil)
			if err != nil {
				continue
			}
			resp, err := w.HTTPClient.Do(req)
			if err != nil {
				continue
			}
			var urls []string
			_ = json.NewDecoder(resp.Body).Decode(&urls)
			resp.Body.Close()

			for _, u := range urls {
				select {
				case w.JobQueue <- u:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// reportOutlinks sends discovered outlinks to the master for deduplication.
func (w *WorkerNode) reportOutlinks(ctx context.Context, links []string) {
	data, err := json.Marshal(links)
	if err != nil {
		return
	}
	discoverURL := fmt.Sprintf("%s/discover", w.MasterURL)
	req, err := http.NewRequestWithContext(ctx, "POST", discoverURL, bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if resp, err := w.HTTPClient.Do(req); err == nil {
		resp.Body.Close()
	}
}
