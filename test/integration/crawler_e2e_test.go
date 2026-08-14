package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/efathom/yase/internal/coordinator"
	"github.com/efathom/yase/pkg/crawler"
	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// TestCrawlerDomainFilterE2E verifies the full master-worker crawl loop:
//  1. A local HTTP server serves a small site with inter-linked pages and an
//     external link to an off-domain page.
//  2. The master's DomainFilter whitelists only the test server's hostname.
//  3. The worker crawls, discovers outlinks, and reports them back.
//  4. We assert all on-domain pages are crawled and no off-domain pages are.
func TestCrawlerDomainFilterE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── Fake website ──
	// Page content uses absolute URLs, injected after the server starts.
	var pages sync.Map // path → HTML content

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		val, ok := pages.Load(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, val.(string))
	})

	site := httptest.NewServer(mux)
	defer site.Close()

	// Populate page content with absolute URLs
	//   /              → links to /about, /docs, https://external.com/evil
	//   /about         → links to /docs
	//   /docs          → links to /docs/install
	//   /docs/install  → leaf page
	base := site.URL
	pages.Store("/", fmt.Sprintf(`<html><body>
		<h1>Welcome to YASE</h1>
		<p>YASE is a high-performance hybrid search engine.</p>
		<a href="%s/about">About</a>
		<a href="%s/docs">Docs</a>
		<a href="https://external.com/should-not-crawl">External</a>
	</body></html>`, base, base))

	pages.Store("/about", fmt.Sprintf(`<html><body>
		<h1>About YASE</h1>
		<p>Built with Go for speed and reliability.</p>
		<a href="%s/docs">Documentation</a>
	</body></html>`, base))

	pages.Store("/docs", fmt.Sprintf(`<html><body>
		<h1>Documentation</h1>
		<p>Learn how to use YASE for semantic search.</p>
		<a href="%s/docs/install">Installation Guide</a>
	</body></html>`, base))

	pages.Store("/docs/install", `<html><body>
		<h1>Installation</h1>
		<p>Run go install to get started with YASE.</p>
	</body></html>`)

	// ── Start Redis (for bloom filter) ──
	redisContainer, err := tcredis.Run(ctx, "redis/redis-stack-server:latest")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	redisURL, _ := redisContainer.ConnectionString(ctx)
	redisOpts, _ := goredis.ParseURL(redisURL)
	rdb := goredis.NewClient(redisOpts)
	defer rdb.Close()

	for i := 0; i < 30; i++ {
		if rdb.Ping(ctx).Err() == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// ── Master with domain whitelist ──
	bloom := crawler.NewBloomFilter(rdb, "crawler:e2e-test")
	frontier := make(chan string, 100)

	filter := &coordinator.DomainFilter{
		AllowedDomains: map[string]bool{
			"127.0.0.1": true,
		},
	}

	master := coordinator.NewMasterScheduler(bloom, frontier, filter)

	// Start master HTTP API
	masterMux := http.NewServeMux()
	masterMux.HandleFunc("/heartbeat", master.HeartbeatHandler)
	masterMux.HandleFunc("/assign", master.AssignHandler)
	masterMux.HandleFunc("/seed", master.SeedHandler)
	masterMux.HandleFunc("/discover", master.DiscoverHandler)
	masterServer := httptest.NewServer(masterMux)
	defer masterServer.Close()

	go master.ReaperDaemon(ctx)

	// ── Worker with relaxed rate limit for testing ──
	rl := crawler.NewGlobalRateLimiter(rdb)
	jobs := make(chan string, 100)
	results := make(chan coordinator.CrawlResult, 100)

	worker := coordinator.NewWorkerNode("e2e-worker", masterServer.URL, rl, jobs, results)
	worker.RateLimit = 10 // 10 requests per second — fast for tests
	worker.RateWindow = time.Second
	worker.Start(ctx, 4)

	// ── Seed the root URL ──
	master.ProcessDiscoveredLinks(ctx, []string{base + "/"})

	// ── Collect results with timeout ──
	crawled := make(map[string]string)
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		for result := range results {
			if result.Error != nil {
				t.Logf("Crawl error: %v", result.Error)
				continue
			}
			mu.Lock()
			crawled[result.SourceURL] = result.Markdown
			mu.Unlock()
			t.Logf("Crawled: %s (%d bytes, %d outlinks)",
				result.SourceURL, len(result.Markdown), len(result.Outlinks))
		}
		close(done)
	}()

	// Wait for all 4 pages to be crawled
	deadline := time.After(30 * time.Second)
	for {
		mu.Lock()
		count := len(crawled)
		mu.Unlock()
		if count >= 4 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timed out: crawled %d/4 pages: %v", len(crawled), mapKeys(crawled))
			mu.Unlock()
		case <-time.After(500 * time.Millisecond):
		}
	}

	cancel()
	<-done

	// ── Assertions ──
	expectedPages := []string{
		base + "/",
		base + "/about",
		base + "/docs",
		base + "/docs/install",
	}

	for _, page := range expectedPages {
		mu.Lock()
		content, ok := crawled[page]
		mu.Unlock()
		if !ok {
			t.Errorf("page %s was NOT crawled. Got: %v", page, mapKeys(crawled))
			continue
		}
		if content == "" {
			t.Errorf("page %s has empty markdown content", page)
		}
	}

	// Verify external.com was NOT crawled
	for u := range crawled {
		if u == "https://external.com/should-not-crawl" {
			t.Error("external domain was crawled despite whitelist — domain filter broken")
		}
	}

	t.Logf("Crawled %d pages within whitelisted domain", len(crawled))
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
