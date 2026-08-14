package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/efathom/yase/pkg/crawler"
)

// WorkerStatus tracks a worker's liveness and inflight tasks.
type WorkerStatus struct {
	LastSeen     time.Time
	Active       bool
	AssignedURLs map[string]time.Time // URL → assignment time
}

// DomainFilter controls which domains the crawler is allowed to visit.
// If AllowedDomains is non-empty, only those domains are crawled (whitelist).
// BlockedDomains are always rejected (blacklist, checked first).
// Supports loading domains from files (one domain per line) with hot-reload on SIGHUP.
type DomainFilter struct {
	mu             sync.RWMutex
	AllowedDomains map[string]bool // whitelist (empty = allow all)
	BlockedDomains map[string]bool // blacklist
	allowedFile    string          // path for hot-reload
	blockedFile    string          // path for hot-reload
}

// IsAllowed returns true if the URL's host passes the domain filter.
// Matches on hostname (without port) so "example.com:8080" matches a
// filter entry for "example.com".
func (f *DomainFilter) IsAllowed(rawURL string) bool {
	if f == nil {
		return true
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := parsed.Hostname()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.BlockedDomains[host] {
		return false
	}
	if len(f.AllowedDomains) > 0 {
		return f.AllowedDomains[host]
	}
	return true
}

// LoadDomainsFromFile reads domains from a text file (one domain per line).
// Empty lines and lines starting with # are ignored.
func LoadDomainsFromFile(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read domain file %s: %w", path, err)
	}

	var domains []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		domains = append(domains, line)
	}
	return domains, nil
}

// Reload re-reads domain lists from the configured files.
// Call on SIGHUP for hot-reload without restart.
func (f *DomainFilter) Reload() error {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.allowedFile != "" {
		domains, err := LoadDomainsFromFile(f.allowedFile)
		if err != nil {
			return fmt.Errorf("reload allowed domains: %w", err)
		}
		f.AllowedDomains = make(map[string]bool, len(domains))
		for _, d := range domains {
			f.AllowedDomains[d] = true
		}
		slog.Info("domain-filter: reloaded allowed domains", "count", len(domains), "file", f.allowedFile)
	}

	if f.blockedFile != "" {
		domains, err := LoadDomainsFromFile(f.blockedFile)
		if err != nil {
			return fmt.Errorf("reload blocked domains: %w", err)
		}
		f.BlockedDomains = make(map[string]bool, len(domains))
		for _, d := range domains {
			f.BlockedDomains[d] = true
		}
		slog.Info("domain-filter: reloaded blocked domains", "count", len(domains), "file", f.blockedFile)
	}

	return nil
}

// SetFiles stores file paths for hot-reload support.
func (f *DomainFilter) SetFiles(allowedFile, blockedFile string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowedFile = allowedFile
	f.blockedFile = blockedFile
}

// MasterScheduler manages the crawler fleet: worker registration, heartbeat
// monitoring, dead-worker detection, and URL deduplication via Bloom filter.
type MasterScheduler struct {
	workers       map[string]*WorkerStatus
	mu            sync.RWMutex
	timeoutLimit  time.Duration
	bloomFilter   *crawler.DistributedBloomFilter
	FrontierQueue chan string
	DomainFilter  *DomainFilter
	MaxPages      int          // 0 = unlimited
	enqueued      atomic.Int64 // total URLs enqueued to frontier
}

// NewMasterScheduler creates a master scheduler with the given Bloom filter
// and frontier channel. An optional DomainFilter restricts which URLs enter
// the frontier.
func NewMasterScheduler(bf *crawler.DistributedBloomFilter, frontier chan string, filter *DomainFilter) *MasterScheduler {
	return &MasterScheduler{
		workers:       make(map[string]*WorkerStatus),
		timeoutLimit:  9 * time.Second, // 3x the 3s heartbeat interval
		bloomFilter:   bf,
		FrontierQueue: frontier,
		DomainFilter:  filter,
	}
}

// tryReserveSlot atomically reserves one enqueue slot. Returns true if
// a slot was available (caller should enqueue), false if the limit is reached.
// When MaxPages is 0 (unlimited), always returns true.
func (ms *MasterScheduler) tryReserveSlot() bool {
	if ms.MaxPages <= 0 {
		return true
	}
	for {
		cur := ms.enqueued.Load()
		if cur >= int64(ms.MaxPages) {
			return false
		}
		if ms.enqueued.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// releaseSlot returns a reserved page slot (e.g., when a URL is dropped
// because the frontier queue is full) so MaxPages is not under-delivered.
func (ms *MasterScheduler) releaseSlot() {
	if ms.MaxPages <= 0 {
		return
	}
	ms.enqueued.Add(-1)
}

// EnqueuedCount returns the total number of URLs enqueued so far.
func (ms *MasterScheduler) EnqueuedCount() int64 {
	return ms.enqueued.Load()
}

// HeartbeatHandler is an HTTP endpoint for workers to report liveness.
func (ms *MasterScheduler) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker_id")
	if workerID == "" {
		http.Error(w, "missing worker_id", http.StatusBadRequest)
		return
	}

	ms.mu.Lock()
	ws, exists := ms.workers[workerID]
	if !exists || !ws.Active {
		slog.Info("master: worker registered/recovered", "worker_id", workerID)
		ms.workers[workerID] = &WorkerStatus{
			Active:       true,
			LastSeen:     time.Now(),
			AssignedURLs: make(map[string]time.Time),
		}
	} else {
		ws.LastSeen = time.Now()
	}
	ms.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

// AssignURL registers an inflight URL for a worker before dispatching.
// The worker is auto-registered if it has not yet heartbeated, so its
// assignments are always tracked by the reaper.
func (ms *MasterScheduler) AssignURL(workerID, url string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	w, exists := ms.workers[workerID]
	if !exists {
		w = &WorkerStatus{
			Active:       true,
			LastSeen:     time.Now(),
			AssignedURLs: make(map[string]time.Time),
		}
		ms.workers[workerID] = w
	}
	w.AssignedURLs[url] = time.Now()
}

// AckURL removes a URL from a worker's inflight set after successful processing.
func (ms *MasterScheduler) AckURL(workerID, url string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if w, exists := ms.workers[workerID]; exists {
		delete(w.AssignedURLs, url)
	}
}

// ReaperDaemon sweeps for dead workers every 4 seconds and re-queues their
// orphaned URLs to the frontier. Workers that miss heartbeats for >9 seconds
// are declared dead.
func (ms *MasterScheduler) ReaperDaemon(ctx context.Context) {
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ms.mu.Lock()
			now := time.Now()
			for id, status := range ms.workers {
				if status.Active && now.Sub(status.LastSeen) > ms.timeoutLimit {
					slog.Error("master: worker dead, re-queuing orphaned URLs", "worker_id", id, "orphaned", len(status.AssignedURLs))
					status.Active = false

					for url := range status.AssignedURLs {
						select {
						case ms.FrontierQueue <- url:
						default:
							slog.Warn("master: frontier saturated, dropped orphaned URL", "url", url)
						}
					}
					// Clear to prevent duplicate re-queuing on next tick
					status.AssignedURLs = make(map[string]time.Time)
				}
			}
			ms.mu.Unlock()
		}
	}
}

// ProcessDiscoveredLinks deduplicates incoming URLs via the Bloom filter and
// pipes new ones to the frontier queue.
func (ms *MasterScheduler) ProcessDiscoveredLinks(ctx context.Context, newLinks []string) {
	for _, link := range newLinks {
		if !ms.DomainFilter.IsAllowed(link) {
			continue
		}
		isNew, err := ms.bloomFilter.CheckAndAdd(ctx, link)
		if err == nil && isNew {
			if !ms.tryReserveSlot() {
				return
			}
			select {
			case ms.FrontierQueue <- link:
			default:
				ms.releaseSlot()
				slog.Warn("master: frontier full, dropping link", "url", link)
			}
		}
	}
}

// AssignHandler is an HTTP endpoint workers poll to receive URLs from the frontier.
// Returns a JSON array of up to 10 URLs, or an empty array if none available.
func (ms *MasterScheduler) AssignHandler(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker_id")
	if workerID == "" {
		http.Error(w, "missing worker_id", http.StatusBadRequest)
		return
	}

	var urls []string
	for i := 0; i < 10; i++ {
		select {
		case u := <-ms.FrontierQueue:
			urls = append(urls, u)
			ms.AssignURL(workerID, u)
		default:
			// No more URLs available right now
			goto done
		}
	}
done:
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(urls)
}

// SeedHandler accepts POST requests with a JSON array of seed URLs,
// deduplicates them via the Bloom filter, and enqueues new ones to the frontier.
func (ms *MasterScheduler) SeedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var urls []string
	if err := json.NewDecoder(r.Body).Decode(&urls); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	var added int
	for _, u := range urls {
		if !ms.DomainFilter.IsAllowed(u) {
			slog.Warn("master: seed URL blocked by domain filter", "url", u)
			continue
		}
		isNew, err := ms.bloomFilter.CheckAndAdd(ctx, u)
		if err != nil {
			slog.Error("master: bloom filter error", "url", u, "error", err)
			continue
		}
		if isNew {
			if !ms.tryReserveSlot() {
				slog.Warn("master: MaxPages limit reached, ignoring remaining seeds", "max_pages", ms.MaxPages)
				break
			}
			select {
			case ms.FrontierQueue <- u:
				added++
			default:
				ms.releaseSlot()
				slog.Warn("master: frontier full, dropped", "url", u)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"seeded":%d,"total":%d}`, added, len(urls))
}

// DiscoverHandler accepts POST with discovered outlinks from a worker,
// deduplicates via Bloom filter, and enqueues new ones.
func (ms *MasterScheduler) DiscoverHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var links []string
	if err := json.NewDecoder(r.Body).Decode(&links); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	ms.ProcessDiscoveredLinks(r.Context(), links)
	w.WriteHeader(http.StatusOK)
}

// WorkerCount returns the number of registered workers.
func (ms *MasterScheduler) WorkerCount() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.workers)
}

// ActiveWorkerCount returns the number of active workers.
func (ms *MasterScheduler) ActiveWorkerCount() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	count := 0
	for _, ws := range ms.workers {
		if ws.Active {
			count++
		}
	}
	return count
}
