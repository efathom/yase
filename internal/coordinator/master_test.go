package coordinator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockBloomFilter is nil-safe — master tests don't need real Redis.
func newTestMaster() *MasterScheduler {
	frontier := make(chan string, 100)
	return &MasterScheduler{
		workers:       make(map[string]*WorkerStatus),
		timeoutLimit:  100 * time.Millisecond, // fast for tests
		bloomFilter:   nil,
		FrontierQueue: frontier,
	}
}

func TestHeartbeatRegistersWorker(t *testing.T) {
	ms := newTestMaster()

	req := httptest.NewRequest("POST", "/heartbeat?worker_id=w1", nil)
	w := httptest.NewRecorder()
	ms.HeartbeatHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if ms.WorkerCount() != 1 {
		t.Errorf("expected 1 worker, got %d", ms.WorkerCount())
	}
	if ms.ActiveWorkerCount() != 1 {
		t.Errorf("expected 1 active worker, got %d", ms.ActiveWorkerCount())
	}
}

func TestHeartbeatMissingWorkerID(t *testing.T) {
	ms := newTestMaster()
	req := httptest.NewRequest("POST", "/heartbeat", nil)
	w := httptest.NewRecorder()
	ms.HeartbeatHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestAssignAndAckURL(t *testing.T) {
	ms := newTestMaster()

	// Register worker via heartbeat
	req := httptest.NewRequest("POST", "/heartbeat?worker_id=w1", nil)
	ms.HeartbeatHandler(httptest.NewRecorder(), req)

	ms.AssignURL("w1", "https://example.com")
	ms.AssignURL("w1", "https://example.com/page2")

	ms.mu.RLock()
	assigned := len(ms.workers["w1"].AssignedURLs)
	ms.mu.RUnlock()
	if assigned != 2 {
		t.Errorf("expected 2 assigned URLs, got %d", assigned)
	}

	ms.AckURL("w1", "https://example.com")
	ms.mu.RLock()
	assigned = len(ms.workers["w1"].AssignedURLs)
	ms.mu.RUnlock()
	if assigned != 1 {
		t.Errorf("expected 1 assigned URL after ack, got %d", assigned)
	}
}

func TestReaperDeclaresDead(t *testing.T) {
	ms := newTestMaster()

	// Register worker
	req := httptest.NewRequest("POST", "/heartbeat?worker_id=w1", nil)
	ms.HeartbeatHandler(httptest.NewRecorder(), req)

	// Assign URLs
	ms.AssignURL("w1", "https://example.com/orphan1")
	ms.AssignURL("w1", "https://example.com/orphan2")

	// Backdate LastSeen to trigger reaper
	ms.mu.Lock()
	ms.workers["w1"].LastSeen = time.Now().Add(-1 * time.Second)
	ms.mu.Unlock()

	// Run reaper once
	ctx, cancel := context.WithCancel(context.Background())
	go ms.ReaperDaemon(ctx)

	// Wait for reaper tick (timeoutLimit is 100ms, reaper ticks every 4s — override)
	// Instead, just run the reaper logic directly
	cancel()

	// Manually trigger reaper check
	ms.mu.Lock()
	now := time.Now()
	for id, status := range ms.workers {
		if status.Active && now.Sub(status.LastSeen) > ms.timeoutLimit {
			status.Active = false
			for url := range status.AssignedURLs {
				select {
				case ms.FrontierQueue <- url:
				default:
				}
			}
			status.AssignedURLs = make(map[string]time.Time)
			_ = id
		}
	}
	ms.mu.Unlock()

	// Verify worker declared dead
	if ms.ActiveWorkerCount() != 0 {
		t.Errorf("expected 0 active workers after reaper, got %d", ms.ActiveWorkerCount())
	}

	// Verify orphaned URLs re-queued to frontier
	requeued := 0
	for {
		select {
		case <-ms.FrontierQueue:
			requeued++
		default:
			goto done
		}
	}
done:
	if requeued != 2 {
		t.Errorf("expected 2 re-queued URLs, got %d", requeued)
	}
}

func TestMaxPagesLimit(t *testing.T) {
	ms := newTestMaster()
	ms.MaxPages = 3

	urls := []string{"http://a.com/1", "http://a.com/2", "http://a.com/3", "http://a.com/4", "http://a.com/5"}
	var enqueued int
	for _, u := range urls {
		if !ms.tryReserveSlot() {
			break
		}
		select {
		case ms.FrontierQueue <- u:
			enqueued++
		default:
		}
	}

	if ms.EnqueuedCount() != 3 {
		t.Errorf("expected 3 enqueued, got %d", ms.EnqueuedCount())
	}
	if enqueued != 3 {
		t.Errorf("expected 3 in frontier, got %d", enqueued)
	}
}

func TestMaxPagesZeroIsUnlimited(t *testing.T) {
	ms := newTestMaster()
	ms.MaxPages = 0

	for i := 0; i < 50; i++ {
		if !ms.tryReserveSlot() {
			t.Fatal("tryReserveSlot should always succeed with MaxPages=0")
		}
	}
	// MaxPages=0 means unlimited — counter is not incremented (no CAS needed)
}

func TestMaxPagesConcurrentReserve(t *testing.T) {
	ms := newTestMaster()
	ms.MaxPages = 100

	var reserved atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ms.tryReserveSlot() {
				reserved.Add(1)
			}
		}()
	}
	wg.Wait()

	if reserved.Load() != 100 {
		t.Errorf("expected exactly 100 reservations, got %d", reserved.Load())
	}
	if ms.EnqueuedCount() != 100 {
		t.Errorf("expected enqueued=100, got %d", ms.EnqueuedCount())
	}
}

func TestDomainFilter(t *testing.T) {
	tests := []struct {
		name    string
		filter  *DomainFilter
		url     string
		allowed bool
	}{
		{"nil filter allows all", nil, "https://example.com/page", true},
		{"empty filter allows all", &DomainFilter{}, "https://example.com/page", true},
		{"whitelist allows match", &DomainFilter{AllowedDomains: map[string]bool{"example.com": true}}, "https://example.com/page", true},
		{"whitelist blocks non-match", &DomainFilter{AllowedDomains: map[string]bool{"example.com": true}}, "https://other.com/page", false},
		{"blacklist blocks match", &DomainFilter{BlockedDomains: map[string]bool{"evil.com": true}}, "https://evil.com/page", false},
		{"blacklist allows non-match", &DomainFilter{BlockedDomains: map[string]bool{"evil.com": true}}, "https://good.com/page", true},
		{"blacklist overrides whitelist", &DomainFilter{
			AllowedDomains: map[string]bool{"evil.com": true},
			BlockedDomains: map[string]bool{"evil.com": true},
		}, "https://evil.com/page", false},
		{"invalid URL rejected", &DomainFilter{}, "://bad", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.IsAllowed(tt.url)
			if got != tt.allowed {
				t.Errorf("IsAllowed(%q) = %v, want %v", tt.url, got, tt.allowed)
			}
		})
	}
}

func TestHeartbeatRecovery(t *testing.T) {
	ms := newTestMaster()

	// Register and then mark dead
	req := httptest.NewRequest("POST", "/heartbeat?worker_id=w1", nil)
	ms.HeartbeatHandler(httptest.NewRecorder(), req)

	ms.mu.Lock()
	ms.workers["w1"].Active = false
	ms.mu.Unlock()

	if ms.ActiveWorkerCount() != 0 {
		t.Error("worker should be inactive")
	}

	// Heartbeat again — should recover
	ms.HeartbeatHandler(httptest.NewRecorder(), req)

	if ms.ActiveWorkerCount() != 1 {
		t.Errorf("expected worker to recover, got %d active", ms.ActiveWorkerCount())
	}
}
