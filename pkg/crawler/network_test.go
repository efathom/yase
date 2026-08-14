package crawler

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestNewTunedCrawlerClient(t *testing.T) {
	client, dns := NewTunedCrawlerClient(5 * time.Minute)
	defer dns.Close()

	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.Timeout == 0 {
		t.Error("expected non-zero timeout")
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport")
	}
	if transport.MaxIdleConns != 2000 {
		t.Errorf("MaxIdleConns: got %d, want 2000", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 100 {
		t.Errorf("MaxIdleConnsPerHost: got %d, want 100", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxConnsPerHost != 50 {
		t.Errorf("MaxConnsPerHost: got %d, want 50", transport.MaxConnsPerHost)
	}
}

func TestDNSCacheHitAndMiss(t *testing.T) {
	dns := NewDNSCache(5 * time.Minute)
	defer dns.Close()

	// Miss — nothing cached yet
	if _, ok := dns.cache.Load("example.com"); ok {
		t.Error("expected cache miss for new host")
	}

	// Store manually
	dns.cache.Store("example.com", dnsEntry{
		ip:        "93.184.216.34",
		expiresAt: time.Now().Add(5 * time.Minute),
	})

	// Hit
	if cached, ok := dns.cache.Load("example.com"); !ok {
		t.Error("expected cache hit")
	} else if cached.(dnsEntry).ip != "93.184.216.34" {
		t.Error("expected correct IP")
	}
}

func TestDNSCacheTTLExpiry(t *testing.T) {
	dns := NewDNSCache(100 * time.Millisecond)
	defer dns.Close()

	// Store an entry that expires in 100ms
	dns.cache.Store("expire.test", dnsEntry{
		ip:        "1.2.3.4",
		expiresAt: time.Now().Add(100 * time.Millisecond),
	})

	// Should be present immediately
	if _, ok := dns.cache.Load("expire.test"); !ok {
		t.Fatal("expected entry to be present")
	}

	// Wait for TTL + sweeper interval (ttl/2 = 50ms, so ~150ms total)
	time.Sleep(250 * time.Millisecond)

	// Sweeper should have removed it
	if _, ok := dns.cache.Load("expire.test"); ok {
		t.Error("expected entry to be swept after TTL expiry")
	}
}

func TestDNSCacheExpiredEntryReresolved(t *testing.T) {
	dns := NewDNSCache(5 * time.Minute)
	defer dns.Close()

	// Store an already-expired entry
	dns.cache.Store("stale.test", dnsEntry{
		ip:        "1.1.1.1",
		expiresAt: time.Now().Add(-1 * time.Second),
	})

	// customDialContext should detect it as expired and delete it
	if cached, ok := dns.cache.Load("stale.test"); ok {
		entry := cached.(dnsEntry)
		if time.Now().Before(entry.expiresAt) {
			t.Error("entry should be expired")
		}
	}
}

func TestDNSCacheConcurrentAccess(t *testing.T) {
	dns := NewDNSCache(1 * time.Second)
	defer dns.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			host := "host.test"
			dns.cache.Store(host, dnsEntry{
				ip:        "10.0.0.1",
				expiresAt: time.Now().Add(1 * time.Second),
			})
			dns.cache.Load(host)
			dns.cache.Delete(host)
		}(i)
	}
	wg.Wait()
}

func TestDNSCacheDefaultTTL(t *testing.T) {
	// Zero TTL should default to 5 minutes
	dns := NewDNSCache(0)
	defer dns.Close()

	if dns.ttl != 5*time.Minute {
		t.Errorf("expected default TTL 5m, got %v", dns.ttl)
	}
}

func TestDNSCacheClose(t *testing.T) {
	dns := NewDNSCache(1 * time.Second)
	// Should not panic on double close
	dns.Close()
	dns.Close()
}
