package crawler

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// dnsEntry holds a cached DNS resolution with an expiration time.
type dnsEntry struct {
	ip        string
	expiresAt time.Time
}

// DNSCache provides thread-safe DNS resolution caching with TTL-based expiry
// to avoid repeated lookups for frequently crawled hosts.
type DNSCache struct {
	cache  sync.Map
	ttl    time.Duration
	stopCh chan struct{}
	once   sync.Once
	dialer net.Dialer
}

// NewDNSCache creates a DNS cache with the given TTL per entry.
// Starts a background sweeper that removes expired entries.
func NewDNSCache(ttl time.Duration) *DNSCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	d := &DNSCache{
		ttl:    ttl,
		stopCh: make(chan struct{}),
		dialer: net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second},
	}
	go d.sweeper()
	return d
}

// sweeper periodically removes expired DNS entries.
func (d *DNSCache) sweeper() {
	ticker := time.NewTicker(d.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			d.cache.Range(func(key, value any) bool {
				if entry, ok := value.(dnsEntry); ok && now.After(entry.expiresAt) {
					d.cache.Delete(key)
				}
				return true
			})
		case <-d.stopCh:
			return
		}
	}
}

// Close stops the background sweeper goroutine.
func (d *DNSCache) Close() {
	d.once.Do(func() { close(d.stopCh) })
}

// customDialContext resolves hostnames through the cache before dialing.
// On cache miss or expired entry, performs an upstream DNS query and stores the result.
func (d *DNSCache) customDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	if cached, ok := d.cache.Load(host); ok {
		entry := cached.(dnsEntry)
		if time.Now().Before(entry.expiresAt) {
			addr = net.JoinHostPort(entry.ip, port)
		} else {
			d.cache.Delete(host)
			// Fall through to re-resolve
			addr = d.resolve(ctx, host, port, addr)
		}
	} else {
		addr = d.resolve(ctx, host, port, addr)
	}

	return d.dialer.DialContext(ctx, network, addr)
}

// resolve performs a DNS lookup and caches the result.
func (d *DNSCache) resolve(ctx context.Context, host, port, fallbackAddr string) string {
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err == nil && len(ips) > 0 {
		ipStr := ips[0].String()
		d.cache.Store(host, dnsEntry{ip: ipStr, expiresAt: time.Now().Add(d.ttl)})
		return net.JoinHostPort(ipStr, port)
	}
	return fallbackAddr
}

// NewTunedCrawlerClient returns an http.Client configured for planetary-scale
// web crawling with tuned connection pooling, timeouts, and DNS caching.
func NewTunedCrawlerClient(dnsTTL time.Duration) (*http.Client, *DNSCache) {
	dns := NewDNSCache(dnsTTL)

	transport := &http.Transport{
		DialContext:           dns.customDialContext,
		MaxIdleConns:          2000,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 0,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}, dns
}
