// Package cache provides LRU caching for search results to avoid
// redundant embedding + search pipeline execution for repeated queries.
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/metrics"
)

// SearchCache is a thread-safe LRU cache for search results.
type SearchCache struct {
	mu           sync.Mutex
	collectionID string
	cache        map[string]*list.Element
	eviction     *list.List
	maxSize      int
	ttl          time.Duration
}

type searchCacheEntry struct {
	key       string
	results   []index.ScoredResult
	createdAt time.Time
}

// NewSearchCache creates a search result cache scoped to a collection ID.
// The collection ID is part of the cache key so collections never share results.
func NewSearchCache(collectionID string, maxSize int, ttl time.Duration) *SearchCache {
	if maxSize <= 0 {
		maxSize = 10000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &SearchCache{
		collectionID: collectionID,
		cache:        make(map[string]*list.Element, maxSize),
		eviction:     list.New(),
		maxSize:      maxSize,
		ttl:          ttl,
	}
}

// Get returns cached results for a query, or nil if not found/expired.
func (c *SearchCache) Get(query string, filters map[string]string, topK int) []index.ScoredResult {
	key := c.cacheKey(query, filters, topK)

	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.cache[key]
	if !ok {
		metrics.SearchCacheMisses.Inc()
		return nil
	}

	entry := el.Value.(*searchCacheEntry)
	if time.Since(entry.createdAt) > c.ttl {
		c.eviction.Remove(el)
		delete(c.cache, key)
		metrics.SearchCacheMisses.Inc()
		return nil
	}

	c.eviction.MoveToFront(el)
	metrics.SearchCacheHits.Inc()
	return append([]index.ScoredResult(nil), entry.results...)
}

// Put stores search results in the cache.
func (c *SearchCache) Put(query string, filters map[string]string, topK int, results []index.ScoredResult) {
	key := c.cacheKey(query, filters, topK)

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.cache[key]; ok {
		c.eviction.Remove(el)
		delete(c.cache, key)
	}

	if c.eviction.Len() >= c.maxSize {
		oldest := c.eviction.Back()
		if oldest != nil {
			c.eviction.Remove(oldest)
			delete(c.cache, oldest.Value.(*searchCacheEntry).key)
		}
	}

	entry := &searchCacheEntry{
		key:       key,
		results:   append([]index.ScoredResult(nil), results...),
		createdAt: time.Now(),
	}
	el := c.eviction.PushFront(entry)
	c.cache[key] = el
}

// Len returns the current number of cached entries.
func (c *SearchCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// Invalidate removes all cached entries (e.g., after document delete/update).
func (c *SearchCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*list.Element, c.maxSize)
	c.eviction.Init()
}

func (c *SearchCache) cacheKey(query string, filters map[string]string, topK int) string {
	// Deterministic key from collection + query + sorted filters + topK.
	//
	// Each part is length-prefixed rather than joined on a separator: a filter
	// value containing the separator could otherwise reproduce a different
	// key's serialization and read back another entry's results.
	h := sha256.New()
	writePart := func(s string) {
		_, _ = fmt.Fprintf(h, "%d:%s", len(s), s)
	}

	writePart(c.collectionID)
	writePart(query)
	writePart(strconv.Itoa(topK))

	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	writePart(strconv.Itoa(len(keys)))
	for _, k := range keys {
		writePart(k)
		writePart(filters[k])
	}

	return hex.EncodeToString(h.Sum(nil)[:16])
}
