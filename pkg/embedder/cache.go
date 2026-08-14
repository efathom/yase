package embedder

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"

	"github.com/efathom/yase/pkg/metrics"
)

// CachedEmbedder wraps an Embedder with an LRU cache to avoid redundant
// API calls for repeated chunks. Thread-safe via sync.Mutex.
type CachedEmbedder struct {
	inner    Embedder
	mu       sync.Mutex
	cache    map[uint64]*list.Element
	eviction *list.List
	maxSize  int
}

type cacheEntry struct {
	key  uint64
	text string // stored for hash collision detection
	vec  []float32
}

// NewCachedEmbedder creates a caching wrapper around the given embedder.
// maxSize is the maximum number of entries to cache.
func NewCachedEmbedder(inner Embedder, maxSize int) *CachedEmbedder {
	if maxSize <= 0 {
		maxSize = 100000
	}
	return &CachedEmbedder{
		inner:    inner,
		cache:    make(map[uint64]*list.Element, maxSize),
		eviction: list.New(),
		maxSize:  maxSize,
	}
}

// Embed returns a cached vector or delegates to the inner embedder.
func (c *CachedEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	key := hashText(text)

	c.mu.Lock()
	if el, ok := c.cache[key]; ok {
		entry := el.Value.(*cacheEntry)
		if entry.text == text {
			c.eviction.MoveToFront(el)
			vec := copyVec(entry.vec)
			c.mu.Unlock()
			metrics.EmbedderCacheHits.Inc()
			return vec, nil
		}
		// Hash collision — fall through to inner embedder
	}
	c.mu.Unlock()

	vec, err := c.inner.Embed(ctx, text)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	// Double-check after acquiring lock
	if el, ok := c.cache[key]; ok {
		entry := el.Value.(*cacheEntry)
		if entry.text == text {
			c.eviction.MoveToFront(el)
			c.mu.Unlock()
			metrics.EmbedderCacheHits.Inc()
			return copyVec(entry.vec), nil
		}
	}
	c.put(key, text, vec)
	c.mu.Unlock()
	metrics.EmbedderCacheMisses.Inc()
	return vec, nil
}

// copyVec returns a shallow copy so callers cannot mutate the cached vector.
func copyVec(v []float32) []float32 {
	return append([]float32(nil), v...)
}

// EmbedBatch returns cached vectors where available and delegates misses.
func (c *CachedEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	var missIndices []int
	var missTexts []string
	var missKeys []uint64

	c.mu.Lock()
	for i, t := range texts {
		key := hashText(t)
		if el, ok := c.cache[key]; ok {
			entry := el.Value.(*cacheEntry)
			if entry.text == t {
				c.eviction.MoveToFront(el)
				result[i] = copyVec(entry.vec)
				metrics.EmbedderCacheHits.Inc()
				continue
			}
		}
		missIndices = append(missIndices, i)
		missTexts = append(missTexts, t)
		missKeys = append(missKeys, key)
	}
	c.mu.Unlock()

	if len(missTexts) == 0 {
		return result, nil
	}
	metrics.EmbedderCacheMisses.Add(float64(len(missTexts)))

	vecs, err := c.inner.EmbedBatch(ctx, missTexts)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	for j, idx := range missIndices {
		result[idx] = vecs[j]
		c.put(missKeys[j], missTexts[j], vecs[j])
	}
	c.mu.Unlock()

	return result, nil
}

// Dimension returns the inner embedder's dimension.
func (c *CachedEmbedder) Dimension() int {
	return c.inner.Dimension()
}

// Len returns the current number of cached entries.
func (c *CachedEmbedder) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

func (c *CachedEmbedder) put(key uint64, text string, vec []float32) {
	// If an entry exists at this key (hash collision or same text), remove it first
	if existing, ok := c.cache[key]; ok {
		c.eviction.Remove(existing)
		delete(c.cache, key)
	}
	// Evict oldest if at capacity
	if c.eviction.Len() >= c.maxSize {
		oldest := c.eviction.Back()
		if oldest != nil {
			c.eviction.Remove(oldest)
			delete(c.cache, oldest.Value.(*cacheEntry).key)
		}
	}
	entry := &cacheEntry{key: key, text: text, vec: vec}
	el := c.eviction.PushFront(entry)
	c.cache[key] = el
}

func hashText(text string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(text))
	return h.Sum64()
}
