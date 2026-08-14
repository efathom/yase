package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// cacheEntry tracks a cached segment on local disk.
type cacheEntry struct {
	localPath  string
	size       int64
	lastAccess time.Time
}

// LocalCache wraps an IndexStore with local disk caching.
// Segments are downloaded on first access and cached until evicted.
type LocalCache struct {
	remote   IndexStore
	cacheDir string
	maxSize  int64 // max total cache size in bytes (0 = unlimited)
	mu       sync.Mutex
	entries  map[string]*cacheEntry
	usedSize int64
}

// NewLocalCache creates a cache backed by the given remote store.
func NewLocalCache(remote IndexStore, cacheDir string, maxSize int64) (*LocalCache, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache dir: %w", err)
	}
	return &LocalCache{
		remote:   remote,
		cacheDir: cacheDir,
		maxSize:  maxSize,
		entries:  make(map[string]*cacheEntry),
	}, nil
}

// Get returns a local file path for the given remote segment.
// Downloads on cache miss. Returns the local path for mmap or direct read.
func (lc *LocalCache) Get(ctx context.Context, remotePath string) (string, error) {
	lc.mu.Lock()
	if entry, ok := lc.entries[remotePath]; ok {
		entry.lastAccess = time.Now()
		localPath := entry.localPath
		lc.mu.Unlock()
		return localPath, nil
	}
	lc.mu.Unlock()

	// Cache miss — download from remote
	rc, err := lc.remote.Download(ctx, remotePath)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", remotePath, err)
	}
	defer rc.Close()

	localPath := filepath.Join(lc.cacheDir, remotePath)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}

	f, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("create: %w", err)
	}

	n, err := io.Copy(f, rc)
	f.Close()
	if err != nil {
		os.Remove(localPath)
		return "", fmt.Errorf("write: %w", err)
	}

	lc.mu.Lock()
	lc.entries[remotePath] = &cacheEntry{
		localPath:  localPath,
		size:       n,
		lastAccess: time.Now(),
	}
	lc.usedSize += n

	// Evict LRU entries if over capacity
	if lc.maxSize > 0 {
		lc.evictLocked()
	}
	lc.mu.Unlock()

	return localPath, nil
}

// Invalidate removes a specific entry from the cache.
func (lc *LocalCache) Invalidate(remotePath string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if entry, ok := lc.entries[remotePath]; ok {
		os.Remove(entry.localPath)
		lc.usedSize -= entry.size
		delete(lc.entries, remotePath)
	}
}

// UsedSize returns the current cache size in bytes.
func (lc *LocalCache) UsedSize() int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.usedSize
}

// EntryCount returns the number of cached entries.
func (lc *LocalCache) EntryCount() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.entries)
}

// evictLocked removes LRU entries until usedSize < maxSize. Caller must hold mu.
func (lc *LocalCache) evictLocked() {
	for lc.usedSize > lc.maxSize && len(lc.entries) > 0 {
		// Find LRU entry
		var lruKey string
		var lruTime time.Time
		first := true
		for k, e := range lc.entries {
			if first || e.lastAccess.Before(lruTime) {
				lruKey = k
				lruTime = e.lastAccess
				first = false
			}
		}
		entry := lc.entries[lruKey]
		os.Remove(entry.localPath)
		lc.usedSize -= entry.size
		delete(lc.entries, lruKey)
	}
}
