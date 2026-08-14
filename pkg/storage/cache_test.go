package storage

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestLocalCacheHitAndMiss(t *testing.T) {
	dir := t.TempDir()
	remote, _ := NewFSStore(dir + "/remote")
	ctx := context.Background()

	// Populate remote
	remote.Upload(ctx, "seg/data.bin", bytes.NewReader([]byte("segment data")))

	cache, err := NewLocalCache(remote, dir+"/cache", 0)
	if err != nil {
		t.Fatalf("NewLocalCache: %v", err)
	}

	// First access: cache miss → download
	path1, err := cache.Get(ctx, "seg/data.bin")
	if err != nil {
		t.Fatalf("Get (miss): %v", err)
	}

	data, _ := os.ReadFile(path1)
	if string(data) != "segment data" {
		t.Errorf("content: got %q, want %q", data, "segment data")
	}

	// Second access: cache hit → same path, no download
	path2, err := cache.Get(ctx, "seg/data.bin")
	if err != nil {
		t.Fatalf("Get (hit): %v", err)
	}
	if path1 != path2 {
		t.Errorf("cache hit should return same path: %q vs %q", path1, path2)
	}
}

func TestLocalCacheLRUEviction(t *testing.T) {
	dir := t.TempDir()
	remote, _ := NewFSStore(dir + "/remote")
	ctx := context.Background()

	// Upload 3 segments of ~10 bytes each
	remote.Upload(ctx, "a.bin", bytes.NewReader(bytes.Repeat([]byte("A"), 10)))
	remote.Upload(ctx, "b.bin", bytes.NewReader(bytes.Repeat([]byte("B"), 10)))
	remote.Upload(ctx, "c.bin", bytes.NewReader(bytes.Repeat([]byte("C"), 10)))

	// Cache with max 20 bytes (fits 2 segments)
	cache, _ := NewLocalCache(remote, dir+"/cache", 20)

	cache.Get(ctx, "a.bin") // fills 10/20
	cache.Get(ctx, "b.bin") // fills 20/20
	cache.Get(ctx, "a.bin") // hit: updates a's lastAccess

	// This should evict b (LRU) since a was accessed more recently
	cache.Get(ctx, "c.bin") // fills 20/20, evicts b

	if cache.EntryCount() != 2 {
		t.Errorf("expected 2 entries, got %d", cache.EntryCount())
	}
	if cache.UsedSize() > 20 {
		t.Errorf("used size %d exceeds max 20", cache.UsedSize())
	}
}

func TestLocalCacheInvalidate(t *testing.T) {
	dir := t.TempDir()
	remote, _ := NewFSStore(dir + "/remote")
	ctx := context.Background()

	remote.Upload(ctx, "x.bin", bytes.NewReader([]byte("data")))
	cache, _ := NewLocalCache(remote, dir+"/cache", 0)

	path, _ := cache.Get(ctx, "x.bin")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cached file should exist: %v", err)
	}

	cache.Invalidate("x.bin")

	if cache.EntryCount() != 0 {
		t.Error("entry count should be 0 after invalidate")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("cached file should be deleted after invalidate")
	}
}

func TestLocalCacheDownloadError(t *testing.T) {
	dir := t.TempDir()
	remote, _ := NewFSStore(dir + "/remote")
	cache, _ := NewLocalCache(remote, dir+"/cache", 0)

	_, err := cache.Get(context.Background(), "nonexistent.bin")
	if err == nil {
		t.Error("expected error for missing remote file")
	}
}
