package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFSStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFSStore(dir)
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	ctx := context.Background()

	data := []byte("hello world")
	if err := store.Upload(ctx, "indexes/v1/shard-0/data.bin", bytes.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := store.Download(ctx, "indexes/v1/shard-0/data.bin")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("roundtrip: got %q, want %q", got, data)
	}
}

func TestFSStoreList(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFSStore(dir)
	ctx := context.Background()

	store.Upload(ctx, "idx/v1/a.bin", bytes.NewReader([]byte("a")))
	store.Upload(ctx, "idx/v1/b.bin", bytes.NewReader([]byte("b")))
	store.Upload(ctx, "idx/v2/c.bin", bytes.NewReader([]byte("c")))

	paths, err := store.List(ctx, "idx/v1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(paths) != 2 {
		t.Errorf("expected 2 files under idx/v1, got %d: %v", len(paths), paths)
	}
}

func TestFSStoreListEmpty(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFSStore(dir)

	paths, err := store.List(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(paths))
	}
}

func TestFSStoreDelete(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFSStore(dir)
	ctx := context.Background()

	store.Upload(ctx, "file.txt", bytes.NewReader([]byte("data")))
	if err := store.Delete(ctx, "file.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.Download(ctx, "file.txt")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestFSStoreDownloadMissing(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFSStore(dir)

	_, err := store.Download(context.Background(), "nonexistent.bin")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestCachedStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	cacheDir := filepath.Join(dir, "cache")

	remote, _ := NewFSStore(remoteDir)
	cached, err := NewCachedStore(remote, cacheDir)
	if err != nil {
		t.Fatalf("NewCachedStore: %v", err)
	}
	ctx := context.Background()

	data := []byte("cached data")
	if err := cached.Upload(ctx, "test/data.bin", bytes.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// First download — cache miss, fetches from remote
	rc, err := cached.Download(ctx, "test/data.bin")
	if err != nil {
		t.Fatalf("Download (miss): %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}

	// Verify cache file exists
	cachePath := filepath.Join(cacheDir, "test/data.bin")
	if _, err := os.Stat(cachePath); err != nil {
		t.Errorf("cache file should exist: %v", err)
	}

	// Second download — cache hit (even if we delete remote)
	os.Remove(filepath.Join(remoteDir, "test/data.bin"))
	rc2, err := cached.Download(ctx, "test/data.bin")
	if err != nil {
		t.Fatalf("Download (hit): %v", err)
	}
	got2, _ := io.ReadAll(rc2)
	rc2.Close()
	if !bytes.Equal(got2, data) {
		t.Errorf("cache hit: got %q, want %q", got2, data)
	}
}

func TestCachedStoreDelete(t *testing.T) {
	dir := t.TempDir()
	remote, _ := NewFSStore(filepath.Join(dir, "remote"))
	cached, _ := NewCachedStore(remote, filepath.Join(dir, "cache"))
	ctx := context.Background()

	cached.Upload(ctx, "del.bin", bytes.NewReader([]byte("x")))
	cached.Download(ctx, "del.bin") // populate cache

	if err := cached.Delete(ctx, "del.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Both remote and cache should be gone
	_, err := cached.Download(ctx, "del.bin")
	if err == nil {
		t.Error("expected error after delete")
	}
}
