// Package storage provides object storage backends (S3, filesystem, cached)
// used by the offline index builder and connectors.
package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// IndexStore abstracts index segment storage (S3, local FS, etc.)
type IndexStore interface {
	// Upload writes an index segment to durable storage.
	Upload(ctx context.Context, path string, reader io.Reader) error
	// Download fetches an index segment.
	Download(ctx context.Context, path string) (io.ReadCloser, error)
	// List returns all segment paths under a prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Delete removes a segment.
	Delete(ctx context.Context, path string) error
}

// FSStore implements IndexStore backed by the local filesystem.
// Useful for development, testing, and single-node deployments.
type FSStore struct {
	root string
}

// NewFSStore creates a filesystem-backed index store rooted at the given directory.
func NewFSStore(root string) (*FSStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir root: %w", err)
	}
	return &FSStore{root: root}, nil
}

func (fs *FSStore) Upload(_ context.Context, path string, reader io.Reader) error {
	fullPath := filepath.Join(fs.root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, reader); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func (fs *FSStore) Download(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(fs.root, path)
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return f, nil
}

func (fs *FSStore) List(_ context.Context, prefix string) ([]string, error) {
	searchDir := filepath.Join(fs.root, prefix)
	var paths []string
	err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(fs.root, path)
			paths = append(paths, rel)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil, nil
	}
	return paths, err
}

func (fs *FSStore) Delete(_ context.Context, path string) error {
	return os.Remove(filepath.Join(fs.root, path))
}

// CachedStore wraps an IndexStore with a local filesystem cache.
// Downloads are cached on first access; subsequent reads come from disk.
type CachedStore struct {
	remote   IndexStore
	cacheDir string
}

// NewCachedStore creates a caching wrapper around any IndexStore.
func NewCachedStore(remote IndexStore, cacheDir string) (*CachedStore, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache: %w", err)
	}
	return &CachedStore{remote: remote, cacheDir: cacheDir}, nil
}

func (c *CachedStore) Upload(ctx context.Context, path string, reader io.Reader) error {
	return c.remote.Upload(ctx, path, reader)
}

func (c *CachedStore) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	cachePath := filepath.Join(c.cacheDir, path)

	// Check cache
	if f, err := os.Open(cachePath); err == nil {
		return f, nil
	}

	// Cache miss — download from remote
	rc, err := c.remote.Download(ctx, path)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// Write to cache
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache path: %w", err)
	}
	f, err := os.Create(cachePath)
	if err != nil {
		return nil, fmt.Errorf("create cache file: %w", err)
	}
	if _, err := io.Copy(f, rc); err != nil {
		f.Close()
		_ = os.Remove(cachePath)
		return nil, fmt.Errorf("write cache: %w", err)
	}
	f.Close()

	// Return from cache
	return os.Open(cachePath)
}

func (c *CachedStore) List(ctx context.Context, prefix string) ([]string, error) {
	return c.remote.List(ctx, prefix)
}

func (c *CachedStore) Delete(ctx context.Context, path string) error {
	// Remove from cache too
	cachePath := filepath.Join(c.cacheDir, path)
	_ = os.Remove(cachePath)
	return c.remote.Delete(ctx, path)
}
