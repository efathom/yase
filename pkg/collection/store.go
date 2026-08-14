package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store persists collection metadata.
type Store interface {
	Save(ctx context.Context, c *Collection) error
	Load(ctx context.Context, id string) (*Collection, error)
	List(ctx context.Context) ([]*Collection, error)
	Delete(ctx context.Context, id string) error
}

// FileStore implements Store using one JSON file per collection.
// Each collection's metadata is stored at <basePath>/<collectionID>/metadata.json.
type FileStore struct {
	basePath string
	mu       sync.RWMutex
}

// NewFileStore creates a file-backed collection store.
func NewFileStore(basePath string) (*FileStore, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("create collection store dir: %w", err)
	}
	return &FileStore{basePath: basePath}, nil
}

func (fs *FileStore) Save(ctx context.Context, c *Collection) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir := filepath.Join(fs.basePath, c.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create collection dir: %w", err)
	}
	return fs.writeJSON(fs.metadataPath(c.ID), c)
}

func (fs *FileStore) Load(ctx context.Context, id string) (*Collection, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var c Collection
	if err := fs.readJSON(fs.metadataPath(id), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (fs *FileStore) List(ctx context.Context) ([]*Collection, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	entries, err := os.ReadDir(fs.basePath)
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}

	var collections []*Collection
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var c Collection
		if err := fs.readJSON(fs.metadataPath(entry.Name()), &c); err != nil {
			continue // skip corrupt entries
		}
		collections = append(collections, &c)
	}
	return collections, nil
}

func (fs *FileStore) Delete(ctx context.Context, id string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return os.RemoveAll(filepath.Join(fs.basePath, id))
}

func (fs *FileStore) metadataPath(id string) string {
	return filepath.Join(fs.basePath, id, "metadata.json")
}

func (fs *FileStore) writeJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()

	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}

	// Fsync the directory so the rename is durable.
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

func (fs *FileStore) readJSON(path string, v interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
