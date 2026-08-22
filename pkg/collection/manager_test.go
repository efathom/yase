package collection

import (
	"context"
	"testing"

	"github.com/efathom/yase/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfig() *config.Config {
	return &config.Config{
		Embedder: config.EmbedderConfig{
			Provider:  "mock",
			Dimension: 32,
		},
		Index: config.IndexConfig{
			ArenaSize:    64 * 1024 * 1024, // 64MB for tests
			CentroidRate: 5,
		},
	}
}

func setupManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	require.NoError(t, err)
	return NewManager(store, dir, testConfig(), nil)
}

func TestRestoreAll_CreatesDefault(t *testing.T) {
	m := setupManager(t)
	ctx := context.Background()

	err := m.RestoreAll(ctx)
	require.NoError(t, err)

	c, err := m.Get(ctx, "", DefaultCollectionID)
	require.NoError(t, err)
	assert.Equal(t, DefaultCollectionID, c.ID)
	assert.Equal(t, StatusReady, c.Status)
	assert.Equal(t, "Default Collection", c.Name)

	t.Cleanup(func() { m.Close() })
}

func TestCreate(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		colName  string
		tenantID string
		cfg      CollectionConfig
		wantErr  bool
	}{
		{
			name:     "basic creation",
			id:       "test-col",
			colName:  "Test Collection",
			tenantID: "tenant-1",
		},
		{
			name:    "with custom config",
			id:      "custom-col",
			colName: "Custom Collection",
			cfg: CollectionConfig{
				CentroidRate: 10,
				VecDim:       32,
			},
		},
		{
			name:    "with embedder override",
			id:      "emb-col",
			colName: "Embedder Override",
			cfg: CollectionConfig{
				Embedder: &EmbedderRef{
					Provider:  "mock",
					Dimension: 64,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupManager(t)
			t.Cleanup(func() { m.Close() })
			ctx := context.Background()

			c, err := m.Create(ctx, tt.tenantID, tt.id, tt.colName, tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.id, c.ID)
			assert.Equal(t, tt.colName, c.Name)
			assert.Equal(t, tt.tenantID, c.TenantID)
			assert.Equal(t, StatusReady, c.Status)

			// Engine should be available
			eng, err := m.GetEngine(tt.id)
			require.NoError(t, err)
			assert.NotNil(t, eng)

			// Embedder should be available
			emb, err := m.GetEmbedder(tt.id)
			require.NoError(t, err)
			assert.NotNil(t, emb)
		})
	}
}

func TestCreate_Duplicate(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	_, err := m.Create(ctx, "", "dup", "First", CollectionConfig{})
	require.NoError(t, err)

	_, err = m.Create(ctx, "", "dup", "Second", CollectionConfig{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestDelete(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	_, err := m.Create(ctx, "t1", "to-delete", "Ephemeral", CollectionConfig{})
	require.NoError(t, err)

	err = m.Delete(ctx, "", "to-delete")
	require.NoError(t, err)

	_, err = m.Get(ctx, "", "to-delete")
	assert.Error(t, err)

	_, err = m.GetEngine("to-delete")
	assert.Error(t, err)
}

func TestDelete_DefaultForbidden(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	require.NoError(t, m.RestoreAll(ctx))

	err := m.Delete(ctx, "", DefaultCollectionID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot delete")
}

func TestList(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	_, err := m.Create(ctx, "t1", "col-a", "A", CollectionConfig{})
	require.NoError(t, err)
	_, err = m.Create(ctx, "t1", "col-b", "B", CollectionConfig{})
	require.NoError(t, err)
	_, err = m.Create(ctx, "t2", "col-c", "C", CollectionConfig{})
	require.NoError(t, err)

	// List all
	all, err := m.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, all, 3)

	// List by tenant
	t1, err := m.List(ctx, "t1")
	require.NoError(t, err)
	assert.Len(t, t1, 2)

	t2, err := m.List(ctx, "t2")
	require.NoError(t, err)
	assert.Len(t, t2, 1)
}

func TestUpdate(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	_, err := m.Create(ctx, "", "upd", "Original", CollectionConfig{})
	require.NoError(t, err)

	updated, err := m.Update(ctx, "", "upd", "Renamed", "A description")
	require.NoError(t, err)
	assert.Equal(t, "Renamed", updated.Name)
	assert.Equal(t, "A description", updated.Description)

	// Verify persisted
	got, err := m.Get(ctx, "", "upd")
	require.NoError(t, err)
	assert.Equal(t, "Renamed", got.Name)
}

func TestBindUnbindConnector(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	_, err := m.Create(ctx, "", "bind-test", "Bind Test", CollectionConfig{})
	require.NoError(t, err)

	// Bind
	err = m.BindConnector(ctx, "", "bind-test", "conn-1")
	require.NoError(t, err)
	err = m.BindConnector(ctx, "", "bind-test", "conn-2")
	require.NoError(t, err)

	c, _ := m.Get(ctx, "", "bind-test")
	assert.Equal(t, []string{"conn-1", "conn-2"}, c.Connectors)

	// Duplicate bind is idempotent
	err = m.BindConnector(ctx, "", "bind-test", "conn-1")
	require.NoError(t, err)
	c, _ = m.Get(ctx, "", "bind-test")
	assert.Len(t, c.Connectors, 2)

	// Unbind
	err = m.UnbindConnector(ctx, "", "bind-test", "conn-1")
	require.NoError(t, err)
	c, _ = m.Get(ctx, "", "bind-test")
	assert.Equal(t, []string{"conn-2"}, c.Connectors)
}

func TestAllEngines(t *testing.T) {
	m := setupManager(t)
	t.Cleanup(func() { m.Close() })
	ctx := context.Background()

	_, err := m.Create(ctx, "", "a", "A", CollectionConfig{})
	require.NoError(t, err)
	_, err = m.Create(ctx, "", "b", "B", CollectionConfig{})
	require.NoError(t, err)

	engines := m.AllEngines()
	assert.Len(t, engines, 2)
	assert.Contains(t, engines, "a")
	assert.Contains(t, engines, "b")
}

func TestRestoreAll_ReloadsPersistedCollections(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	ctx := context.Background()

	// Create manager, add collections, close
	store1, err := NewFileStore(dir)
	require.NoError(t, err)
	m1 := NewManager(store1, dir, cfg, nil)

	require.NoError(t, m1.RestoreAll(ctx))
	_, err = m1.Create(ctx, "t1", "persist-me", "Persistent", CollectionConfig{})
	require.NoError(t, err)
	require.NoError(t, m1.Close())

	// New manager from same directory should restore
	store2, err := NewFileStore(dir)
	require.NoError(t, err)
	m2 := NewManager(store2, dir, cfg, nil)
	t.Cleanup(func() { m2.Close() })

	require.NoError(t, m2.RestoreAll(ctx))

	c, err := m2.Get(ctx, "", "persist-me")
	require.NoError(t, err)
	assert.Equal(t, "Persistent", c.Name)
	assert.Equal(t, StatusReady, c.Status)

	// _default should also exist
	_, err = m2.Get(ctx, "", DefaultCollectionID)
	require.NoError(t, err)

	eng, err := m2.GetEngine("persist-me")
	require.NoError(t, err)
	assert.NotNil(t, eng)
}

func TestGet_NotFound(t *testing.T) {
	m := setupManager(t)
	ctx := context.Background()

	_, err := m.Get(ctx, "", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDelete_NotFound(t *testing.T) {
	m := setupManager(t)
	ctx := context.Background()

	err := m.Delete(ctx, "", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
