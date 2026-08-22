package collection

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// twoTenantManager builds a manager holding one collection per tenant.
func twoTenantManager(t *testing.T) (*Manager, context.Context) {
	t.Helper()
	mgr := setupManager(t)
	ctx := context.Background()

	_, err := mgr.Create(ctx, "tenant-a", "col-a", "A's collection", CollectionConfig{})
	require.NoError(t, err)
	_, err = mgr.Create(ctx, "tenant-b", "col-b", "B's collection", CollectionConfig{})
	require.NoError(t, err)

	return mgr, ctx
}

// C-01: reading another tenant's collection must fail.
func TestGetRejectsOtherTenantsCollection(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	_, err := mgr.Get(ctx, "tenant-a", "col-b")
	require.Error(t, err, "tenant-a must not read tenant-b's collection")
	assert.ErrorIs(t, err, ErrNotFound,
		"the error must not confirm the collection exists")
}

func TestGetAllowsOwnCollection(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	c, err := mgr.Get(ctx, "tenant-a", "col-a")
	require.NoError(t, err)
	assert.Equal(t, "col-a", c.ID)
}

// C-01: the destructive case — deleting another tenant's collection removes
// its index from disk.
func TestDeleteRejectsOtherTenantsCollection(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	err := mgr.Delete(ctx, "tenant-a", "col-b")
	require.Error(t, err, "tenant-a must not delete tenant-b's collection")

	// The collection must survive.
	c, err := mgr.Get(ctx, "tenant-b", "col-b")
	require.NoError(t, err)
	assert.Equal(t, "col-b", c.ID)
}

func TestUpdateRejectsOtherTenantsCollection(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	_, err := mgr.Update(ctx, "tenant-a", "col-b", "renamed", "")
	require.Error(t, err, "tenant-a must not rename tenant-b's collection")

	c, err := mgr.Get(ctx, "tenant-b", "col-b")
	require.NoError(t, err)
	assert.Equal(t, "B's collection", c.Name, "the name must be unchanged")
}

func TestBindConnectorRejectsOtherTenantsCollection(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	err := mgr.BindConnector(ctx, "tenant-a", "col-b", "job-1")
	require.Error(t, err, "tenant-a must not bind a connector to tenant-b's collection")
}

// An empty caller tenant means multi-tenancy is off — every collection is
// reachable, which is what single-tenant deployments rely on.
func TestEmptyCallerTenantHasFullAccess(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	for _, id := range []string{"col-a", "col-b"} {
		c, err := mgr.Get(ctx, "", id)
		require.NoError(t, err, "unscoped caller must reach %q", id)
		assert.Equal(t, id, c.ID)
	}
}

// C-02: a tenant listing collections sees only its own, regardless of what it
// asks for.
func TestListIsScopedToCallerTenant(t *testing.T) {
	mgr, ctx := twoTenantManager(t)

	got, err := mgr.List(ctx, "tenant-a")
	require.NoError(t, err)

	for _, c := range got {
		assert.Equal(t, "tenant-a", c.TenantID,
			"listing must not include another tenant's collection")
	}
	assert.Len(t, got, 1)
}
