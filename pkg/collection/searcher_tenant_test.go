package collection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// C-01 (defense in depth): document-level "_tenant" filtering already stops
// content crossing tenants, but the fan-out should not open another tenant's
// engine at all — naming one must not even confirm it exists.
func TestResolveEnginesExcludesOtherTenants(t *testing.T) {
	mgr, _ := twoTenantManager(t)
	s := NewSearcher(mgr)

	got := s.resolveEngines("tenant-a", []string{"col-a", "col-b"})

	assert.Contains(t, got, "col-a")
	assert.NotContains(t, got, "col-b",
		"tenant-a must not reach tenant-b's engine even when it names it")
}

// An empty collection list fans out across the caller's collections only.
func TestResolveEnginesEmptyListIsTenantScoped(t *testing.T) {
	mgr, _ := twoTenantManager(t)
	s := NewSearcher(mgr)

	got := s.resolveEngines("tenant-a", nil)

	assert.NotContains(t, got, "col-b",
		"a cross-collection search must not fan out into another tenant")
}

// Unscoped callers (auth disabled) keep full reach.
func TestResolveEnginesUnscopedCallerSeesAll(t *testing.T) {
	mgr, _ := twoTenantManager(t)
	s := NewSearcher(mgr)

	got := s.resolveEngines("", nil)

	assert.Contains(t, got, "col-a")
	assert.Contains(t, got, "col-b")
}

// The manager-level helper backing the above.
func TestEnginesForTenantFiltersByOwner(t *testing.T) {
	mgr, _ := twoTenantManager(t)

	got := mgr.EnginesForTenant("tenant-b", nil)

	assert.Contains(t, got, "col-b")
	assert.NotContains(t, got, "col-a")
}

// Naming only another tenant's collection resolves to no reachable engines,
// so the search fails exactly as it would for a collection that does not
// exist — the caller learns nothing either way.
func TestSearchOnOtherTenantsCollectionFindsNothing(t *testing.T) {
	mgr, ctx := twoTenantManager(t)
	s := NewSearcher(mgr)

	results, err := s.Search(ctx, "tenant-a", []string{"col-b"}, "anything",
		make([]float32, 32), nil, 10)

	require.Error(t, err, "tenant-a must not be able to search tenant-b's collection")
	assert.Empty(t, results)

	// Identical outcome for a collection that genuinely does not exist.
	_, missingErr := s.Search(ctx, "tenant-a", []string{"no-such-collection"}, "anything",
		make([]float32, 32), nil, 10)
	assert.Equal(t, err.Error(), missingErr.Error(),
		"the two cases must be indistinguishable to the caller")
}
