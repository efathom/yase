package index

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedTenantDocs ingests n documents for each of two tenants, returning the
// IDs belonging to each.
func seedTenantDocs(t *testing.T, he *HybridEngine, vecDim, n int) (tenantA, tenantB []uint32) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(7))

	for i := 0; i < n*2; i++ {
		vec := make([]float32, vecDim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		tenant := "tenant-a"
		if i%2 == 1 {
			tenant = "tenant-b"
		}
		doc := Document{
			ID:       uint32(i),
			Text:     fmt.Sprintf("shared subject matter document %d", i),
			Vector:   vec,
			Metadata: map[string]string{"_tenant": tenant},
		}
		require.NoError(t, he.Ingest(ctx, doc))
		if tenant == "tenant-a" {
			tenantA = append(tenantA, uint32(i))
		} else {
			tenantB = append(tenantB, uint32(i))
		}
	}
	return tenantA, tenantB
}

// H-05: Delete must honor a metadata constraint so a caller holding another
// tenant's document IDs still cannot remove them.
func TestDeleteWithFilterSkipsDocumentsOutsideTheFilter(t *testing.T) {
	const vecDim = 32
	he, cleanup := tempHybridEngine(t, vecDim)
	defer cleanup()

	ctx := context.Background()
	tenantA, tenantB := seedTenantDocs(t, he, vecDim, 10)

	// tenant-a asks to delete every document it knows about, including
	// tenant-b's IDs.
	all := append(append([]uint32{}, tenantA...), tenantB...)
	deleted, err := he.Delete(ctx, all, map[string]string{"_tenant": "tenant-a"})
	require.NoError(t, err)

	assert.Equal(t, len(tenantA), deleted,
		"only the caller's own documents should be deleted")

	// tenant-b's documents must still be retrievable.
	hits, err := he.BlugeStore.GetDocumentsByIDs(ctx, tenantB)
	require.NoError(t, err)
	assert.Len(t, hits, len(tenantB),
		"tenant-b documents must survive a delete issued by tenant-a")
}

// An empty filter keeps the previous unconstrained behavior, which is what the
// single-tenant and internal compaction paths rely on.
func TestDeleteWithNoFilterDeletesEverythingRequested(t *testing.T) {
	const vecDim = 32
	he, cleanup := tempHybridEngine(t, vecDim)
	defer cleanup()

	ctx := context.Background()
	tenantA, tenantB := seedTenantDocs(t, he, vecDim, 5)

	all := append(append([]uint32{}, tenantA...), tenantB...)
	deleted, err := he.Delete(ctx, all, nil)
	require.NoError(t, err)

	assert.Equal(t, len(all), deleted, "an unfiltered delete removes every requested ID")
}
