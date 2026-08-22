package indexsvc

import (
	"context"
	"testing"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tenantCtx(tenant string) context.Context {
	return auth.WithContext(context.Background(), &auth.AuthContext{
		TenantID: tenant,
		UserID:   "u1",
		Roles:    []string{"writer"},
		Scopes:   []string{"search", "ingest"},
	})
}

func unitVec(dim int, seed float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = seed + float32(i)*0.01
	}
	return v
}

// H-04: ingestion must tag each document with the authenticated tenant, or
// nothing carries "_tenant" and tenant-scoped search matches nothing.
func TestIngestTagsDocumentWithAuthenticatedTenant(t *testing.T) {
	const dim = 8
	engine := makeEngine(t, dim)
	srv := NewServer(engine)

	_, err := srv.Ingest(tenantCtx("tenant-a"), &ingestionv1.IngestRequest{
		DocId:    1,
		Text:     "quarterly revenue projections",
		Vector:   unitVec(dim, 0.1),
		Metadata: map[string]string{"source": "wiki"},
	})
	require.NoError(t, err)

	hits, err := engine.BlugeStore.GetDocumentsByIDs(context.Background(), []uint32{1})
	require.NoError(t, err)
	require.Contains(t, hits, uint32(1))

	assert.Equal(t, "tenant-a", hits[1].Metadata[index.TenantField],
		"the ingested document must carry its owning tenant")
	assert.Equal(t, "wiki", hits[1].Metadata["source"],
		"caller metadata must be preserved alongside the tenant tag")
}

// H-04: a caller must not be able to plant a document under another tenant.
func TestIngestOverridesCallerSuppliedTenant(t *testing.T) {
	const dim = 8
	engine := makeEngine(t, dim)
	srv := NewServer(engine)

	_, err := srv.Ingest(tenantCtx("tenant-a"), &ingestionv1.IngestRequest{
		DocId:    1,
		Text:     "planted document",
		Vector:   unitVec(dim, 0.1),
		Metadata: map[string]string{index.TenantField: "victim"},
	})
	require.NoError(t, err)

	hits, err := engine.BlugeStore.GetDocumentsByIDs(context.Background(), []uint32{1})
	require.NoError(t, err)
	assert.Equal(t, "tenant-a", hits[1].Metadata[index.TenantField],
		"a caller-supplied tenant tag must be overwritten with the authenticated one")
}

// C-03 equivalent on the gRPC surface: search must be tenant-scoped there too.
func TestGRPCSearchIsTenantScoped(t *testing.T) {
	const dim = 8
	engine := makeEngine(t, dim)
	srv := NewServer(engine)

	// Two tenants ingest near-identical documents.
	_, err := srv.Ingest(tenantCtx("tenant-a"), &ingestionv1.IngestRequest{
		DocId: 1, Text: "shared subject matter", Vector: unitVec(dim, 0.1),
	})
	require.NoError(t, err)
	_, err = srv.Ingest(tenantCtx("tenant-b"), &ingestionv1.IngestRequest{
		DocId: 2, Text: "shared subject matter", Vector: unitVec(dim, 0.1),
	})
	require.NoError(t, err)

	resp, err := srv.Search(tenantCtx("tenant-a"), &ingestionv1.SearchRequest{
		Query:       "shared subject matter",
		QueryVector: unitVec(dim, 0.1),
		TopK:        10,
	})
	require.NoError(t, err)

	for _, r := range resp.Results {
		assert.NotEqual(t, uint32(2), r.Id,
			"tenant-a search must not return tenant-b's document")
	}
}
