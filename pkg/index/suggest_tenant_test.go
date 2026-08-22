package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func suggestTexts(sugs []Suggestion) []string {
	out := make([]string, 0, len(sugs))
	for _, s := range sugs {
		out = append(out, s.Text)
	}
	return out
}

// M-14: completions must not surface terms from another tenant's documents.
func TestSuggestDoesNotLeakTermsAcrossTenants(t *testing.T) {
	se := NewSuggestEngine()
	se.AddDocument("Acmecorp roadmap", "acmecorp quarterly planning notes",
		map[string]string{"_tenant": "tenant-a"})
	se.AddDocument("Acmerival roadmap", "acmerival quarterly planning notes",
		map[string]string{"_tenant": "tenant-b"})

	got := suggestTexts(se.Suggest("acme", 10, map[string]string{"_tenant": "tenant-a"}))

	assert.Contains(t, got, "acmecorp", "tenant-a must see its own terms")
	assert.NotContains(t, got, "acmerival", "tenant-a must not see tenant-b's terms")
}

// With no tenant constraint (single-tenant deployments) every term is visible.
func TestSuggestWithoutTenantFilterSeesAllTerms(t *testing.T) {
	se := NewSuggestEngine()
	se.AddDocument("Acmecorp roadmap", "acmecorp planning", map[string]string{"_tenant": "tenant-a"})
	se.AddDocument("Acmerival roadmap", "acmerival planning", map[string]string{"_tenant": "tenant-b"})

	got := suggestTexts(se.Suggest("acme", 10, nil))

	assert.Contains(t, got, "acmecorp")
	assert.Contains(t, got, "acmerival")
}

// Documents ingested without tenant metadata stay reachable by an unscoped
// caller but must not appear for a tenant-scoped one.
func TestSuggestUntaggedDocumentsAreNotVisibleToTenants(t *testing.T) {
	se := NewSuggestEngine()
	se.AddDocument("Legacy handbook", "legacyterm reference", nil)

	require.Contains(t, suggestTexts(se.Suggest("legacy", 10, nil)), "legacyterm")

	got := suggestTexts(se.Suggest("legacy", 10, map[string]string{"_tenant": "tenant-a"}))
	assert.NotContains(t, got, "legacyterm",
		"untagged terms must not leak into a tenant-scoped completion")
}

// Frequency ranking must still work within a tenant partition.
func TestSuggestRanksByFrequencyWithinTenant(t *testing.T) {
	se := NewSuggestEngine()
	// "alpha" appears in the title (weight 10), "alphabet" only in content.
	se.AddDocument("alpha", "alphabet alphabet", map[string]string{"_tenant": "t1"})

	got := suggestTexts(se.Suggest("alpha", 10, map[string]string{"_tenant": "t1"}))

	require.NotEmpty(t, got)
	assert.Equal(t, "alpha", got[0], "title terms outrank content terms")
}
