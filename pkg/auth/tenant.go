package auth

import "github.com/efathom/yase/pkg/index"

// TenantField is the reserved document metadata field carrying the owning
// tenant. Re-exported from pkg/index so callers on the policy side do not need
// to import the index package directly.
const TenantField = index.TenantField

// InjectTenantFilter adds the tenant isolation filter to search metadata filters.
// If auth is enabled and the user has a tenant ID, searches are automatically
// scoped to that tenant's documents. This cannot be bypassed by the caller.
func InjectTenantFilter(ac *AuthContext, filters map[string]string) map[string]string {
	if ac == nil || ac.TenantID == "" {
		return filters
	}

	if filters == nil {
		filters = make(map[string]string)
	}
	// Force tenant filter — cannot be overridden by the request
	filters[TenantField] = ac.TenantID
	return filters
}

// InjectTenantMetadata adds the tenant ID to document metadata during ingestion.
// Ensures every document is tagged with its owning tenant.
//
// This is the write-side counterpart to InjectTenantFilter: without it nothing
// carries a tenant, and every tenant-scoped search matches zero documents while
// every unscoped read matches all of them.
func InjectTenantMetadata(ac *AuthContext, metadata map[string]string) map[string]string {
	if ac == nil || ac.TenantID == "" {
		return metadata
	}

	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[TenantField] = ac.TenantID
	return metadata
}
