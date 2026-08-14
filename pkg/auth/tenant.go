package auth

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
	filters["_tenant"] = ac.TenantID
	return filters
}

// InjectTenantMetadata adds the tenant ID to document metadata during ingestion.
// Ensures every document is tagged with its owning tenant.
func InjectTenantMetadata(ac *AuthContext, metadata map[string]string) map[string]string {
	if ac == nil || ac.TenantID == "" {
		return metadata
	}

	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["_tenant"] = ac.TenantID
	return metadata
}
