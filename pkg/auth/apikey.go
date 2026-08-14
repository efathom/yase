package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync"
)

// APIKeyEntry defines a configured API key with its associated permissions.
type APIKeyEntry struct {
	Key      string   `json:"key" yaml:"key"`
	TenantID string   `json:"tenant_id" yaml:"tenant_id"`
	UserID   string   `json:"user_id,omitempty" yaml:"user_id"`
	Roles    []string `json:"roles" yaml:"roles"`
	Scopes   []string `json:"scopes,omitempty" yaml:"scopes"`
}

// APIKeyAuthenticator validates API keys against a configured set.
type APIKeyAuthenticator struct {
	mu   sync.RWMutex
	keys map[string]*APIKeyEntry // key hash → entry
}

// NewAPIKeyAuthenticator creates an authenticator from a list of API key entries.
func NewAPIKeyAuthenticator(entries []APIKeyEntry) *APIKeyAuthenticator {
	keys := make(map[string]*APIKeyEntry, len(entries))
	for i := range entries {
		keys[entries[i].Key] = &entries[i]
	}
	return &APIKeyAuthenticator{keys: keys}
}

// Authenticate validates the provided token against configured API keys.
// Uses constant-time comparison to prevent timing attacks.
func (a *APIKeyAuthenticator) Authenticate(_ context.Context, token string) (*AuthContext, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Always iterate ALL keys to prevent timing side-channels on key count/position
	var matched *APIKeyEntry
	for key, entry := range a.keys {
		if subtle.ConstantTimeCompare([]byte(token), []byte(key)) == 1 {
			matched = entry
		}
	}
	if matched == nil {
		return nil, fmt.Errorf("invalid API key")
	}

	scopes := matched.Scopes
	if len(scopes) == 0 {
		scopes = scopesFromRoles(matched.Roles)
	}
	return &AuthContext{
		TenantID: matched.TenantID,
		UserID:   matched.UserID,
		Roles:    matched.Roles,
		Scopes:   scopes,
	}, nil
}

// AddKey dynamically adds an API key (for management API use).
func (a *APIKeyAuthenticator) AddKey(entry APIKeyEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keys[entry.Key] = &entry
}

// RemoveKey removes an API key.
func (a *APIKeyAuthenticator) RemoveKey(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.keys, key)
}

func scopesFromRoles(roles []string) []string {
	scopeSet := make(map[string]bool)
	for _, role := range roles {
		switch role {
		case "admin":
			scopeSet["search"] = true
			scopeSet["ingest"] = true
			scopeSet["delete"] = true
			scopeSet["admin"] = true
		case "writer":
			scopeSet["search"] = true
			scopeSet["ingest"] = true
		case "reader":
			scopeSet["search"] = true
		}
	}
	scopes := make([]string, 0, len(scopeSet))
	for s := range scopeSet {
		scopes = append(scopes, s)
	}
	return scopes
}
