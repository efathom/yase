// Package auth provides authentication and authorization for YASE APIs.
// Supports API key and JWT-based authentication with tenant isolation.
package auth

import (
	"context"
	"fmt"
)

// contextKey is an unexported type for context keys to avoid collisions.
type contextKey int

const authContextKey contextKey = iota

// AuthContext holds the authenticated identity and permissions for a request.
type AuthContext struct {
	TenantID string   `json:"tenant_id"`
	UserID   string   `json:"user_id"`
	Roles    []string `json:"roles"`  // "admin", "reader", "writer"
	Scopes   []string `json:"scopes"` // "search", "ingest", "admin", "delete"
}

// HasRole checks if the auth context includes a specific role.
func (a *AuthContext) HasRole(role string) bool {
	for _, r := range a.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasScope checks if the auth context includes a specific scope.
func (a *AuthContext) HasScope(scope string) bool {
	for _, s := range a.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Authenticator validates a credential and returns the authenticated context.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (*AuthContext, error)
}

// FromContext extracts the AuthContext from a request context.
// Returns nil if no auth context is present (unauthenticated).
func FromContext(ctx context.Context) *AuthContext {
	ac, _ := ctx.Value(authContextKey).(*AuthContext)
	return ac
}

// WithContext stores an AuthContext in the request context.
func WithContext(ctx context.Context, ac *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, ac)
}

// ErrUnauthorized is returned when no valid credentials are provided.
var ErrUnauthorized = fmt.Errorf("unauthorized: missing or invalid credentials")

// ErrForbidden is returned when credentials are valid but lack required permissions.
var ErrForbidden = fmt.Errorf("forbidden: insufficient permissions")
