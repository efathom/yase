package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// JWTConfig configures JWT-based authentication.
type JWTConfig struct {
	Secret      string `json:"secret" yaml:"secret"`             // HMAC-SHA256 secret
	Issuer      string `json:"issuer" yaml:"issuer"`             // expected issuer claim
	Audience    string `json:"audience" yaml:"audience"`         // expected audience claim
	TenantClaim string `json:"tenant_claim" yaml:"tenant_claim"` // JWT claim containing tenant ID (default "tenant_id")
	RolesClaim  string `json:"roles_claim" yaml:"roles_claim"`   // JWT claim containing roles (default "roles")

	// DefaultRoles are granted when the token carries no roles claim. Empty
	// by default: a token that asserts no authority receives none.
	DefaultRoles []string `json:"default_roles" yaml:"default_roles"`

	// RequireTenant rejects tokens with no tenant claim. Enable it whenever
	// multi-tenancy is in use — an empty tenant produces an unscoped context
	// that the tenant filter cannot narrow.
	RequireTenant bool `json:"require_tenant" yaml:"require_tenant"`
}

// JWTAuthenticator validates HS256 JWT tokens.
// For production RS256/JWKS, extend with a JWKS fetcher.
type JWTAuthenticator struct {
	secret        []byte
	issuer        string
	audience      string
	tenantClaim   string
	rolesClaim    string
	defaultRoles  []string
	requireTenant bool
}

// NewJWTAuthenticator creates a JWT authenticator.
func NewJWTAuthenticator(cfg JWTConfig) *JWTAuthenticator {
	tenantClaim := cfg.TenantClaim
	if tenantClaim == "" {
		tenantClaim = "tenant_id"
	}
	rolesClaim := cfg.RolesClaim
	if rolesClaim == "" {
		rolesClaim = "roles"
	}
	return &JWTAuthenticator{
		secret:        []byte(cfg.Secret),
		issuer:        cfg.Issuer,
		audience:      cfg.Audience,
		tenantClaim:   tenantClaim,
		rolesClaim:    rolesClaim,
		defaultRoles:  cfg.DefaultRoles,
		requireTenant: cfg.RequireTenant,
	}
}

// audienceMatches reports whether the "aud" claim satisfies want.
//
// RFC 7519 §4.1.3 permits either a single string or an array of strings;
// identity providers commonly emit the array form for multi-audience tokens.
func audienceMatches(claim any, want string) bool {
	switch aud := claim.(type) {
	case string:
		return aud == want
	case []any:
		for _, v := range aud {
			if s, ok := v.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

// Authenticate validates a JWT token and extracts the auth context.
func (j *JWTAuthenticator) Authenticate(_ context.Context, token string) (*AuthContext, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT: expected 3 parts, got %d", len(parts))
	}

	// Validate the signing algorithm from the header before verifying.
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT header encoding: %w", err)
	}
	var header map[string]interface{}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("invalid JWT header JSON: %w", err)
	}
	if alg, _ := header["alg"].(string); alg != "HS256" {
		return nil, fmt.Errorf("unsupported JWT algorithm %q (only HS256 is accepted)", alg)
	}

	// Verify signature (HS256)
	signingInput := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, j.secret)
	mac.Write([]byte(signingInput))
	expectedSig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(parts[2]), []byte(expectedSig)) != 1 {
		return nil, fmt.Errorf("invalid JWT signature")
	}

	// Decode payload
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT payload encoding: %w", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("invalid JWT payload JSON: %w", err)
	}

	// Validate standard claims
	if j.issuer != "" {
		if iss, _ := claims["iss"].(string); iss != j.issuer {
			return nil, fmt.Errorf("invalid JWT issuer")
		}
	}
	if j.audience != "" && !audienceMatches(claims["aud"], j.audience) {
		return nil, fmt.Errorf("invalid JWT audience")
	}

	now := time.Now().Unix()

	// exp is required — reject tokens without an expiry.
	exp, ok := claims["exp"].(float64)
	if !ok {
		return nil, fmt.Errorf("JWT missing exp claim")
	}
	if now >= int64(exp) {
		return nil, fmt.Errorf("JWT token expired")
	}

	if nbf, ok := claims["nbf"].(float64); ok {
		if now < int64(nbf) {
			return nil, fmt.Errorf("JWT not yet valid")
		}
	}

	// Extract custom claims
	tenantID, _ := claims[j.tenantClaim].(string)
	if j.requireTenant && tenantID == "" {
		return nil, fmt.Errorf("JWT missing required tenant claim %q", j.tenantClaim)
	}
	userID, _ := claims["sub"].(string)

	var roles []string
	switch r := claims[j.rolesClaim].(type) {
	case []interface{}:
		for _, v := range r {
			if s, ok := v.(string); ok {
				roles = append(roles, s)
			}
		}
	case string:
		roles = strings.Split(r, ",")
	}

	// A token that asserts no roles gets only what the deployment configured
	// as a default — empty unless set, so the fallback cannot silently grant
	// read access.
	if len(roles) == 0 {
		roles = j.defaultRoles
	}

	return &AuthContext{
		TenantID: tenantID,
		UserID:   userID,
		Roles:    roles,
		Scopes:   scopesFromRoles(roles),
	}, nil
}
