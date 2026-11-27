package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
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

	// PublicKey verifies RS256/ES256 tokens (an *rsa.PublicKey or a P-256
	// *ecdsa.PublicKey). JWKS fetches keys from an identity provider instead.
	PublicKey crypto.PublicKey `json:"-" yaml:"-"`
	JWKS      *JWKSCache       `json:"-" yaml:"-"`
}

// JWTAuthenticator validates JWTs signed with HS256 (shared secret), or
// RS256/ES256 (static public key or JWKS). Only algorithms backed by a
// configured key are accepted, which rules out algorithm-confusion attacks
// (e.g. an HS256 token "signed" with an RSA public key).
type JWTAuthenticator struct {
	secret        []byte
	publicKey     crypto.PublicKey
	jwks          *JWKSCache
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
		publicKey:     cfg.PublicKey,
		jwks:          cfg.JWKS,
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
func (j *JWTAuthenticator) Authenticate(ctx context.Context, token string) (*AuthContext, error) {
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
	alg, _ := header["alg"].(string)
	kid, _ := header["kid"].(string)
	if err := j.verify(ctx, alg, kid, parts[0]+"."+parts[1], parts[2]); err != nil {
		return nil, err
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

// verify checks the signature with the key that matches alg. An algorithm is
// accepted only when a key of the matching type is configured.
func (j *JWTAuthenticator) verify(ctx context.Context, alg, kid, signingInput, sigB64 string) error {
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("invalid JWT signature encoding")
	}
	digest := sha256.Sum256([]byte(signingInput))

	switch alg {
	case "HS256":
		if len(j.secret) == 0 {
			return fmt.Errorf("unsupported JWT algorithm %q", alg)
		}
		mac := hmac.New(sha256.New, j.secret)
		mac.Write([]byte(signingInput))
		if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
			return fmt.Errorf("invalid JWT signature")
		}
		return nil
	case "RS256", "ES256":
		key, err := j.lookupKey(ctx, kid)
		if err != nil {
			return err
		}
		switch k := key.(type) {
		case *rsa.PublicKey:
			if alg != "RS256" {
				return fmt.Errorf("JWT algorithm %q does not match RSA key", alg)
			}
			if rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], sig) != nil {
				return fmt.Errorf("invalid JWT signature")
			}
			return nil
		case *ecdsa.PublicKey:
			if alg != "ES256" || k.Curve.Params().Name != "P-256" || len(sig) != 64 {
				return fmt.Errorf("JWT algorithm %q does not match EC key", alg)
			}
			r := new(big.Int).SetBytes(sig[:32])
			s := new(big.Int).SetBytes(sig[32:])
			if !ecdsa.Verify(k, digest[:], r, s) {
				return fmt.Errorf("invalid JWT signature")
			}
			return nil
		}
		return fmt.Errorf("unsupported JWT key type")
	}
	return fmt.Errorf("unsupported JWT algorithm %q", alg)
}

func (j *JWTAuthenticator) lookupKey(ctx context.Context, kid string) (crypto.PublicKey, error) {
	if j.jwks != nil {
		return j.jwks.Key(ctx, kid)
	}
	if j.publicKey != nil {
		return j.publicKey, nil
	}
	return nil, fmt.Errorf("no public key configured for asymmetric JWTs")
}
