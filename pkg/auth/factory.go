package auth

import (
	"crypto"
	"fmt"
	"os"
	"strings"

	"github.com/efathom/yase/pkg/config"
)

// NewFromConfig builds an Authenticator from the application auth configuration.
// Returns (nil, nil) when auth is disabled or method is "none".
func NewFromConfig(cfg config.AuthConfig) (Authenticator, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	switch cfg.Method {
	case "", "none":
		return nil, nil
	case "api_key":
		entries := make([]APIKeyEntry, 0, len(cfg.APIKeys))
		for _, k := range cfg.APIKeys {
			entries = append(entries, APIKeyEntry{
				Key:      k.Key,
				TenantID: k.TenantID,
				UserID:   k.UserID,
				Roles:    k.Roles,
			})
		}
		return NewAPIKeyAuthenticator(entries), nil
	case "jwt":
		if cfg.JWT.Secret == "" && cfg.JWT.PublicKeyPath == "" && cfg.JWT.JWKSURL == "" {
			return nil, fmt.Errorf("auth.jwt: one of secret, public_key_path or jwks_url is required")
		}
		if cfg.JWT.Secret != "" && len(cfg.JWT.Secret) < 16 {
			return nil, fmt.Errorf("auth.jwt.secret: must be at least 16 characters")
		}
		var pub crypto.PublicKey
		if cfg.JWT.PublicKeyPath != "" {
			data, err := os.ReadFile(cfg.JWT.PublicKeyPath)
			if err != nil {
				return nil, fmt.Errorf("auth.jwt.public_key_path: %w", err)
			}
			if pub, err = ParsePublicKeyPEM(data); err != nil {
				return nil, fmt.Errorf("auth.jwt.public_key_path: %w", err)
			}
		}
		var jwks *JWKSCache
		if cfg.JWT.JWKSURL != "" {
			if !strings.HasPrefix(cfg.JWT.JWKSURL, "https://") {
				return nil, fmt.Errorf("auth.jwt.jwks_url: must use https")
			}
			jwks = NewJWKSCache(cfg.JWT.JWKSURL)
		}
		return NewJWTAuthenticator(JWTConfig{
			PublicKey:     pub,
			JWKS:          jwks,
			Secret:        cfg.JWT.Secret,
			Issuer:        cfg.JWT.Issuer,
			Audience:      cfg.JWT.Audience,
			TenantClaim:   cfg.JWT.TenantClaim,
			RolesClaim:    cfg.JWT.RolesClaim,
			DefaultRoles:  cfg.JWT.DefaultRoles,
			RequireTenant: cfg.JWT.RequireTenant,
		}), nil
	default:
		return nil, fmt.Errorf("unknown auth method %q", cfg.Method)
	}
}
