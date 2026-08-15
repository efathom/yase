package auth

import (
	"fmt"

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
		if len(cfg.JWT.Secret) < 16 {
			return nil, fmt.Errorf("auth.jwt.secret: must be at least 16 characters")
		}
		return NewJWTAuthenticator(JWTConfig{
			Secret:      cfg.JWT.Secret,
			Issuer:      cfg.JWT.Issuer,
			Audience:    cfg.JWT.Audience,
			TenantClaim: cfg.JWT.TenantClaim,
			RolesClaim:  cfg.JWT.RolesClaim,
		}), nil
	default:
		return nil, fmt.Errorf("unknown auth method %q", cfg.Method)
	}
}
