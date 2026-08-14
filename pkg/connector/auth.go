package connector

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
)

// AuthMethod identifies the type of authentication a connector uses.
type AuthMethod string

const (
	AuthOAuth2  AuthMethod = "oauth2"
	AuthAPIKey  AuthMethod = "api_key"
	AuthBasic   AuthMethod = "basic"
	AuthBearer  AuthMethod = "bearer"
	AuthMTLS    AuthMethod = "mtls"
	AuthSession AuthMethod = "session"
)

// AuthConfig holds credentials for any supported authentication method.
// Only the fields relevant to the chosen Method need to be populated.
type AuthConfig struct {
	Method AuthMethod `json:"method" yaml:"method"`

	// OAuth2
	ClientID     string   `json:"client_id,omitempty" yaml:"client_id"`
	ClientSecret string   `json:"client_secret,omitempty" yaml:"client_secret"`
	TokenURL     string   `json:"token_url,omitempty" yaml:"token_url"`
	AuthURL      string   `json:"auth_url,omitempty" yaml:"auth_url"`
	Scopes       []string `json:"scopes,omitempty" yaml:"scopes"`
	RefreshToken string   `json:"refresh_token,omitempty" yaml:"refresh_token"`

	// API Key / Bearer
	APIKey     string `json:"api_key,omitempty" yaml:"api_key"`
	HeaderName string `json:"header_name,omitempty" yaml:"header_name"` // default "Authorization"
	Prefix     string `json:"prefix,omitempty" yaml:"prefix"`           // e.g., "Bearer", "Token"

	// Basic Auth
	Username string `json:"username,omitempty" yaml:"username"`
	Password string `json:"password,omitempty" yaml:"password"`

	// mTLS
	CertPath string `json:"cert_path,omitempty" yaml:"cert_path"`
	KeyPath  string `json:"key_path,omitempty" yaml:"key_path"`
	CAPath   string `json:"ca_path,omitempty" yaml:"ca_path"`
}

// Authenticator applies credentials to HTTP requests and handles token refresh.
type Authenticator interface {
	// Apply adds authentication headers/TLS config to an outgoing request.
	Apply(req *http.Request) error
	// Refresh renews credentials if needed (e.g., OAuth2 token refresh).
	// No-op for static auth methods.
	Refresh(ctx context.Context) error
}

// TLSConfigurer is implemented by authenticators that configure TLS at the
// transport level (e.g., mTLS) rather than via request headers.
type TLSConfigurer interface {
	TLSConfig() *tls.Config
}

// NewAuthenticator creates an Authenticator based on the AuthConfig method.
func NewAuthenticator(cfg *AuthConfig) (Authenticator, error) {
	if cfg == nil {
		return &noopAuth{}, nil
	}

	switch cfg.Method {
	case AuthOAuth2:
		return newOAuth2Authenticator(cfg)
	case AuthAPIKey:
		return newAPIKeyAuthenticator(cfg), nil
	case AuthBasic:
		return newBasicAuthenticator(cfg), nil
	case AuthBearer:
		return newBearerAuthenticator(cfg), nil
	case AuthMTLS:
		return newMTLSAuthenticator(cfg)
	case AuthSession:
		return newBearerAuthenticator(cfg), nil
	case "":
		return &noopAuth{}, nil
	default:
		return nil, fmt.Errorf("unsupported auth method: %q", cfg.Method)
	}
}

// noopAuth is used when no authentication is needed.
type noopAuth struct{}

func (n *noopAuth) Apply(_ *http.Request) error     { return nil }
func (n *noopAuth) Refresh(_ context.Context) error { return nil }
