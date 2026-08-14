package connector

import (
	"context"
	"encoding/base64"
	"net/http"
)

// apiKeyAuthenticator injects an API key into request headers.
type apiKeyAuthenticator struct {
	headerName string
	value      string
}

func newAPIKeyAuthenticator(cfg *AuthConfig) *apiKeyAuthenticator {
	headerName := cfg.HeaderName
	if headerName == "" {
		headerName = "X-API-Key"
	}
	value := cfg.APIKey
	if cfg.Prefix != "" {
		value = cfg.Prefix + " " + value
	}
	return &apiKeyAuthenticator{headerName: headerName, value: value}
}

func (a *apiKeyAuthenticator) Apply(req *http.Request) error {
	req.Header.Set(a.headerName, a.value)
	return nil
}

func (a *apiKeyAuthenticator) Refresh(_ context.Context) error { return nil }

// basicAuthenticator sets the Authorization header with base64-encoded credentials.
type basicAuthenticator struct {
	username string
	password string
}

func newBasicAuthenticator(cfg *AuthConfig) *basicAuthenticator {
	return &basicAuthenticator{username: cfg.Username, password: cfg.Password}
}

func (a *basicAuthenticator) Apply(req *http.Request) error {
	cred := base64.StdEncoding.EncodeToString([]byte(a.username + ":" + a.password))
	req.Header.Set("Authorization", "Basic "+cred)
	return nil
}

func (a *basicAuthenticator) Refresh(_ context.Context) error { return nil }

// bearerAuthenticator sets a Bearer token in the Authorization header.
type bearerAuthenticator struct {
	token string
}

func newBearerAuthenticator(cfg *AuthConfig) *bearerAuthenticator {
	token := cfg.APIKey
	if token == "" {
		token = cfg.RefreshToken // fallback for session tokens
	}
	return &bearerAuthenticator{token: token}
}

func (a *bearerAuthenticator) Apply(req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+a.token)
	return nil
}

func (a *bearerAuthenticator) Refresh(_ context.Context) error { return nil }
