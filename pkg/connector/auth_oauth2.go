package connector

import (
	"context"
	"net/http"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// oauth2Authenticator handles OAuth2 token management with auto-refresh.
// Supports both client credentials (server-to-server) and authorization code
// (user-delegated with refresh token) flows.
type oauth2Authenticator struct {
	tokenSource oauth2.TokenSource
	mu          sync.Mutex
	token       *oauth2.Token
}

func newOAuth2Authenticator(cfg *AuthConfig) (*oauth2Authenticator, error) {
	a := &oauth2Authenticator{}

	if cfg.RefreshToken != "" {
		// Authorization code flow — use refresh token
		oauthCfg := &oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: cfg.TokenURL,
				AuthURL:  cfg.AuthURL,
			},
			Scopes: cfg.Scopes,
		}
		token := &oauth2.Token{RefreshToken: cfg.RefreshToken}
		a.tokenSource = oauthCfg.TokenSource(context.Background(), token)
	} else {
		// Client credentials flow — server-to-server
		ccCfg := &clientcredentials.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			TokenURL:     cfg.TokenURL,
			Scopes:       cfg.Scopes,
		}
		a.tokenSource = ccCfg.TokenSource(context.Background())
	}

	// Pre-fetch initial token
	token, err := a.tokenSource.Token()
	if err != nil {
		return nil, err
	}
	a.token = token

	return a, nil
}

func (a *oauth2Authenticator) Apply(req *http.Request) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Auto-refresh expired tokens so a stale cached token is never sent.
	if a.token == nil || !a.token.Valid() {
		token, err := a.tokenSource.Token()
		if err != nil {
			return err
		}
		a.token = token
	}
	a.token.SetAuthHeader(req)
	return nil
}

func (a *oauth2Authenticator) Refresh(ctx context.Context) error {
	token, err := a.tokenSource.Token()
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.token = token
	a.mu.Unlock()
	return nil
}
