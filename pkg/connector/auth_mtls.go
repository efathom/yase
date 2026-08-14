package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
)

// mtlsAuthenticator configures mutual TLS authentication.
// It modifies the HTTP client's TLS config rather than request headers.
type mtlsAuthenticator struct {
	tlsConfig *tls.Config
}

func newMTLSAuthenticator(cfg *AuthConfig) (*mtlsAuthenticator, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	if cfg.CAPath != "" {
		caCert, err := os.ReadFile(cfg.CAPath)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.RootCAs = pool
	}

	return &mtlsAuthenticator{tlsConfig: tlsCfg}, nil
}

// Apply is a no-op for mTLS — the TLS config is set on the HTTP transport, not per-request.
func (a *mtlsAuthenticator) Apply(_ *http.Request) error { return nil }

func (a *mtlsAuthenticator) Refresh(_ context.Context) error { return nil }

// TLSConfig returns the TLS configuration for use with http.Transport.
func (a *mtlsAuthenticator) TLSConfig() *tls.Config {
	return a.tlsConfig
}
