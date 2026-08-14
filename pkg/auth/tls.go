package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig mirrors config.TLSConfig for the auth package.
type TLSConfig struct {
	Enabled    bool
	CertPath   string
	KeyPath    string
	CAPath     string
	ClientAuth bool
}

// LoadTLSConfig creates a *tls.Config from certificate files.
// Used by both HTTP and gRPC servers for inbound TLS.
func LoadTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
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
		tlsCfg.ClientCAs = pool
		if cfg.ClientAuth {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	return tlsCfg, nil
}

// LoadClientTLSConfig builds an outbound client *tls.Config: it trusts the
// provided CA for server verification and optionally presents a client cert.
func LoadClientTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

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

	if cfg.CertPath != "" && cfg.KeyPath != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}
