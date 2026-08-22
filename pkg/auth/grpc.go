package auth

import (
	"crypto/tls"

	"github.com/efathom/yase/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func toTLSConfig(c config.TLSConfig) TLSConfig {
	return TLSConfig{
		Enabled:    c.Enabled,
		CertPath:   c.CertPath,
		KeyPath:    c.KeyPath,
		CAPath:     c.CAPath,
		ClientAuth: c.ClientAuth,
	}
}

// GRPCServerOptions builds grpc.ServerOption(s) wiring panic recovery, auth
// interceptors (when authn is non-nil), and inbound TLS (when tlsCfg is
// non-nil).
//
// Recovery is always installed and always outermost, so a panic anywhere in
// the chain — including in an auth interceptor — fails the one RPC instead of
// terminating the process.
func GRPCServerOptions(authn Authenticator, tlsCfg *tls.Config) []grpc.ServerOption {
	unary := []grpc.UnaryServerInterceptor{RecoveryUnaryInterceptor()}
	stream := []grpc.StreamServerInterceptor{RecoveryStreamInterceptor()}

	if authn != nil {
		unary = append(unary, UnaryServerInterceptor(authn))
		stream = append(stream, StreamServerInterceptor(authn))
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	}
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	return opts
}

// ServerAuthTLS builds the full gRPC server option set from application config.
// Auth is applied only when a concrete auth method is configured; TLS only
// when tls.enabled is true.
func ServerAuthTLS(authCfg config.AuthConfig, tlsCfg config.TLSConfig) ([]grpc.ServerOption, error) {
	authn, err := NewFromConfig(authCfg)
	if err != nil {
		return nil, err
	}
	tlsConf, err := LoadTLSConfig(toTLSConfig(tlsCfg))
	if err != nil {
		return nil, err
	}
	return GRPCServerOptions(authn, tlsConf), nil
}

// ClientDialOptions builds the full outbound gRPC dial option set: transport
// security plus, when auth.client_token is configured, the per-RPC credentials
// that satisfy the server's auth interceptor.
//
// Use this rather than ClientDialOption for any client that talks to a server
// with auth enabled.
func ClientDialOptions(authCfg config.AuthConfig, tlsCfg config.TLSConfig) ([]grpc.DialOption, error) {
	transport, err := ClientDialOption(tlsCfg)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{transport}

	if authCfg.Enabled && authCfg.ClientToken != "" {
		creds := NewTokenCredentials(authCfg.ClientToken)
		creds.RequireTLS = tlsCfg.Enabled
		opts = append(opts, grpc.WithPerRPCCredentials(creds))
	}
	return opts, nil
}

// ClientDialOption builds the outbound gRPC dial option (TLS or insecure).
func ClientDialOption(tlsCfg config.TLSConfig) (grpc.DialOption, error) {
	tlsConf, err := LoadClientTLSConfig(toTLSConfig(tlsCfg))
	if err != nil {
		return nil, err
	}
	if tlsConf == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)), nil
}
