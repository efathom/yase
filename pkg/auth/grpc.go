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

// GRPCServerOptions builds grpc.ServerOption(s) wiring auth interceptors
// (when authn is non-nil) and inbound TLS (when tlsCfg is non-nil).
func GRPCServerOptions(authn Authenticator, tlsCfg *tls.Config) []grpc.ServerOption {
	var opts []grpc.ServerOption
	if authn != nil {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(UnaryServerInterceptor(authn)),
			grpc.ChainStreamInterceptor(StreamServerInterceptor(authn)),
		)
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
