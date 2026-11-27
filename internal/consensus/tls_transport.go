package consensus

import (
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// tlsStreamLayer implements raft.StreamLayer over mutually authenticated TLS
// so Raft RPCs (log replication, votes, snapshots) are encrypted and only
// nodes holding a certificate from the cluster CA can participate.
type tlsStreamLayer struct {
	net.Listener
	advertise net.Addr
	client    *tls.Config
}

// Dial implements raft.StreamLayer.
func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(d, "tcp", string(address), t.client)
}

// Addr implements net.Listener, returning the advertised address.
func (t *tlsStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.Listener.Addr()
}

// newTLSTransport listens on bindAddr with TLS. server must require and
// verify client certificates; client must trust the same CA.
func newTLSTransport(bindAddr string, advertise net.Addr, server, client *tls.Config) (*raft.NetworkTransport, error) {
	if server == nil || client == nil {
		return nil, errors.New("raft TLS requires server and client TLS configs")
	}
	if server.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, errors.New("raft TLS requires mutual TLS (tls.client_auth: true)")
	}
	ln, err := tls.Listen("tcp", bindAddr, server)
	if err != nil {
		return nil, err
	}
	stream := &tlsStreamLayer{Listener: ln, advertise: advertise, client: client}
	return raft.NewNetworkTransport(stream, 3, 10*time.Second, nil), nil
}
