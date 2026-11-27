package consensus

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPKI(t *testing.T) (server, client, rogue *tls.Config) {
	t.Helper()
	mk := func(tmpl, parent *x509.Certificate, pub, signer any) []byte {
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
		require.NoError(t, err)
		return der
	}
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	caCert, _ := x509.ParseCertificate(mk(caTmpl, caTmpl, &caKey.PublicKey, caKey))
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	leaf := func(serial int64, parent *x509.Certificate, signer any) tls.Certificate {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "node"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		if parent == nil {
			parent = tmpl
			signer = key
		}
		return tls.Certificate{Certificate: [][]byte{mk(tmpl, parent, &key.PublicKey, signer)}, PrivateKey: key}
	}
	node := leaf(2, caCert, caKey)
	server = &tls.Config{Certificates: []tls.Certificate{node}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	client = &tls.Config{Certificates: []tls.Certificate{node}, RootCAs: pool, MinVersion: tls.VersionTLS12}
	rogue = &tls.Config{Certificates: []tls.Certificate{leaf(3, nil, nil)}, RootCAs: pool, MinVersion: tls.VersionTLS12}
	return
}

func TestRaftTLSTransport(t *testing.T) {
	server, client, rogue := testPKI(t)
	tr, err := newTLSTransport("127.0.0.1:0", nil, server, client)
	require.NoError(t, err)
	defer tr.Close()
	addr := tr.LocalAddr()

	tests := []struct {
		name    string
		cfg     *tls.Config
		wantErr bool
	}{
		{"cluster member connects", client, false},
		{"certificate from another CA is rejected", rogue, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layer := &tlsStreamLayer{client: tt.cfg}
			conn, err := layer.Dial(addr, 2*time.Second)
			if err == nil {
				// TLS 1.3 reports client-cert rejection on first read.
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				_, err = conn.Write([]byte{0})
				if err == nil {
					_, err = io.ReadFull(conn, make([]byte, 1))
					if err == io.EOF || isTimeout(err) {
						err = nil // accepted; raft just has nothing to say to a junk byte
					}
				}
				conn.Close()
			}
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	_, err = newTLSTransport("127.0.0.1:0", nil, &tls.Config{}, client)
	assert.Error(t, err, "server without mandatory client certs is refused")
	var _ raft.StreamLayer = (*tlsStreamLayer)(nil)
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}
