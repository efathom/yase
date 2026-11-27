package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// JWKSCache fetches and caches an identity provider's JSON Web Key Set.
// Keys refresh every RefreshInterval, and on an unknown "kid" (rate-limited)
// so key rotation is picked up without a restart.
type JWKSCache struct {
	URL             string
	RefreshInterval time.Duration
	Client          *http.Client

	mu          sync.Mutex
	keys        map[string]crypto.PublicKey
	fetchedAt   time.Time
	lastAttempt time.Time
}

// NewJWKSCache creates a cache for url.
func NewJWKSCache(url string) *JWKSCache {
	return &JWKSCache{URL: url, RefreshInterval: 10 * time.Minute, Client: &http.Client{Timeout: 5 * time.Second}}
}

const jwksMinRefetch = 30 * time.Second

// Key returns the key with the given kid (or the only key when kid is empty).
func (c *JWKSCache) Key(ctx context.Context, kid string) (crypto.PublicKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stale := time.Since(c.fetchedAt) > c.RefreshInterval
	k, ok := c.pick(kid)
	if (stale || !ok) && time.Since(c.lastAttempt) > jwksMinRefetch {
		c.lastAttempt = time.Now()
		if err := c.fetch(ctx); err != nil && c.keys == nil {
			return nil, fmt.Errorf("fetch JWKS: %w", err)
		}
		k, ok = c.pick(kid)
	}
	if !ok {
		return nil, fmt.Errorf("no JWKS key for kid %q", kid)
	}
	return k, nil
}

func (c *JWKSCache) pick(kid string) (crypto.PublicKey, bool) {
	if kid == "" && len(c.keys) == 1 {
		for _, k := range c.keys {
			return k, true
		}
	}
	k, ok := c.keys[kid]
	return k, ok
}

type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (c *JWKSCache) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return err
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&set); err != nil {
		return err
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		pk, err := k.publicKey()
		if err != nil {
			continue // skip unsupported key types
		}
		keys[k.Kid] = pk
	}
	if len(keys) == 0 {
		return errors.New("JWKS contains no usable signing keys")
	}
	c.keys = keys
	c.fetchedAt = time.Now()
	return nil
}

func b64int(s string) (*big.Int, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(b), nil
}

func (k jwk) publicKey() (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		n, err1 := b64int(k.N)
		e, err2 := b64int(k.E)
		if err := errors.Join(err1, err2); err != nil {
			return nil, err
		}
		if n.BitLen() < 2048 {
			return nil, errors.New("RSA key too small")
		}
		return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
	case "EC":
		if k.Crv != "P-256" {
			return nil, errors.New("unsupported curve")
		}
		x, err1 := b64int(k.X)
		y, err2 := b64int(k.Y)
		if err := errors.Join(err1, err2); err != nil {
			return nil, err
		}
		pk := &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}
		if !pk.Curve.IsOnCurve(x, y) {
			return nil, errors.New("EC point not on curve")
		}
		return pk, nil
	}
	return nil, fmt.Errorf("unsupported kty %q", k.Kty)
}

// ParsePublicKeyPEM parses a PEM-encoded PKIX public key (RSA or P-256 EC).
func ParsePublicKeyPEM(data []byte) (crypto.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	pk, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	switch k := pk.(type) {
	case *rsa.PublicKey:
		if k.N.BitLen() < 2048 {
			return nil, errors.New("RSA key too small (min 2048 bits)")
		}
		return k, nil
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() {
			return nil, errors.New("only P-256 EC keys are supported")
		}
		return k, nil
	}
	return nil, fmt.Errorf("unsupported public key type %T", pk)
}
