package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func signAsymJWT(t *testing.T, alg, kid string, key any, claims map[string]any) string {
	t.Helper()
	hdr := map[string]any{"alg": alg, "typ": "JWT"}
	if kid != "" {
		hdr["kid"] = kid
	}
	h, _ := json.Marshal(hdr)
	c, _ := json.Marshal(claims)
	input := b64(h) + "." + b64(c)
	digest := sha256.Sum256([]byte(input))
	var sig []byte
	switch k := key.(type) {
	case *rsa.PrivateKey:
		var err error
		sig, err = rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, digest[:])
		require.NoError(t, err)
	case *ecdsa.PrivateKey:
		r, s, err := ecdsa.Sign(rand.Reader, k, digest[:])
		require.NoError(t, err)
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		s.FillBytes(sig[32:])
	case []byte:
		m := hmac.New(sha256.New, k)
		m.Write([]byte(input))
		sig = m.Sum(nil)
	case nil:
	}
	return input + "." + b64(sig)
}

func validClaims() map[string]any {
	return map[string]any{"sub": "u1", "tenant_id": "t1", "roles": []string{"reader"}, "exp": time.Now().Add(time.Hour).Unix()}
}

func TestJWTAsymmetric(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	rsaPubDER, _ := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	rsaPubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: rsaPubDER})

	rsaOnly := NewJWTAuthenticator(JWTConfig{PublicKey: &rsaKey.PublicKey})
	ecOnly := NewJWTAuthenticator(JWTConfig{PublicKey: &ecKey.PublicKey})

	tests := []struct {
		name    string
		authn   *JWTAuthenticator
		token   string
		wantErr bool
	}{
		{"RS256 valid", rsaOnly, signAsymJWT(t, "RS256", "", rsaKey, validClaims()), false},
		{"ES256 valid", ecOnly, signAsymJWT(t, "ES256", "", ecKey, validClaims()), false},
		{"alg confusion: HS256 signed with RSA public key", rsaOnly, signAsymJWT(t, "HS256", "", rsaPubPEM, validClaims()), true},
		{"alg none rejected", rsaOnly, signAsymJWT(t, "none", "", nil, validClaims()), true},
		{"ES256 token against RSA key", rsaOnly, signAsymJWT(t, "ES256", "", ecKey, validClaims()), true},
		{"tampered payload", rsaOnly, func() string {
			tok := signAsymJWT(t, "RS256", "", rsaKey, validClaims())
			c := validClaims()
			c["tenant_id"] = "other"
			cb, _ := json.Marshal(c)
			parts := splitToken(tok)
			return parts[0] + "." + b64(cb) + "." + parts[2]
		}(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ac, err := tt.authn.Authenticate(context.Background(), tt.token)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "t1", ac.TenantID)
		})
	}

	pk, err := ParsePublicKeyPEM(rsaPubPEM)
	require.NoError(t, err)
	assert.Equal(t, &rsaKey.PublicKey, pk)
}

func splitToken(tok string) [3]string {
	var out [3]string
	i := 0
	start := 0
	for j := 0; j < len(tok); j++ {
		if tok[j] == '.' {
			out[i] = tok[start:j]
			i++
			start = j + 1
		}
	}
	out[2] = tok[start:]
	return out
}

func TestJWKSRotation(t *testing.T) {
	k1, _ := rsa.GenerateKey(rand.Reader, 2048)
	k2, _ := rsa.GenerateKey(rand.Reader, 2048)
	current := map[string]*rsa.PrivateKey{"k1": k1}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var keys []map[string]string
		for kid, k := range current {
			keys = append(keys, map[string]string{"kty": "RSA", "kid": kid, "use": "sig",
				"n": b64(k.N.Bytes()), "e": b64(big.NewInt(int64(k.E)).Bytes())})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	}))
	defer srv.Close()

	cache := NewJWKSCache(srv.URL)
	authn := NewJWTAuthenticator(JWTConfig{JWKS: cache})
	_, err := authn.Authenticate(context.Background(), signAsymJWT(t, "RS256", "k1", k1, validClaims()))
	require.NoError(t, err)

	// Rotate: k2 appears. Unknown kid triggers a (rate-limited) refetch.
	current["k2"] = k2
	cache.mu.Lock()
	cache.lastAttempt = time.Time{}
	cache.mu.Unlock()
	_, err = authn.Authenticate(context.Background(), signAsymJWT(t, "RS256", "k2", k2, validClaims()))
	require.NoError(t, err)

	_, err = authn.Authenticate(context.Background(), signAsymJWT(t, "RS256", "k9", k2, validClaims()))
	assert.Error(t, err, "unknown kid rejected")
}
