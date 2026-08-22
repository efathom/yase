package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSecret = "test-secret-at-least-16-chars"

// signJWT builds an HS256 token from the given claims.
func signJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	enc := base64.RawURLEncoding

	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	require.NoError(t, err)
	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	signingInput := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(signingInput))
	return signingInput + "." + enc.EncodeToString(mac.Sum(nil))
}

func baseClaims() map[string]any {
	return map[string]any{
		"sub": "user-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
}

// M-13: RFC 7519 §4.1.3 allows "aud" to be a string OR an array of strings.
// Most identity providers emit the array form for multi-audience tokens.
func TestJWTAcceptsAudienceArray(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret, Audience: "yase-api"})

	claims := baseClaims()
	claims["aud"] = []string{"other-service", "yase-api"}

	ac, err := j.Authenticate(context.Background(), signJWT(t, claims))

	require.NoError(t, err, "an array audience containing the expected value must be accepted")
	assert.Equal(t, "user-1", ac.UserID)
}

func TestJWTRejectsAudienceArrayWithoutMatch(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret, Audience: "yase-api"})

	claims := baseClaims()
	claims["aud"] = []string{"other-service", "third-service"}

	_, err := j.Authenticate(context.Background(), signJWT(t, claims))

	require.Error(t, err, "an array audience with no matching value must be rejected")
}

func TestJWTStillAcceptsStringAudience(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret, Audience: "yase-api"})

	claims := baseClaims()
	claims["aud"] = "yase-api"

	_, err := j.Authenticate(context.Background(), signJWT(t, claims))
	require.NoError(t, err)
}

// L-17: a token asserting no roles previously became a reader, silently
// granting the search scope.
func TestJWTWithNoRolesClaimGetsNoRolesByDefault(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret})

	ac, err := j.Authenticate(context.Background(), signJWT(t, baseClaims()))
	require.NoError(t, err)

	assert.Empty(t, ac.Roles, "a token with no roles claim must not be granted one")
	assert.False(t, ac.HasScope("search"),
		"no roles means no scopes — the fallback must not grant read access")
}

// The fallback is still available, but only when explicitly configured.
func TestJWTDefaultRolesAreConfigurable(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret, DefaultRoles: []string{"reader"}})

	ac, err := j.Authenticate(context.Background(), signJWT(t, baseClaims()))
	require.NoError(t, err)

	assert.Equal(t, []string{"reader"}, ac.Roles)
	assert.True(t, ac.HasScope("search"))
}

// L-17: with multi-tenancy on, a token carrying no tenant claim would produce
// an unscoped context that InjectTenantFilter leaves wide open.
func TestJWTCanRequireTenantClaim(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret, RequireTenant: true})

	_, err := j.Authenticate(context.Background(), signJWT(t, baseClaims()))

	require.Error(t, err, "a token with no tenant claim must be rejected when tenancy is required")
	assert.Contains(t, err.Error(), "tenant")
}

func TestJWTWithTenantClaimPassesTenantRequirement(t *testing.T) {
	j := NewJWTAuthenticator(JWTConfig{Secret: testSecret, RequireTenant: true})

	claims := baseClaims()
	claims["tenant_id"] = "tenant-a"

	ac, err := j.Authenticate(context.Background(), signJWT(t, claims))
	require.NoError(t, err)
	assert.Equal(t, "tenant-a", ac.TenantID)
}
