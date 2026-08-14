package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAPIKeyAuthenticator(t *testing.T) {
	auth := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "key-admin", TenantID: "acme", Roles: []string{"admin"}},
		{Key: "key-reader", TenantID: "acme", Roles: []string{"reader"}},
		{Key: "key-other", TenantID: "other-co", Roles: []string{"writer"}},
	})

	tests := []struct {
		name     string
		token    string
		wantErr  bool
		wantTID  string
		wantRole string
	}{
		{"admin key", "key-admin", false, "acme", "admin"},
		{"reader key", "key-reader", false, "acme", "reader"},
		{"other tenant", "key-other", false, "other-co", "writer"},
		{"invalid key", "key-bad", true, "", ""},
		{"empty key", "", true, "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ac, err := auth.Authenticate(context.Background(), tt.token)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ac.TenantID != tt.wantTID {
				t.Errorf("tenant: got %q, want %q", ac.TenantID, tt.wantTID)
			}
			if !ac.HasRole(tt.wantRole) {
				t.Errorf("expected role %q in %v", tt.wantRole, ac.Roles)
			}
		})
	}
}

func TestAPIKeyScopesFromRoles(t *testing.T) {
	auth := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "admin-key", TenantID: "t", Roles: []string{"admin"}},
		{Key: "reader-key", TenantID: "t", Roles: []string{"reader"}},
	})

	ac, _ := auth.Authenticate(context.Background(), "admin-key")
	if !ac.HasScope("search") || !ac.HasScope("ingest") || !ac.HasScope("delete") || !ac.HasScope("admin") {
		t.Errorf("admin should have all scopes, got %v", ac.Scopes)
	}

	ac2, _ := auth.Authenticate(context.Background(), "reader-key")
	if !ac2.HasScope("search") {
		t.Error("reader should have search scope")
	}
	if ac2.HasScope("ingest") || ac2.HasScope("delete") {
		t.Errorf("reader should not have write scopes, got %v", ac2.Scopes)
	}
}

func TestJWTAuthenticator(t *testing.T) {
	secret := "test-secret-256bits-long-enough!"
	auth := NewJWTAuthenticator(JWTConfig{
		Secret:      secret,
		Issuer:      "test-issuer",
		Audience:    "yase",
		TenantClaim: "org_id",
		RolesClaim:  "roles",
	})

	// Build a valid JWT
	token := buildTestJWT(t, secret, map[string]interface{}{
		"sub":    "user-123",
		"iss":    "test-issuer",
		"aud":    "yase",
		"org_id": "acme-corp",
		"roles":  []string{"admin"},
		"exp":    float64(time.Now().Add(1 * time.Hour).Unix()),
	})

	ac, err := auth.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("valid JWT failed: %v", err)
	}
	if ac.TenantID != "acme-corp" {
		t.Errorf("tenant: got %q", ac.TenantID)
	}
	if ac.UserID != "user-123" {
		t.Errorf("user: got %q", ac.UserID)
	}
	if !ac.HasRole("admin") {
		t.Errorf("expected admin role")
	}
}

func TestJWTExpired(t *testing.T) {
	secret := "test-secret-256bits-long-enough!"
	auth := NewJWTAuthenticator(JWTConfig{Secret: secret})

	token := buildTestJWT(t, secret, map[string]interface{}{
		"sub": "user",
		"exp": float64(time.Now().Add(-1 * time.Hour).Unix()),
	})

	_, err := auth.Authenticate(context.Background(), token)
	if err == nil {
		t.Error("expected error for expired token")
	}
}

func TestJWTBadSignature(t *testing.T) {
	auth := NewJWTAuthenticator(JWTConfig{Secret: "correct-secret"})
	token := buildTestJWT(t, "wrong-secret", map[string]interface{}{
		"sub": "user",
		"exp": float64(time.Now().Add(1 * time.Hour).Unix()),
	})

	_, err := auth.Authenticate(context.Background(), token)
	if err == nil {
		t.Error("expected error for bad signature")
	}
}

func TestHTTPMiddleware(t *testing.T) {
	auth := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "valid-key", TenantID: "t1", Roles: []string{"reader"}},
	})

	handler := HTTPMiddleware(auth)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ac := FromContext(r.Context())
		if ac == nil {
			t.Error("expected auth context")
			return
		}
		fmt.Fprintf(w, "tenant=%s", ac.TenantID)
	}))

	// Valid key via X-API-Key
	req := httptest.NewRequest("GET", "/search", nil)
	req.Header.Set("X-API-Key", "valid-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("valid key: got %d, want 200", w.Code)
	}

	// Valid key via Authorization: Bearer
	req2 := httptest.NewRequest("GET", "/search", nil)
	req2.Header.Set("Authorization", "Bearer valid-key")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Errorf("bearer key: got %d, want 200", w2.Code)
	}

	// No key
	req3 := httptest.NewRequest("GET", "/search", nil)
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)
	if w3.Code != 401 {
		t.Errorf("no key: got %d, want 401", w3.Code)
	}

	// Invalid key
	req4 := httptest.NewRequest("GET", "/search", nil)
	req4.Header.Set("X-API-Key", "bad-key")
	w4 := httptest.NewRecorder()
	handler.ServeHTTP(w4, req4)
	if w4.Code != 401 {
		t.Errorf("bad key: got %d, want 401", w4.Code)
	}

	// Health endpoint bypasses auth — use a handler that doesn't need auth context
	healthHandler := HTTPMiddleware(auth)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	req5 := httptest.NewRequest("GET", "/health", nil)
	w5 := httptest.NewRecorder()
	healthHandler.ServeHTTP(w5, req5)
	if w5.Code != 200 {
		t.Errorf("health bypass: got %d, want 200", w5.Code)
	}
}

func TestRequireScope(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	handler := RequireScope("admin")(inner)

	// No auth context → 403
	req := httptest.NewRequest("GET", "/admin", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Errorf("no context: got %d, want 403", w.Code)
	}

	// Auth context without scope → 403
	ctx := WithContext(context.Background(), &AuthContext{Roles: []string{"reader"}, Scopes: []string{"search"}})
	req2 := httptest.NewRequest("GET", "/admin", nil).WithContext(ctx)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != 403 {
		t.Errorf("wrong scope: got %d, want 403", w2.Code)
	}

	// Auth context with scope → 200
	ctx3 := WithContext(context.Background(), &AuthContext{Roles: []string{"admin"}, Scopes: []string{"admin", "search"}})
	req3 := httptest.NewRequest("GET", "/admin", nil).WithContext(ctx3)
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)
	if w3.Code != 200 {
		t.Errorf("has scope: got %d, want 200", w3.Code)
	}
}

func TestAuthContext(t *testing.T) {
	// Nil context returns nil
	if FromContext(context.Background()) != nil {
		t.Error("expected nil from empty context")
	}

	ac := &AuthContext{TenantID: "t1", Roles: []string{"admin"}}
	ctx := WithContext(context.Background(), ac)
	got := FromContext(ctx)
	if got == nil || got.TenantID != "t1" {
		t.Errorf("got %v", got)
	}
}

func buildTestJWT(t *testing.T, secret string, claims map[string]interface{}) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(claims)
	payloadEnc := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := header + "." + payloadEnc
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}
