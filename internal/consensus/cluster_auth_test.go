package consensus

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterAPIRequiresToken(t *testing.T) {
	cs := (&ClusterService{}).SetToken("0123456789abcdef")
	mux := http.NewServeMux()
	cs.RegisterRoutes(mux)

	tests := []struct {
		name   string
		method string
		path   string
		token  string
	}{
		{"join without token", "POST", "/cluster/join", ""},
		{"leave with wrong token", "POST", "/cluster/leave", "nope"},
		{"topology without token", "GET", "/cluster/topology", ""},
		{"shard assign without token", "POST", "/cluster/shard/assign", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.token != "" {
				r.Header.Set(TokenHeader, tt.token)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, r)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		})
	}
}
