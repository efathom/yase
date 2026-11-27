package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helm passes list settings as comma-separated env vars.
func TestListSettingsFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want []string
	}{
		{"two proxies", "10.0.0.0/8,192.168.0.0/16", []string{"10.0.0.0/8", "192.168.0.0/16"}},
		{"single", "10.0.0.1", []string{"10.0.0.1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("YASE_SERVER_TRUSTED_PROXIES", tt.env)
			cfg, err := Load("")
			require.NoError(t, err)
			assert.Equal(t, tt.want, cfg.Server.TrustedProxies)
		})
	}
}
