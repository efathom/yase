package crawler

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// H-06: a crawler that follows any URL it is handed will fetch cloud metadata
// and internal services and index the results. Blocking happens on the
// resolved IP, which is the one place a redirect cannot slip past.
func TestIsBlockedAddressRejectsInternalRanges(t *testing.T) {
	tests := []struct {
		name string
		ip   string
	}{
		{"IMDS link-local", "169.254.169.254"},
		{"IPv6 link-local", "fe80::1"},
		{"loopback", "127.0.0.1"},
		{"loopback alternate", "127.1.2.3"},
		{"IPv6 loopback", "::1"},
		{"private 10/8", "10.0.0.1"},
		{"private 172.16/12", "172.16.5.4"},
		{"private 192.168/16", "192.168.1.1"},
		{"unique local IPv6", "fd00::1"},
		{"unspecified", "0.0.0.0"},
		{"carrier-grade NAT", "100.64.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "test fixture %q must parse", tt.ip)
			assert.True(t, IsBlockedAddress(ip), "%s must be blocked", tt.ip)
		})
	}
}

func TestIsBlockedAddressAllowsPublicAddresses(t *testing.T) {
	for _, s := range []string{"93.184.216.34", "8.8.8.8", "2606:2800:220:1:248:1893:25c8:1946"} {
		ip := net.ParseIP(s)
		require.NotNil(t, ip)
		assert.False(t, IsBlockedAddress(ip), "%s is public and must be allowed", s)
	}
}

// The dialer must refuse a connection to a blocked address rather than
// resolving and connecting to it.
func TestDialContextRefusesInternalAddress(t *testing.T) {
	d := NewDNSCache(time.Minute)
	t.Cleanup(d.Close)

	_, err := d.customDialContext(context.Background(), "tcp", "169.254.169.254:80")

	require.Error(t, err, "dialing cloud metadata must be refused")
	assert.ErrorIs(t, err, ErrBlockedAddress)
}

func TestDialContextRefusesLoopback(t *testing.T) {
	d := NewDNSCache(time.Minute)
	t.Cleanup(d.Close)

	_, err := d.customDialContext(context.Background(), "tcp", "127.0.0.1:9080")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBlockedAddress)
}

// Deployments that legitimately crawl an internal network can opt out.
func TestAllowPrivateAddressesDisablesTheGuard(t *testing.T) {
	d := NewDNSCache(time.Minute)
	d.AllowPrivateAddresses = true
	t.Cleanup(d.Close)

	// Nothing is listening, so the dial fails — but it must not be refused
	// by the SSRF guard.
	_, err := d.customDialContext(context.Background(), "tcp", "127.0.0.1:1")
	if err != nil {
		assert.NotErrorIs(t, err, ErrBlockedAddress,
			"the guard must be off when private addresses are allowed")
	}
}
