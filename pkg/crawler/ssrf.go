package crawler

import (
	"errors"
	"net"
)

// ErrBlockedAddress is returned when a crawl target resolves to an address
// outside the public internet.
var ErrBlockedAddress = errors.New("crawler: refusing to connect to non-public address")

// blockedCIDRs are ranges a web crawler must never reach. Fetching them turns
// the crawler into a proxy into its own network: cloud instance metadata
// (169.254.169.254) hands out credentials, and loopback reaches the management
// APIs that are bound to 127.0.0.1 precisely so they are not externally
// reachable.
var blockedCIDRs = func() []*net.IPNet {
	ranges := []string{
		"0.0.0.0/8",          // "this" network
		"10.0.0.0/8",         // RFC 1918 private
		"100.64.0.0/10",      // RFC 6598 carrier-grade NAT
		"127.0.0.0/8",        // loopback
		"169.254.0.0/16",     // RFC 3927 link-local, incl. cloud metadata
		"172.16.0.0/12",      // RFC 1918 private
		"192.0.0.0/24",       // IETF protocol assignments
		"192.0.2.0/24",       // TEST-NET-1
		"192.168.0.0/16",     // RFC 1918 private
		"198.18.0.0/15",      // benchmarking
		"198.51.100.0/24",    // TEST-NET-2
		"203.0.113.0/24",     // TEST-NET-3
		"224.0.0.0/4",        // multicast
		"240.0.0.0/4",        // reserved
		"255.255.255.255/32", // broadcast
		"::/128",             // unspecified
		"::1/128",            // IPv6 loopback
		"fc00::/7",           // IPv6 unique local
		"fe80::/10",          // IPv6 link-local
		"ff00::/8",           // IPv6 multicast
	}
	nets := make([]*net.IPNet, 0, len(ranges))
	for _, r := range ranges {
		if _, n, err := net.ParseCIDR(r); err == nil {
			nets = append(nets, n)
		}
	}
	return nets
}()

// IsBlockedAddress reports whether ip is outside the public internet and so
// must not be crawled.
//
// IPv4-mapped IPv6 addresses (::ffff:127.0.0.1) are normalized first, so an
// attacker cannot evade the check by changing address family.
func IsBlockedAddress(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if !ip.IsGlobalUnicast() {
		return true
	}
	for _, n := range blockedCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
