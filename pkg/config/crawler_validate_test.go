package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validCrawlerBase returns a config that passes validation, so each test can
// vary only the crawler field under test.
func validCrawlerBase(t *testing.T) *Config {
	t.Helper()
	c, err := Load("")
	require.NoError(t, err)
	return c
}

// H-06: the master scheduler's /seed endpoint injects crawl targets. Binding
// it to a public interface without a token lets anyone drive the crawler.
func TestValidateRejectsPublicMasterAddrWithoutToken(t *testing.T) {
	c := validCrawlerBase(t)
	c.Crawler.MasterAddr = ":9080"
	c.Crawler.MasterToken = ""

	err := c.Validate()

	require.Error(t, err, "an all-interfaces master address must require a token")
	assert.Contains(t, err.Error(), "master_token")
}

func TestValidateAllowsPublicMasterAddrWithToken(t *testing.T) {
	c := validCrawlerBase(t)
	c.Crawler.MasterAddr = ":9080"
	c.Crawler.MasterToken = "a-sufficiently-long-shared-secret"

	assert.NoError(t, c.Validate())
}

func TestValidateAllowsLoopbackMasterAddrWithoutToken(t *testing.T) {
	c := validCrawlerBase(t)
	c.Crawler.MasterAddr = "127.0.0.1:9080"
	c.Crawler.MasterToken = ""

	assert.NoError(t, c.Validate(),
		"loopback needs no token — it is not externally reachable")
}

func TestValidateRejectsShortMasterToken(t *testing.T) {
	c := validCrawlerBase(t)
	c.Crawler.MasterAddr = "0.0.0.0:9080"
	c.Crawler.MasterToken = "short"

	err := c.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "master_token")
}

// The default configuration must be safe with no operator action.
func TestDefaultMasterAddrIsLoopback(t *testing.T) {
	c := validCrawlerBase(t)

	assert.Equal(t, "127.0.0.1:9080", c.Crawler.MasterAddr,
		"the master API must default to loopback")
	assert.False(t, c.Crawler.AllowPrivateAddresses,
		"the SSRF guard must default to on")
}
