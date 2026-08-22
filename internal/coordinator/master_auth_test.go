package coordinator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/efathom/yase/pkg/crawler"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testScheduler builds a scheduler backed by an in-memory Redis so the Bloom
// filter is usable.
func testScheduler(t *testing.T) *MasterScheduler {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	bloom := crawler.NewBloomFilter(rdb, "test:frontier")
	return NewMasterScheduler(bloom, make(chan string, 10), nil)
}

func seedBody(t *testing.T, urls []string) []byte {
	t.Helper()
	b, err := json.Marshal(urls)
	require.NoError(t, err)
	return b
}

// H-06: /seed injects crawl targets. Once the master API is reachable off
// loopback it must demand the shared token.
func TestSeedHandlerRejectsMissingToken(t *testing.T) {
	ms := testScheduler(t)
	ms.APIToken = "a-sufficiently-long-shared-secret"

	req := httptest.NewRequest("POST", "/seed",
		bytes.NewReader(seedBody(t, []string{"http://169.254.169.254/latest/meta-data/"})))
	w := httptest.NewRecorder()

	ms.SeedHandler(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code,
		"seeding must require the configured token")
}

func TestSeedHandlerRejectsWrongToken(t *testing.T) {
	ms := testScheduler(t)
	ms.APIToken = "a-sufficiently-long-shared-secret"

	req := httptest.NewRequest("POST", "/seed", bytes.NewReader(seedBody(t, []string{"http://example.com/"})))
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()

	ms.SeedHandler(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestSeedHandlerAcceptsCorrectToken(t *testing.T) {
	ms := testScheduler(t)
	ms.APIToken = "a-sufficiently-long-shared-secret"

	req := httptest.NewRequest("POST", "/seed", bytes.NewReader(seedBody(t, []string{"http://example.com/"})))
	req.Header.Set("Authorization", "Bearer a-sufficiently-long-shared-secret")
	w := httptest.NewRecorder()

	ms.SeedHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// With no token configured (loopback-only deployments) the endpoint stays open.
func TestSeedHandlerOpenWhenNoTokenConfigured(t *testing.T) {
	ms := testScheduler(t)

	req := httptest.NewRequest("POST", "/seed", bytes.NewReader(seedBody(t, []string{"http://example.com/"})))
	w := httptest.NewRecorder()

	ms.SeedHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDiscoverHandlerRejectsMissingToken(t *testing.T) {
	ms := testScheduler(t)
	ms.APIToken = "a-sufficiently-long-shared-secret"

	req := httptest.NewRequest("POST", "/discover", bytes.NewReader(seedBody(t, []string{"http://example.com/"})))
	w := httptest.NewRecorder()

	ms.DiscoverHandler(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// H-06: an unbounded JSON body lets one request exhaust memory.
func TestSeedHandlerRejectsOversizedBody(t *testing.T) {
	ms := testScheduler(t)

	// A single absurdly long URL, well past the cap.
	huge := `["http://example.com/` + strings.Repeat("a", maxSeedBodyBytes+1024) + `"]`

	req := httptest.NewRequest("POST", "/seed", strings.NewReader(huge))
	w := httptest.NewRecorder()

	ms.SeedHandler(w, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code,
		"an oversized seed body must be refused, not buffered")
}
