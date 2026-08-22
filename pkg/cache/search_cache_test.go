package cache

import (
	"testing"
	"time"

	"github.com/efathom/yase/pkg/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCache(t *testing.T) *SearchCache {
	t.Helper()
	return NewSearchCache("col-1", 100, time.Minute)
}

// L-16: key parts were joined with "|" without escaping, so a value containing
// the separator could reproduce another key's serialization.
func TestCacheKeysDoNotCollideAcrossSeparatorInValues(t *testing.T) {
	c := newCache(t)

	resultsA := []index.ScoredResult{{ID: 1}}
	resultsB := []index.ScoredResult{{ID: 2}}

	// These two differ only in where the "|" and "=" fall.
	c.Put("q", map[string]string{"a": "x|b=y"}, 10, resultsA)
	c.Put("q", map[string]string{"a": "x", "b": "y"}, 10, resultsB)

	gotA := c.Get("q", map[string]string{"a": "x|b=y"}, 10)
	gotB := c.Get("q", map[string]string{"a": "x", "b": "y"}, 10)

	require.Len(t, gotA, 1)
	require.Len(t, gotB, 1)
	assert.Equal(t, uint32(1), gotA[0].ID, "filter values must not bleed across keys")
	assert.Equal(t, uint32(2), gotB[0].ID)
}

// A query containing the separator must not collide with a filtered query.
func TestCacheKeysDoNotCollideAcrossSeparatorInQuery(t *testing.T) {
	c := newCache(t)

	c.Put("plain", map[string]string{"k": "v"}, 10, []index.ScoredResult{{ID: 1}})
	c.Put("plain|k=v", nil, 10, []index.ScoredResult{{ID: 2}})

	got := c.Get("plain", map[string]string{"k": "v"}, 10)
	require.Len(t, got, 1)
	assert.Equal(t, uint32(1), got[0].ID)
}

// Two tenants issuing the same query must never share an entry.
func TestCacheSeparatesTenants(t *testing.T) {
	c := newCache(t)

	c.Put("q", map[string]string{"_tenant": "a"}, 10, []index.ScoredResult{{ID: 1}})

	assert.Nil(t, c.Get("q", map[string]string{"_tenant": "b"}, 10),
		"tenant-b must not read tenant-a's cached results")
}

// Ordinary hit/miss behavior must be preserved.
func TestCacheHitAndMiss(t *testing.T) {
	c := newCache(t)

	assert.Nil(t, c.Get("q", nil, 10), "empty cache must miss")

	c.Put("q", nil, 10, []index.ScoredResult{{ID: 7}})

	got := c.Get("q", nil, 10)
	require.Len(t, got, 1)
	assert.Equal(t, uint32(7), got[0].ID)

	assert.Nil(t, c.Get("q", nil, 20), "a different topK is a different key")
}

func TestCacheInvalidateClearsEntries(t *testing.T) {
	c := newCache(t)
	c.Put("q", nil, 10, []index.ScoredResult{{ID: 7}})
	require.Equal(t, 1, c.Len())

	c.Invalidate()

	assert.Equal(t, 0, c.Len())
	assert.Nil(t, c.Get("q", nil, 10))
}

func TestCacheExpiresEntries(t *testing.T) {
	c := NewSearchCache("col-1", 100, time.Nanosecond)
	c.Put("q", nil, 10, []index.ScoredResult{{ID: 7}})

	time.Sleep(time.Millisecond)

	assert.Nil(t, c.Get("q", nil, 10), "an expired entry must not be served")
}

// The cache must hand back a copy — a caller mutating results must not corrupt
// what the next reader sees.
func TestCacheReturnsCopies(t *testing.T) {
	c := newCache(t)
	c.Put("q", nil, 10, []index.ScoredResult{{ID: 7}})

	first := c.Get("q", nil, 10)
	require.Len(t, first, 1)
	first[0].ID = 999

	second := c.Get("q", nil, 10)
	require.Len(t, second, 1)
	assert.Equal(t, uint32(7), second[0].ID)
}
