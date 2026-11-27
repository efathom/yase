package cache

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/efathom/yase/pkg/index"
	"github.com/stretchr/testify/assert"
)

func TestCacheEntriesExpireWhenIndexChanges(t *testing.T) {
	var gen atomic.Uint64
	c := NewSearchCache("_default", 10, time.Hour).SetGeneration(gen.Load)
	c.Put("q", nil, 5, []index.ScoredResult{{ID: 1}})

	tests := []struct {
		name    string
		bump    bool
		wantHit bool
	}{
		{"unchanged index hits", false, true},
		{"write to index misses", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.bump {
				gen.Add(1)
			}
			assert.Equal(t, tt.wantHit, c.Get("q", nil, 5) != nil)
		})
	}
}
