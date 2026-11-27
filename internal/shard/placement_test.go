package shard

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForKey(t *testing.T) {
	tests := []struct {
		name   string
		shards int
	}{
		{"single shard", 1},
		{"three shards", 3},
		{"eight shards", 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counts := make([]int, tt.shards)
			for i := 0; i < 3000; i++ {
				k := fmt.Sprintf("https://example.com/doc/%d", i)
				s := ForKey(k, tt.shards)
				assert.Equal(t, s, ForKey(k, tt.shards), "placement is deterministic")
				counts[s]++
			}
			for _, c := range counts {
				assert.Greater(t, c, 3000/tt.shards/2, "roughly balanced: %v", counts)
			}
		})
	}
}
