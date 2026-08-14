package crawler

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// DistributedBloomFilter wraps a Redis Bloom filter (RedisBloom module) for
// global O(1) URL deduplication across the crawler fleet.
// In production, initialize capacity out-of-band:
//
//	BF.RESERVE crawler:frontier 0.001 10000000000
type DistributedBloomFilter struct {
	client *redis.Client
	key    string
}

// NewBloomFilter creates a new distributed Bloom filter backed by Redis.
func NewBloomFilter(client *redis.Client, key string) *DistributedBloomFilter {
	return &DistributedBloomFilter{client: client, key: key}
}

// CheckAndAdd evaluates if a URL has been seen across the global cluster.
// Returns true if newly added (URL is NEW), false if it already existed (DUPLICATE).
func (b *DistributedBloomFilter) CheckAndAdd(ctx context.Context, url string) (bool, error) {
	cmd := redis.NewCmd(ctx, "BF.ADD", b.key, url)
	err := b.client.Process(ctx, cmd)
	if err != nil {
		return false, fmt.Errorf("bloom filter process: %w", err)
	}

	// BF.ADD returns int(1)/int(0) in some Redis versions, bool in others
	val := cmd.Val()
	switch v := val.(type) {
	case int64:
		return v == 1, nil
	case bool:
		return v, nil
	default:
		return false, fmt.Errorf("bloom filter: unexpected result type %T", val)
	}
}
