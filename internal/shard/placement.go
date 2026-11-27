package shard

import "hash/fnv"

// ForKey returns the shard a document key (its URL) belongs to. Placement
// depends only on the key and the shard count — not on which node happens to
// consume which Kafka partition — so every version of a document lands on
// the same shard and replaces its predecessor there.
func ForKey(key string, numShards int) uint32 {
	if numShards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % uint32(numShards)
}
