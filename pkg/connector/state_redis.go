package connector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	redisStatePrefix = "yase:connector:state:"
	redisJobPrefix   = "yase:connector:job:"
	redisJobSetKey   = "yase:connector:jobs"
)

// RedisStateStore implements StateStore backed by Redis.
// Suitable for production and multi-node connector manager deployments.
type RedisStateStore struct {
	client *redis.Client
}

// NewRedisStateStore creates a Redis-backed state store.
func NewRedisStateStore(client *redis.Client) *RedisStateStore {
	return &RedisStateStore{client: client}
}

func (r *RedisStateStore) SaveState(ctx context.Context, jobID string, state *SyncState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, redisStatePrefix+jobID, data, 0).Err()
}

func (r *RedisStateStore) LoadState(ctx context.Context, jobID string) (*SyncState, error) {
	data, err := r.client.Get(ctx, redisStatePrefix+jobID).Bytes()
	if err == redis.Nil {
		return NewSyncState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get state: %w", err)
	}

	var state SyncState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *RedisStateStore) SaveJob(ctx context.Context, job *SyncJob) error {
	data, err := json.Marshal(redactJob(job))
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.Set(ctx, redisJobPrefix+job.ID, data, 0)
	pipe.SAdd(ctx, redisJobSetKey, job.ID)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStateStore) LoadJob(ctx context.Context, jobID string) (*SyncJob, error) {
	data, err := r.client.Get(ctx, redisJobPrefix+jobID).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("job %q not found", jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("redis get job: %w", err)
	}

	var job SyncJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *RedisStateStore) ListJobs(ctx context.Context) ([]*SyncJob, error) {
	ids, err := r.client.SMembers(ctx, redisJobSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list jobs: %w", err)
	}

	var jobs []*SyncJob
	for _, id := range ids {
		job, err := r.LoadJob(ctx, id)
		if err != nil {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (r *RedisStateStore) DeleteJob(ctx context.Context, jobID string) error {
	pipe := r.client.Pipeline()
	pipe.Del(ctx, redisJobPrefix+jobID)
	pipe.Del(ctx, redisStatePrefix+jobID)
	pipe.SRem(ctx, redisJobSetKey, jobID)
	_, err := pipe.Exec(ctx)
	return err
}
