package index

import (
	"context"
	"log/slog"
	"strconv"
	"time"
)

// TTLConfig configures document TTL enforcement.
type TTLConfig struct {
	SweepInterval time.Duration // how often to check for expired docs (default 10m)
	DefaultTTL    time.Duration // default TTL for documents without explicit _ttl (0 = no expiry)
}

// StartTTLSweeper runs a background goroutine that periodically removes
// expired documents from the index. Documents expire when:
// - They have a _ttl metadata field and (ingest_time + ttl) has passed
// - Or the default TTL is set and the document is older than that
//
// The sweeper uses BlugeStore to find expired documents and Delete() to remove them.
func (he *HybridEngine) StartTTLSweeper(ctx context.Context, cfg TTLConfig) {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 10 * time.Minute
	}

	go func() {
		ticker := time.NewTicker(cfg.SweepInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				swept := he.sweepExpired(ctx, cfg.DefaultTTL)
				if swept > 0 {
					slog.Info("ttl swept expired documents", "count", swept)
				}
			}
		}
	}()
}

func (he *HybridEngine) sweepExpired(ctx context.Context, defaultTTL time.Duration) int {
	now := time.Now()
	var cutoff int64
	if defaultTTL > 0 {
		cutoff = now.Add(-defaultTTL).Unix()
	}

	var expiredIDs []uint32
	collect := func(hit SearchHit) error {
		// Check explicit per-document _ttl (works regardless of defaultTTL)
		if ttlStr, ok := hit.Metadata["_ttl"]; ok {
			ttl, err := time.ParseDuration(ttlStr)
			if err == nil {
				if ingestStr, ok := hit.Metadata["_ingest_time"]; ok {
					ingestUnix, _ := strconv.ParseInt(ingestStr, 10, 64)
					if ingestUnix > 0 && time.Unix(ingestUnix, 0).Add(ttl).Before(now) {
						if id, err := strconv.ParseUint(hit.ID, 10, 32); err == nil {
							expiredIDs = append(expiredIDs, uint32(id))
						}
						return nil
					}
				}
			}
		}

		// Check default TTL via crawled_at (only if defaultTTL is configured)
		if defaultTTL > 0 {
			if crawledStr, ok := hit.Metadata["crawled_at"]; ok {
				crawledUnix, _ := strconv.ParseInt(crawledStr, 10, 64)
				if crawledUnix > 0 && crawledUnix < cutoff {
					if id, err := strconv.ParseUint(hit.ID, 10, 32); err == nil {
						expiredIDs = append(expiredIDs, uint32(id))
					}
				}
			}
		}
		return nil
	}

	if err := he.BlugeStore.SearchAllPages(ctx, 10000, collect); err != nil {
		slog.Error("ttl search error", "error", err)
		return 0
	}

	if len(expiredIDs) == 0 {
		return 0
	}

	deleted, err := he.Delete(ctx, expiredIDs, nil)
	if err != nil {
		slog.Error("ttl delete error", "error", err)
	}
	return deleted
}
