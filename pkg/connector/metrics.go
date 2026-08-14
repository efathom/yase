package connector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RecordsReadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "yase_connector_records_read_total",
		Help: "Total records read by connector syncs",
	}, []string{"connector", "stream"})

	RecordsErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "yase_connector_records_errors_total",
		Help: "Total errors during connector syncs",
	}, []string{"connector", "stream"})

	SyncDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "yase_connector_sync_duration_seconds",
		Help:    "Duration of connector sync operations",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s to ~1h
	}, []string{"connector", "mode"})

	SyncStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "yase_connector_sync_status",
		Help: "Current sync status (0=idle, 1=running, 2=failed, 3=completed)",
	}, []string{"connector"})

	LastSyncTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "yase_connector_last_sync_timestamp",
		Help: "Unix timestamp of last completed sync",
	}, []string{"connector"})
)
