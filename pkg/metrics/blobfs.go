package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.d7z.net/blobfs"
)

type blobfsCollector struct {
	store *blobfs.Store

	txID                  *prometheus.Desc
	tenants               *prometheus.Desc
	inodes                *prometheus.Desc
	objects               *prometheus.Desc
	directories           *prometheus.Desc
	manifests             *prometheus.Desc
	chunks                *prometheus.Desc
	segments              *prometheus.Desc
	bytes                 *prometheus.Desc
	gcRuns                *prometheus.Desc
	gcLastEpoch           *prometheus.Desc
	gcLastRunState        *prometheus.Desc
	gcLastBackgroundAt    *prometheus.Desc
	gcLastBackgroundEpoch *prometheus.Desc
	gcLastBackgroundError *prometheus.Desc
}

func NewBlobFSCollector(store *blobfs.Store) prometheus.Collector {
	return &blobfsCollector{
		store: store,
		txID: prometheus.NewDesc(
			"cache_proxy_blobfs_txid",
			"Current BlobFS metadata transaction id.",
			nil, nil,
		),
		tenants: prometheus.NewDesc(
			"cache_proxy_blobfs_tenants",
			"Current BlobFS tenant count.",
			nil, nil,
		),
		inodes: prometheus.NewDesc(
			"cache_proxy_blobfs_inodes",
			"Current active BlobFS inode count.",
			nil, nil,
		),
		objects: prometheus.NewDesc(
			"cache_proxy_blobfs_objects",
			"Current active BlobFS object count.",
			nil, nil,
		),
		directories: prometheus.NewDesc(
			"cache_proxy_blobfs_directories",
			"Current active BlobFS directory count.",
			nil, nil,
		),
		manifests: prometheus.NewDesc(
			"cache_proxy_blobfs_manifests",
			"BlobFS manifest count by state.",
			[]string{"state"}, nil,
		),
		chunks: prometheus.NewDesc(
			"cache_proxy_blobfs_chunks",
			"BlobFS chunk count by state.",
			[]string{"state"}, nil,
		),
		segments: prometheus.NewDesc(
			"cache_proxy_blobfs_segments",
			"BlobFS segment count by state.",
			[]string{"state"}, nil,
		),
		bytes: prometheus.NewDesc(
			"cache_proxy_blobfs_bytes",
			"BlobFS byte counters by kind.",
			[]string{"kind"}, nil,
		),
		gcRuns: prometheus.NewDesc(
			"cache_proxy_blobfs_gc_runs_total",
			"Total recorded BlobFS GC runs.",
			nil, nil,
		),
		gcLastEpoch: prometheus.NewDesc(
			"cache_proxy_blobfs_gc_last_epoch",
			"Latest recorded BlobFS GC epoch.",
			nil, nil,
		),
		gcLastRunState: prometheus.NewDesc(
			"cache_proxy_blobfs_gc_last_run_state",
			"Latest BlobFS GC run state as a labeled gauge.",
			[]string{"state"}, nil,
		),
		gcLastBackgroundAt: prometheus.NewDesc(
			"cache_proxy_blobfs_gc_last_background_timestamp_seconds",
			"Unix timestamp of the latest BlobFS background GC attempt.",
			nil, nil,
		),
		gcLastBackgroundEpoch: prometheus.NewDesc(
			"cache_proxy_blobfs_gc_last_background_epoch",
			"Latest BlobFS background GC epoch.",
			nil, nil,
		),
		gcLastBackgroundError: prometheus.NewDesc(
			"cache_proxy_blobfs_gc_last_background_error",
			"Whether the latest BlobFS background GC attempt recorded an error.",
			nil, nil,
		),
	}
}

func (c *blobfsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.txID
	ch <- c.tenants
	ch <- c.inodes
	ch <- c.objects
	ch <- c.directories
	ch <- c.manifests
	ch <- c.chunks
	ch <- c.segments
	ch <- c.bytes
	ch <- c.gcRuns
	ch <- c.gcLastEpoch
	ch <- c.gcLastRunState
	ch <- c.gcLastBackgroundAt
	ch <- c.gcLastBackgroundEpoch
	ch <- c.gcLastBackgroundError
}

func (c *blobfsCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stats, err := c.store.Stats(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.objects, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.txID, prometheus.GaugeValue, float64(stats.TxID))
	ch <- prometheus.MustNewConstMetric(c.tenants, prometheus.GaugeValue, float64(stats.Tenants))
	ch <- prometheus.MustNewConstMetric(c.inodes, prometheus.GaugeValue, float64(stats.Inodes))
	ch <- prometheus.MustNewConstMetric(c.objects, prometheus.GaugeValue, float64(stats.Objects))
	ch <- prometheus.MustNewConstMetric(c.directories, prometheus.GaugeValue, float64(stats.Directories))
	ch <- prometheus.MustNewConstMetric(c.manifests, prometheus.GaugeValue, float64(stats.Manifests.Active), "active")
	ch <- prometheus.MustNewConstMetric(c.manifests, prometheus.GaugeValue, float64(stats.Manifests.Deleted), "deleted")
	ch <- prometheus.MustNewConstMetric(c.chunks, prometheus.GaugeValue, float64(stats.Chunks.Active), "active")
	ch <- prometheus.MustNewConstMetric(c.chunks, prometheus.GaugeValue, float64(stats.Chunks.GarbageCandidate), "garbage_candidate")
	ch <- prometheus.MustNewConstMetric(c.chunks, prometheus.GaugeValue, float64(stats.Chunks.Deleted), "deleted")
	ch <- prometheus.MustNewConstMetric(c.chunks, prometheus.GaugeValue, float64(stats.Chunks.Missing), "missing")
	ch <- prometheus.MustNewConstMetric(c.segments, prometheus.GaugeValue, float64(stats.Segments.Sealed), "sealed")
	ch <- prometheus.MustNewConstMetric(c.segments, prometheus.GaugeValue, float64(stats.Segments.Compacting), "compacting")
	ch <- prometheus.MustNewConstMetric(c.segments, prometheus.GaugeValue, float64(stats.Segments.Deleted), "deleted")
	ch <- prometheus.MustNewConstMetric(c.segments, prometheus.GaugeValue, float64(stats.Segments.Missing), "missing")
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes.LogicalObjectBytes), "logical_object")
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes.RawChunkBytes), "raw_chunk")
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes.StoredChunkBytes), "stored_chunk")
	ch <- prometheus.MustNewConstMetric(c.gcRuns, prometheus.CounterValue, float64(stats.GC.Runs))
	ch <- prometheus.MustNewConstMetric(c.gcLastEpoch, prometheus.GaugeValue, float64(stats.GC.LastEpoch))
	ch <- prometheus.MustNewConstMetric(c.gcLastRunState, prometheus.GaugeValue, 1, labelOrNone(stats.GC.LastRunState))
	ch <- prometheus.MustNewConstMetric(c.gcLastBackgroundAt, prometheus.GaugeValue, unixSeconds(stats.GC.LastBackgroundAt))
	ch <- prometheus.MustNewConstMetric(c.gcLastBackgroundEpoch, prometheus.GaugeValue, float64(stats.GC.LastBackgroundEpoch))
	ch <- prometheus.MustNewConstMetric(c.gcLastBackgroundError, prometheus.GaugeValue, boolGauge(stats.GC.LastBackgroundError != ""))
}

func unixSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func labelOrNone(value string) string {
	if value == "" {
		return "none"
	}
	return value
}
