package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/bus"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const DefaultGCInterval = 24 * time.Hour
const DefaultMaxActiveDownloads = 64
const DefaultMaxActiveDownloadsPerInstance = 8
const DefaultStatusDiskSampleInterval = 15 * time.Minute
const DefaultStatusDiskHistoryWindow = 24 * time.Hour
const DefaultStatusEventLimit = 500

var driverSet = builtinDrivers

func planEntries(ctx context.Context, doc *config.Document, store *blobfs.Store, stats *httpcache.Stats, downloads *httpcache.DownloadLimiter, sched *scheduler.Scheduler, b *bus.Bus) (map[string]*proxyruntime.Entry, error) {
	plan := proxyruntime.NewPlanContext(store, stats, downloads, doc.Storage.Cleanup, doc.Server.Bind, doc.Metrics.Path, sched, b)
	drivers := driverSet()
	for _, decl := range doc.Instances {
		selected, err := decl.SelectMode()
		if err != nil {
			return nil, err
		}
		driver, ok := drivers[selected.Mode]
		if !ok {
			return nil, fmt.Errorf("instance %s: unsupported mode %q", selected.Name, selected.Mode)
		}
		instancePlan, err := plan.Instance(decl, selected)
		if err != nil {
			return nil, err
		}
		if err := driver.Plan(ctx, instancePlan); err != nil {
			return nil, err
		}
	}
	result, err := plan.Finalize()
	if err != nil {
		return nil, err
	}
	entries := make(map[string]*proxyruntime.Entry, len(result.Entries))
	for _, entry := range result.Entries {
		if entry.ExpireAfter.IsUnset() {
			entry.ExpireAfter = config.DefaultExpireAfter
		}
		entries[entry.Name] = entry
	}
	return entries, nil
}

func normalizeDocument(doc *config.Document) {
	if strings.TrimSpace(doc.Server.Bind) == "" {
		doc.Server.Bind = DefaultBind
	}
	if strings.TrimSpace(doc.Server.Backend) == "" {
		doc.Server.Backend = DefaultBackend
	}
	if strings.TrimSpace(doc.Metrics.Path) == "" {
		doc.Metrics.Path = DefaultMetricsPath
	}
	if doc.Server.Status.DiskSampleInterval <= 0 {
		doc.Server.Status.DiskSampleInterval = config.Duration(DefaultStatusDiskSampleInterval)
	}
	if doc.Server.Status.DiskHistoryWindow <= 0 {
		doc.Server.Status.DiskHistoryWindow = config.Duration(DefaultStatusDiskHistoryWindow)
	}
	if doc.Server.Status.EventLimit <= 0 {
		doc.Server.Status.EventLimit = DefaultStatusEventLimit
	}
	if doc.Storage.GC.Blob <= 0 {
		doc.Storage.GC.Blob = config.Duration(DefaultGCInterval)
	}
	defaults := config.DefaultCleanupConfig()
	if doc.Storage.Cleanup.BatchSize <= 0 {
		doc.Storage.Cleanup.BatchSize = defaults.BatchSize
	}
	if doc.Storage.Download.MaxActive <= 0 {
		doc.Storage.Download.MaxActive = DefaultMaxActiveDownloads
	}
	if doc.Storage.Download.MaxActivePerInstance <= 0 {
		doc.Storage.Download.MaxActivePerInstance = DefaultMaxActiveDownloadsPerInstance
	}
}

func validateServerConfig(doc *config.Document) error {
	if err := proxyruntime.ValidateBindAddress(doc.Server.Bind); err != nil {
		return fmt.Errorf("server bind: %w", err)
	}
	if err := validateMetricsPath(doc.Metrics.Path); err != nil {
		return err
	}
	if doc.Storage.Download.MaxActive <= 0 {
		return errors.New("download max_active must be positive")
	}
	if doc.Storage.Download.MaxActivePerInstance <= 0 {
		return errors.New("download max_active_per_instance must be positive")
	}
	if doc.Server.Status.DiskSampleInterval <= 0 {
		return errors.New("server status disk_sample_interval must be positive")
	}
	if doc.Server.Status.DiskHistoryWindow < doc.Server.Status.DiskSampleInterval {
		return errors.New("server status disk_history_window must be greater than or equal to disk_sample_interval")
	}
	if doc.Server.Status.EventLimit <= 0 {
		return errors.New("server status event_limit must be positive")
	}
	return nil
}

func validateMetricsPath(path string) error {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "//") || strings.HasSuffix(path, "/") || strings.ContainsAny(path, " \t\r\n{}") {
		return fmt.Errorf("invalid metrics path %q", path)
	}
	if path == "/" {
		return fmt.Errorf("metrics path %q conflicts with proxy root", path)
	}
	return nil
}
