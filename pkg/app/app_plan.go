package app

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const DefaultGCInterval = 24 * time.Hour
const DefaultMaxActiveDownloads = 256
const DefaultMaxActiveDownloadsPerHost = 16
const DefaultUpstreamStartInterval = 5 * time.Millisecond
const DefaultMaxCacheObjectSize = 2 << 30
const DefaultMaxActiveSpoolSize = 8 << 30
const DefaultStatusDiskSampleInterval = 15 * time.Minute
const DefaultStatusDiskHistoryWindow = 24 * time.Hour
const DefaultStatusEventLimit = 500

var driverSet = builtinDrivers

func normalizeDownloadHost(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "://") {
		return "", errors.New("must be a host without scheme or path")
	}
	parsed, err := url.Parse("//" + value)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("must be a valid host without scheme or path")
	}
	return strings.ToLower(parsed.Host), nil
}

func upstreamGateConfig(download config.DownloadConfig) proxyruntime.UpstreamGateConfig {
	hosts := make(map[string]proxyruntime.UpstreamHostGateConfig, len(download.Hosts))
	for configuredHost, override := range download.Hosts {
		host, _ := normalizeDownloadHost(configuredHost)
		hosts[host] = proxyruntime.UpstreamHostGateConfig{
			MaxActive:   override.MaxActive,
			MinInterval: override.MinInterval.Duration(),
		}
	}
	return proxyruntime.UpstreamGateConfig{
		MaxActive:        download.MaxActive,
		MaxActivePerHost: download.MaxActivePerHost,
		MinInterval:      download.MinInterval.Duration(),
		Hosts:            hosts,
	}
}

func planEntries(
	ctx context.Context,
	doc *config.Document,
	stats *metrics.Stats,
	upstreamGate *proxyruntime.UpstreamGate,
	sched *scheduler.Scheduler,
) (*proxyruntime.Result, error) {
	plan := proxyruntime.NewPlanContext(
		doc.Server.Backend,
		stats,
		upstreamGate,
		proxyruntime.NewSpoolBudget(doc.Storage.Download.MaxActiveSpoolSize.Bytes()),
		doc.Storage.Download.MaxCacheObjectSize.Bytes(),
		doc.Storage.Cleanup,
		doc.Server.Bind,
		doc.Metrics.Path,
		sched,
	)
	plan.ReservePathPrefix(statusAPIPath, "status API")
	drivers := driverSet()
	for _, configured := range doc.Instances {
		decl := configured
		selected, err := decl.SelectMode()
		if err != nil {
			plan.CloseStores()
			return nil, err
		}
		driver, ok := drivers[selected.Mode]
		if !ok {
			plan.CloseStores()
			return nil, fmt.Errorf("instance %s: unsupported mode %q", selected.Name, selected.Mode)
		}
		instancePlan, err := plan.Instance(decl, selected)
		if err != nil {
			plan.CloseStores()
			return nil, err
		}
		if err := driver.Plan(ctx, instancePlan); err != nil {
			plan.CloseStores()
			return nil, err
		}
	}
	result, err := plan.Finalize()
	if err != nil {
		plan.CloseStores()
		return nil, err
	}
	return result, nil
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
	if doc.Server.Status.DiskSampleInterval == 0 {
		doc.Server.Status.DiskSampleInterval = config.Duration(DefaultStatusDiskSampleInterval)
	}
	if doc.Server.Status.DiskHistoryWindow == 0 {
		doc.Server.Status.DiskHistoryWindow = config.Duration(DefaultStatusDiskHistoryWindow)
	}
	if doc.Server.Status.EventLimit == 0 {
		doc.Server.Status.EventLimit = DefaultStatusEventLimit
	}
	if doc.Storage.GC.Blob == 0 {
		doc.Storage.GC.Blob = config.Duration(DefaultGCInterval)
	}
	defaults := config.DefaultCleanupConfig()
	if doc.Storage.Cleanup.BatchSize == 0 {
		doc.Storage.Cleanup.BatchSize = defaults.BatchSize
	}
	if doc.Storage.Download.MaxActive == 0 {
		doc.Storage.Download.MaxActive = DefaultMaxActiveDownloads
	}
	if doc.Storage.Download.MaxActivePerHost == 0 {
		doc.Storage.Download.MaxActivePerHost = DefaultMaxActiveDownloadsPerHost
	}
	if doc.Storage.Download.MinInterval == 0 {
		doc.Storage.Download.MinInterval = config.Duration(DefaultUpstreamStartInterval)
	}
	if doc.Storage.Download.MaxCacheObjectSize == 0 {
		doc.Storage.Download.MaxCacheObjectSize = config.ByteSize(DefaultMaxCacheObjectSize)
	}
	if doc.Storage.Download.MaxActiveSpoolSize == 0 {
		doc.Storage.Download.MaxActiveSpoolSize = config.ByteSize(DefaultMaxActiveSpoolSize)
	}
}

func validateServerConfig(doc *config.Document) error {
	if err := proxyruntime.ValidateBindAddress(doc.Server.Bind); err != nil {
		return fmt.Errorf("server bind: %w", err)
	}
	if err := validateMetricsPath(doc.Metrics.Path); err != nil {
		return err
	}
	if publicURL := strings.TrimSpace(doc.Server.PublicURL); publicURL != "" {
		if err := config.ValidateHTTPURL(publicURL); err != nil {
			return fmt.Errorf("server public_url: %w", err)
		}
		parsed, _ := url.Parse(publicURL)
		if parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("server public_url must not contain a query or fragment")
		}
	}
	if doc.Storage.Download.MaxActive <= 0 {
		return errors.New("download max_active must be positive")
	}
	if doc.Storage.Download.MaxActivePerHost <= 0 {
		return errors.New("download max_active_per_host must be positive")
	}
	if doc.Storage.Download.MinInterval < 0 {
		return errors.New("download min_interval must not be negative")
	}
	if doc.Storage.Download.MaxCacheObjectSize <= 0 {
		return errors.New("download max_cache_object_size must be positive")
	}
	if doc.Storage.Download.MaxActiveSpoolSize < doc.Storage.Download.MaxCacheObjectSize {
		return errors.New("download max_active_spool_size must be greater than or equal to max_cache_object_size")
	}
	if doc.Storage.GC.Blob <= 0 {
		return errors.New("storage gc blob interval must be positive")
	}
	if doc.Storage.Cleanup.BatchSize <= 0 {
		return errors.New("storage cleanup batch_size must be positive")
	}
	switch doc.Storage.OrphanPolicy {
	case "", "report", "auto":
	default:
		return errors.New("storage orphan_policy must be report or auto")
	}
	seenHosts := map[string]string{}
	for configuredHost, override := range doc.Storage.Download.Hosts {
		host, err := normalizeDownloadHost(configuredHost)
		if err != nil {
			return fmt.Errorf("download host %q: %w", configuredHost, err)
		}
		if previous, exists := seenHosts[host]; exists {
			return fmt.Errorf("download hosts %q and %q normalize to the same host", previous, configuredHost)
		}
		seenHosts[host] = configuredHost
		if override.MaxActive < 0 {
			return fmt.Errorf("download host %q max_active must not be negative", configuredHost)
		}
		if override.MinInterval < 0 {
			return fmt.Errorf("download host %q min_interval must not be negative", configuredHost)
		}
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
