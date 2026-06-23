package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	httpproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

const DefaultGCInterval = 24 * time.Hour

func planEntries(ctx context.Context, doc *config.Document, store *blobfs.Store, stats *httpproxy.Stats) (map[string]*proxyruntime.Entry, error) {
	plan := proxyruntime.NewPlanContext(store, stats, doc.Server.Bind, doc.Metrics.Path)
	drivers := builtinDrivers()
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
	if doc.Storage.GC.Blob <= 0 {
		doc.Storage.GC.Blob = config.Duration(DefaultGCInterval)
	}
	defaults := config.DefaultCleanupConfig()
	if doc.Storage.Cleanup.Interval <= 0 {
		doc.Storage.Cleanup.Interval = defaults.Interval
	}
	if doc.Storage.Cleanup.BatchSize <= 0 {
		doc.Storage.Cleanup.BatchSize = defaults.BatchSize
	}
}

func validateServerConfig(doc *config.Document) error {
	if err := proxyruntime.ValidateBindAddress(doc.Server.Bind); err != nil {
		return fmt.Errorf("server bind: %w", err)
	}
	if err := validateMetricsPath(doc.Metrics.Path); err != nil {
		return err
	}
	if doc.Storage.Cleanup.Workers < 0 {
		return errors.New("cleanup workers must not be negative")
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

func sortedEntryNames(entries map[string]*proxyruntime.Entry) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
