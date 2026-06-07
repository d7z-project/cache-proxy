package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/server"
)

func main() {
	defaults := server.DefaultOptions()
	backend := flag.String("backend", envString("CACHE_PROXY_BACKEND", defaults.Backend), "blobfs backend directory")
	admin := flag.String("admin", envString("CACHE_PROXY_ADMIN", defaults.AdminBind), "admin web/API bind address")
	proxyBind := flag.String("proxy-bind", envString("CACHE_PROXY_PROXY_BIND", defaults.ProxyBind), "path proxy bind address")
	metricsBind := flag.String("metrics-bind", envString("CACHE_PROXY_METRICS_BIND", defaults.MetricsBind), "metrics bind address, empty disables metrics")
	metricsPath := flag.String("metrics-path", envString("CACHE_PROXY_METRICS_PATH", defaults.MetricsPath), "metrics HTTP path")
	gcInterval := flag.Duration("gc-interval", envDuration("CACHE_PROXY_GC_INTERVAL", defaults.GCInterval), "blobfs GC interval")
	flag.Parse()

	ctx := context.Background()
	runtime, err := server.OpenWithOptions(ctx, server.Options{
		Backend:     *backend,
		AdminBind:   *admin,
		ProxyBind:   *proxyBind,
		MetricsBind: *metricsBind,
		MetricsPath: *metricsPath,
		GCInterval:  *gcInterval,
	})
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := runtime.Start(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	slog.Info("cache proxy started", "admin", *admin, "proxy", *proxyBind, "backend", *backend, "metrics", *metricsBind, "gc_interval", gcInterval.String())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runtime.Close(shutdownCtx); err != nil {
		slog.Error("shutdown failed", "err", err)
		os.Exit(1)
	}
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid %s duration %q: %v\n", name, value, err)
		os.Exit(2)
	}
	return parsed
}
