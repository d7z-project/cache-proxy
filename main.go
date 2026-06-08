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

var DefaultShutdownTimeout = "10s"

func main() {
	opts := server.DefaultOptions()
	bind := flag.String("bind", envString("CACHE_PROXY_BIND", opts.Bind), "主监听地址")
	backend := flag.String("backend", envString("CACHE_PROXY_BACKEND", opts.Backend), "blobfs 存储目录")
	pass := flag.String("pass", envString("CACHE_PROXY_PASS", opts.Password), "管理员密码，空=关闭鉴权")
	metricsToken := flag.String("metrics-token", envString("CACHE_PROXY_METRICS_TOKEN", opts.MetricsToken), "Metrics Bearer token")
	metricsPath := flag.String("metrics-path", envString("CACHE_PROXY_METRICS_PATH", opts.MetricsPath), "Metrics HTTP 路径")
	gcInterval := flag.Duration("gc-interval", envDuration("CACHE_PROXY_GC_INTERVAL", opts.GCInterval), "blobfs GC 间隔")
	flag.Parse()

	runtime, err := server.OpenWithOptions(context.Background(), server.Options{
		Backend:      *backend,
		Bind:         *bind,
		Password:     *pass,
		MetricsToken: *metricsToken,
		MetricsPath:  *metricsPath,
		GCInterval:   *gcInterval,
	})
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := runtime.Start(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout())
		if closeErr := runtime.Close(shutdownCtx); closeErr != nil {
			slog.Error("shutdown after start failure failed", "err", closeErr)
		}
		cancel()
		os.Exit(1)
	}
	slog.Info("cache proxy started", "bind", *bind, "backend", *backend, "metrics_path", *metricsPath, "gc_interval", gcInterval.String())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout())
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

func defaultShutdownTimeout() time.Duration {
	duration, err := time.ParseDuration(DefaultShutdownTimeout)
	if err != nil {
		panic(fmt.Sprintf("invalid DefaultShutdownTimeout %q: %v", DefaultShutdownTimeout, err))
	}
	return duration
}
