package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/app"
	"gopkg.d7z.net/cache-proxy/pkg/config"
)

const shutdownTimeout = 10 * time.Second

func main() {
	level := slog.LevelWarn
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	debug := os.Getenv("DEBUG") == "true"
	if debug && os.Getenv("LOG_LEVEL") == "" {
		level = slog.LevelDebug
	}
	logOptions := &slog.HandlerOptions{Level: level}
	if debug {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, logOptions)))
		slog.Debug("debug logging enabled; logs may contain sensitive data")
	} else {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, logOptions)))
	}

	configPath := flag.String("config", "", "YAML configuration file")
	validateOnly := flag.Bool("validate", false, "validate configuration and exit")
	flag.Parse()

	if *configPath == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing required -config")
		os.Exit(2)
	}

	doc, err := config.LoadFile(*configPath)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *validateOnly {
		if err := app.Validate(doc); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	application, err := app.Open(context.Background(), doc)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := application.Start(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		_ = application.Close(shutdownCtx)
		cancel()
		os.Exit(1)
	}
	slog.Info("cache proxy started", "bind", doc.Server.Bind, "backend", doc.Server.Backend, "metrics_path", doc.Metrics.Path)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := application.Close(shutdownCtx); err != nil {
		slog.Error("shutdown failed", "err", err)
		os.Exit(1)
	}
}
