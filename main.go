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

	"gopkg.d7z.net/cache-proxy/pkg/app"
)

const shutdownTimeout = 10 * time.Second

func main() {
	configPath := flag.String("config", "", "YAML configuration file")
	validateOnly := flag.Bool("validate", false, "validate configuration and exit")
	flag.Parse()

	if *configPath == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing required -config")
		os.Exit(2)
	}

	doc, err := app.Load(*configPath)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := app.Validate(doc); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *validateOnly {
		return
	}

	runtime, err := app.Open(context.Background(), doc)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := runtime.Start(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		_ = runtime.Close(shutdownCtx)
		cancel()
		os.Exit(1)
	}
	slog.Info("cache proxy started", "bind", doc.Server.Bind, "backend", doc.Server.Backend, "metrics_path", doc.Metrics.Path)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := runtime.Close(shutdownCtx); err != nil {
		slog.Error("shutdown failed", "err", err)
		os.Exit(1)
	}
}
