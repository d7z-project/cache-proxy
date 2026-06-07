package utils

import (
	"log/slog"
	"os"
	"strings"
)

func init() {
	setupLogger()
}

func setupLogger() {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	if os.Getenv("DEBUG") == "true" && os.Getenv("LOG_LEVEL") == "" {
		level = slog.LevelDebug
	}

	options := &slog.HandlerOptions{Level: level}
	if os.Getenv("DEBUG") == "true" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, options)))
		slog.Debug("当前为调试模式,请注意敏感信息泄漏")
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, options)))
}
