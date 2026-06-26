package httpcache

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
)

type StreamConfig struct {
	Body       io.ReadCloser
	ObjectPath string
	Downloads  *sync.Map
	Wait       *sync.WaitGroup
	StatsStart func()
	StatsDone  func()
	StoreFn    func(ctx context.Context, r io.Reader) error
}

func StreamToPipe(ctx context.Context, cfg StreamConfig) (io.ReadCloser, error) {
	tempFile, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		cfg.Body.Close()
		return nil, err
	}

	done := make(chan struct{})
	cfg.Downloads.Store(cfg.ObjectPath, done)

	pr, pw := io.Pipe()
	cfg.Wait.Add(1)
	if cfg.StatsStart != nil {
		cfg.StatsStart()
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("download goroutine panic", "path", cfg.ObjectPath, "panic", r)
			}
		}()
		defer cfg.Wait.Done()
		defer close(done)
		defer cfg.Downloads.Delete(cfg.ObjectPath)
		defer cfg.Body.Close()
		defer pw.Close()
		if cfg.StatsDone != nil {
			defer cfg.StatsDone()
		}

		tee := io.TeeReader(cfg.Body, tempFile)
		if _, copyErr := io.Copy(pw, tee); copyErr == nil {
			if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr == nil {
				if err := cfg.StoreFn(context.Background(), tempFile); err != nil {
					slog.Warn("cache store write failed", "path", cfg.ObjectPath, "err", err)
				}
			}
		}
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	return pr, nil
}
