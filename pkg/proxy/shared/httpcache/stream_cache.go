package httpcache

import (
	"bufio"
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
	VerifyFn   func(io.ReadSeeker) error
	StoreFn    func(ctx context.Context, r io.Reader) error
}

func StreamToPipe(ctx context.Context, cfg StreamConfig) (io.ReadCloser, error) {
	tempFile, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		cfg.Body.Close()
		return nil, err
	}

	pr, pw := io.Pipe()
	cfg.Wait.Add(1)
	if cfg.StatsStart != nil {
		cfg.StatsStart()
	}
	slog.Debug("download started", "path", cfg.ObjectPath, "temp", tempFile.Name())
	storeCtx := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("download goroutine panic", "path", cfg.ObjectPath, "panic", r)
			}
		}()
		defer cfg.Wait.Done()
		defer cfg.Downloads.Delete(cfg.ObjectPath)
		defer cfg.Body.Close()
		defer pw.Close()
		defer tempFile.Close()
		defer os.Remove(tempFile.Name())
		if cfg.StatsDone != nil {
			defer cfg.StatsDone()
		}

		bw := bufio.NewWriterSize(tempFile, 256<<10)
		tee := io.TeeReader(cfg.Body, bw)
		if _, copyErr := io.Copy(pw, tee); copyErr == nil {
			if err := bw.Flush(); err != nil {
				slog.Warn("download temp flush failed", "path", cfg.ObjectPath, "err", err)
				return
			}
			if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr != nil {
				slog.Warn("download temp rewind failed", "path", cfg.ObjectPath, "err", seekErr)
				return
			}
			if cfg.VerifyFn != nil {
				if err := cfg.VerifyFn(tempFile); err != nil {
					slog.Warn("cache store verification failed", "path", cfg.ObjectPath, "err", err)
					return
				}
				if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr != nil {
					slog.Warn("download temp rewind failed", "path", cfg.ObjectPath, "err", seekErr)
					return
				}
			}
			if err := cfg.StoreFn(storeCtx, tempFile); err != nil {
				slog.Warn("cache store write failed", "path", cfg.ObjectPath, "err", err)
				return
			}
			slog.Debug("download completed", "path", cfg.ObjectPath)
		} else {
			slog.Debug("download aborted", "path", cfg.ObjectPath, "err", copyErr)
		}
	}()

	return pr, nil
}
