package utils

import (
	"errors"
	"io"
	"sync"
	"time"
)

const (
	defaultMinBytesPerSec = 4 << 10 // 4KB/s
	defaultWindow         = 10 * time.Second
)

var ErrReadRateTooSlow = errors.New("read rate below minimum threshold")

type RateLimitReader struct {
	inner         io.ReadCloser
	minBytesPerSec int64
	window        time.Duration

	mu         sync.Mutex
	totalBytes int64
	startedAt  time.Time
	lastReadAt time.Time
	dead       bool
}

func NewRateLimitReader(inner io.ReadCloser) *RateLimitReader {
	return NewRateLimitReaderWithConfig(inner, defaultMinBytesPerSec, defaultWindow)
}

func NewRateLimitReaderWithConfig(inner io.ReadCloser, minBytesPerSec int64, window time.Duration) *RateLimitReader {
	now := time.Now()
	return &RateLimitReader{
		inner:         inner,
		minBytesPerSec: minBytesPerSec,
		window:        window,
		startedAt:     now,
		lastReadAt:    now,
	}
}

func (r *RateLimitReader) Read(p []byte) (int, error) {
	if r == nil {
		return 0, io.EOF
	}
	n, err := r.inner.Read(p)
	if n > 0 {
		r.mu.Lock()
		r.totalBytes += int64(n)
		r.lastReadAt = time.Now()
		r.mu.Unlock()
	}
	if err != nil {
		if r.dead {
			return n, ErrReadRateTooSlow
		}
		return n, err
	}
	if n == 0 {
		return 0, nil
	}
	r.checkRate()
	return n, nil
}

func (r *RateLimitReader) checkRate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead {
		return
	}
	elapsed := time.Since(r.startedAt)
	if elapsed < r.window {
		return
	}
	rate := float64(r.totalBytes) / elapsed.Seconds()
	if rate < float64(r.minBytesPerSec) {
		r.dead = true
	}
}

func (r *RateLimitReader) Close() error {
	if r == nil {
		return nil
	}
	return r.inner.Close()
}
