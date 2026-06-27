package utils

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMinBytesPerSec = 4 << 10 // 4KB/s
	defaultWindow         = 10 * time.Second
)

var ErrReadRateTooSlow = errors.New("read rate below minimum threshold")

type RateLimitReader struct {
	inner          io.ReadCloser
	minBytesPerSec int64
	window         time.Duration
	startedAt      time.Time

	totalBytes  atomic.Int64
	lastReadAt  atomic.Int64
	dead        atomic.Bool
	lastChecked atomic.Int64

	mu sync.Mutex
}

func NewRateLimitReader(inner io.ReadCloser) *RateLimitReader {
	return NewRateLimitReaderWithConfig(inner, defaultMinBytesPerSec, defaultWindow)
}

func NewRateLimitReaderWithConfig(inner io.ReadCloser, minBytesPerSec int64, window time.Duration) *RateLimitReader {
	return &RateLimitReader{
		inner:          inner,
		minBytesPerSec: minBytesPerSec,
		window:         window,
		startedAt:      time.Now(),
	}
}

func (r *RateLimitReader) Read(p []byte) (int, error) {
	if r == nil {
		return 0, io.EOF
	}
	n, err := r.inner.Read(p)
	if n > 0 {
		r.totalBytes.Add(int64(n))
		r.lastReadAt.Store(time.Now().UnixNano())
	}
	if err != nil {
		if r.dead.Load() {
			return n, ErrReadRateTooSlow
		}
		return n, err
	}
	if n == 0 {
		return 0, nil
	}
	r.maybeCheck()
	return n, nil
}

func (r *RateLimitReader) maybeCheck() {
	lastChecked := r.lastChecked.Load()
	now := time.Now().UnixNano()
	if now-lastChecked < r.window.Nanoseconds()/20 {
		return
	}
	if r.lastChecked.CompareAndSwap(lastChecked, now) {
		r.checkRate()
	}
}

func (r *RateLimitReader) checkRate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead.Load() {
		return
	}
	elapsed := time.Since(r.startedAt)
	if elapsed < r.window {
		return
	}
	rate := float64(r.totalBytes.Load()) / elapsed.Seconds()
	if rate < float64(r.minBytesPerSec) {
		r.dead.Store(true)
	}
}

func (r *RateLimitReader) Close() error {
	if r == nil {
		return nil
	}
	return r.inner.Close()
}
