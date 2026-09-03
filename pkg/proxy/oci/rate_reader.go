package oci

import (
	"errors"
	"io"
	"sync/atomic"
	"time"
)

const (
	minimumReadBytesPerSecond = 4 << 10
	minimumReadRateWindow     = 10 * time.Second
)

var errReadRateTooSlow = errors.New("read rate below minimum threshold")

type minimumRateReadCloser struct {
	inner          io.ReadCloser
	bytesPerSecond int64
	window         time.Duration
	startedAt      time.Time

	totalBytes  atomic.Int64
	lastChecked atomic.Int64
	tooSlow     atomic.Bool
}

func newMinimumRateReadCloser(inner io.ReadCloser) *minimumRateReadCloser {
	return &minimumRateReadCloser{
		inner:          inner,
		bytesPerSecond: minimumReadBytesPerSecond,
		window:         minimumReadRateWindow,
		startedAt:      time.Now(),
	}
}

func (r *minimumRateReadCloser) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		r.totalBytes.Add(int64(n))
	}
	if err != nil {
		if r.tooSlow.Load() {
			return n, errReadRateTooSlow
		}
		return n, err
	}
	if n == 0 {
		return 0, nil
	}

	now := time.Now().UnixNano()
	lastChecked := r.lastChecked.Load()
	if now-lastChecked >= r.window.Nanoseconds()/20 && r.lastChecked.CompareAndSwap(lastChecked, now) {
		if !r.tooSlow.Load() {
			elapsed := time.Since(r.startedAt)
			if elapsed >= r.window && float64(r.totalBytes.Load())/elapsed.Seconds() < float64(r.bytesPerSecond) {
				r.tooSlow.Store(true)
			}
		}
	}
	return n, nil
}

func (r *minimumRateReadCloser) Close() error {
	return r.inner.Close()
}
