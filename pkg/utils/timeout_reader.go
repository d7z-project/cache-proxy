package utils

import (
	"errors"
	"io"
	"sync"
	"time"
)

var ErrIdleBodyTimeout = errors.New("idle body timeout")

type IdleTimeoutReadCloser struct {
	rc      io.ReadCloser
	timeout time.Duration
	timer   *time.Timer
	mu      sync.Mutex
	fired   bool
	closed  bool
}

func NewIdleTimeoutReadCloser(rc io.ReadCloser, timeout time.Duration) io.ReadCloser {
	if timeout <= 0 {
		return rc
	}
	return &IdleTimeoutReadCloser{rc: rc, timeout: timeout}
}

func (r *IdleTimeoutReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if r.timer == nil {
		r.timer = time.AfterFunc(r.timeout, r.timeoutClose)
	} else {
		r.timer.Reset(r.timeout)
	}
	r.mu.Unlock()

	n, err := r.rc.Read(p)

	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
	}
	fired := r.fired
	r.mu.Unlock()
	if fired {
		return n, ErrIdleBodyTimeout
	}
	return n, err
}

func (r *IdleTimeoutReadCloser) Close() error {
	r.mu.Lock()
	r.closed = true
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	return r.rc.Close()
}

func (r *IdleTimeoutReadCloser) timeoutClose() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.fired = true
	r.mu.Unlock()
	_ = r.rc.Close()
}
