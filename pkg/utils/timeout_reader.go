package utils

import (
	"errors"
	"io"
	"sync"
	"time"
)

var ErrIdleBodyTimeout = errors.New("idle body timeout")

type IdleTimeoutReadCloser struct {
	rc           io.ReadCloser
	timeout      time.Duration
	timer        *time.Timer
	mu           sync.Mutex
	close        sync.Once
	closeErr     error
	readSequence uint64
	fired        bool
	closed       bool
}

func NewIdleTimeoutReadCloser(rc io.ReadCloser, timeout time.Duration) io.ReadCloser {
	if timeout <= 0 {
		return rc
	}
	return &IdleTimeoutReadCloser{rc: rc, timeout: timeout}
}

func (r *IdleTimeoutReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.fired {
		r.mu.Unlock()
		return 0, ErrIdleBodyTimeout
	}
	if r.closed {
		r.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if r.timer != nil {
		r.timer.Stop()
	}
	r.readSequence++
	sequence := r.readSequence
	r.timer = time.AfterFunc(r.timeout, func() { r.timeoutClose(sequence) })
	r.mu.Unlock()

	n, err := r.rc.Read(p)

	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.readSequence++
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
	r.readSequence++
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.mu.Unlock()
	return r.closeUnderlying()
}

func (r *IdleTimeoutReadCloser) timeoutClose(sequence uint64) {
	r.mu.Lock()
	if r.closed || sequence != r.readSequence {
		r.mu.Unlock()
		return
	}
	r.fired = true
	r.closed = true
	r.timer = nil
	r.mu.Unlock()
	_ = r.closeUnderlying()
}

func (r *IdleTimeoutReadCloser) closeUnderlying() error {
	r.close.Do(func() { r.closeErr = r.rc.Close() })
	return r.closeErr
}
