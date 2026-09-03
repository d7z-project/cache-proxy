package transport

import (
	"errors"
	"io"
	"sync"
	"time"
)

var ErrIdleBodyTimeout = errors.New("idle body timeout")

type idleTimeoutReadCloser struct {
	inner        io.ReadCloser
	timeout      time.Duration
	timer        *time.Timer
	mu           sync.Mutex
	closeOnce    sync.Once
	closeErr     error
	readSequence uint64
	timedOut     bool
	closed       bool
}

func newIdleTimeoutReadCloser(inner io.ReadCloser, timeout time.Duration) io.ReadCloser {
	if timeout <= 0 {
		return inner
	}
	return &idleTimeoutReadCloser{inner: inner, timeout: timeout}
}

func (r *idleTimeoutReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.timedOut {
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
	r.timer = time.AfterFunc(r.timeout, func() { r.closeTimedOutRead(sequence) })
	r.mu.Unlock()

	n, err := r.inner.Read(p)

	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.readSequence++
	timedOut := r.timedOut
	r.mu.Unlock()
	if timedOut {
		return n, ErrIdleBodyTimeout
	}
	return n, err
}

func (r *idleTimeoutReadCloser) Close() error {
	r.mu.Lock()
	r.closed = true
	r.readSequence++
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.mu.Unlock()
	return r.closeInner()
}

func (r *idleTimeoutReadCloser) closeTimedOutRead(sequence uint64) {
	r.mu.Lock()
	if r.closed || sequence != r.readSequence {
		r.mu.Unlock()
		return
	}
	r.timedOut = true
	r.closed = true
	r.timer = nil
	r.mu.Unlock()
	_ = r.closeInner()
}

func (r *idleTimeoutReadCloser) closeInner() error {
	r.closeOnce.Do(func() { r.closeErr = r.inner.Close() })
	return r.closeErr
}
