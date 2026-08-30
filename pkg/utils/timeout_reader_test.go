package utils

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingReadCloser struct {
	closed    chan struct{}
	closeOnce atomic.Bool
	closes    atomic.Int32
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{closed: make(chan struct{})}
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *blockingReadCloser) Close() error {
	r.closes.Add(1)
	if r.closeOnce.CompareAndSwap(false, true) {
		close(r.closed)
	}
	return nil
}

func TestIdleTimeoutReadCloserInterruptsBlockedRead(t *testing.T) {
	inner := newBlockingReadCloser()
	reader := NewIdleTimeoutReadCloser(inner, 20*time.Millisecond)

	started := time.Now()
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrIdleBodyTimeout)
	require.Less(t, time.Since(started), time.Second)
	require.NoError(t, reader.Close())
	require.Equal(t, int32(1), inner.closes.Load())
}

func TestIdleTimeoutReadCloserDoesNotReuseExpiredTimer(t *testing.T) {
	reader := NewIdleTimeoutReadCloser(io.NopCloser(&pacedReader{delay: 2 * time.Millisecond}), 20*time.Millisecond)
	defer func() { require.NoError(t, reader.Close()) }()

	buffer := make([]byte, 1)
	for range 50 {
		_, err := reader.Read(buffer)
		require.NoError(t, err)
	}
}

type pacedReader struct {
	delay time.Duration
}

func (r *pacedReader) Read(p []byte) (int, error) {
	time.Sleep(r.delay)
	p[0] = 'x'
	return 1, nil
}
