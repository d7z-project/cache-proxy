package transport

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewUpstreamHTTPClient(t *testing.T) {
	client := NewUpstreamHTTPClient()
	require.NotNil(t, client.Client)
	require.NotNil(t, client.Transport)
	require.Equal(t, defaultHTTPTimeout, client.Timeout)
	require.Equal(t, DefaultUserAgent, client.UserAgent)
	require.Equal(t, defaultIdleBodyTimeout, client.IdleBodyTimeout)

	httpTransport, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, httpTransport.DialContext)
	require.Equal(t, defaultHeaderTimeout, httpTransport.ResponseHeaderTimeout)
}

type blockingReadCloser struct {
	closed chan struct{}
	once   atomic.Bool
	closes atomic.Int32
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *blockingReadCloser) Close() error {
	r.closes.Add(1)
	if r.once.CompareAndSwap(false, true) {
		close(r.closed)
	}
	return nil
}

func TestIdleTimeoutReadCloserInterruptsBlockedRead(t *testing.T) {
	inner := &blockingReadCloser{closed: make(chan struct{})}
	reader := newIdleTimeoutReadCloser(inner, 20*time.Millisecond)

	started := time.Now()
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrIdleBodyTimeout)
	require.Less(t, time.Since(started), time.Second)
	require.NoError(t, reader.Close())
	require.Equal(t, int32(1), inner.closes.Load())
}

func TestIdleTimeoutReadCloserDoesNotReuseExpiredTimer(t *testing.T) {
	reader := newIdleTimeoutReadCloser(io.NopCloser(&pacedReader{delay: 2 * time.Millisecond}), 20*time.Millisecond)
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
