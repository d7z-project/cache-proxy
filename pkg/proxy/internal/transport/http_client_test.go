package transport

import (
	"io"
	"net/http"
	"net/http/httptest"
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

func TestUpstreamHTTPClientSelectsUserAgent(t *testing.T) {
	browser := httptest.NewRequest(http.MethodGet, "/", nil)
	browser.Header.Set("User-Agent", "Mozilla/5.0 Firefox/127.0")
	nonBrowser := httptest.NewRequest(http.MethodGet, "/", nil)
	nonBrowser.Header.Set("User-Agent", "curl/8.8.0")

	client := NewUpstreamHTTPClient()
	userAgent, forwarded := client.RequestUserAgent(browser)
	require.Equal(t, browser.UserAgent(), userAgent)
	require.True(t, forwarded)

	userAgent, forwarded = client.RequestUserAgent(nonBrowser)
	require.Equal(t, DefaultUserAgent, userAgent)
	require.False(t, forwarded)

	client.UserAgent = "forced/1"
	client.UserAgentConfigured = true
	userAgent, forwarded = client.RequestUserAgent(browser)
	require.Equal(t, "forced/1", userAgent)
	require.False(t, forwarded)
}

func TestIsBrowserRequest(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		fetchMode string
		want      bool
	}{
		{name: "chrome", userAgent: "Mozilla/5.0 Chrome/126.0.0.0 Safari/537.36", want: true},
		{name: "firefox", userAgent: "Mozilla/5.0 Firefox/127.0", want: true},
		{name: "fetch metadata", userAgent: "custom-browser", fetchMode: "navigate", want: true},
		{name: "curl", userAgent: "curl/8.8.0"},
		{name: "package manager", userAgent: "Debian APT-HTTP/1.3"},
		{name: "mozilla compatible client", userAgent: "Mozilla/5.0 compatible-client"},
		{name: "empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("User-Agent", test.userAgent)
			if test.fetchMode != "" {
				req.Header.Set("Sec-Fetch-Mode", test.fetchMode)
			}
			require.Equal(t, test.want, IsBrowserRequest(req))
		})
	}
}

func TestVariesByUserAgent(t *testing.T) {
	require.True(t, VariesByUserAgent("Accept-Encoding, user-agent"))
	require.True(t, VariesByUserAgent("Accept-Encoding", "*"))
	require.False(t, VariesByUserAgent("Accept-Encoding", "Origin"))
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
