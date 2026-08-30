package utils

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRateLimitReaderFastEnough(t *testing.T) {
	data := strings.Repeat("x", 4096)
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 1024, 100*time.Millisecond)
	defer func() { require.NoError(t, r.Close()) }()

	time.Sleep(150 * time.Millisecond)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, string(out))
}

func TestRateLimitReaderTooSlow(t *testing.T) {
	data := strings.Repeat("x", 10)
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 4096, 50*time.Millisecond)
	defer func() { require.NoError(t, r.Close()) }()

	time.Sleep(100 * time.Millisecond)
	_, err := io.ReadAll(r)
	require.ErrorIs(t, err, ErrReadRateTooSlow)
}

func TestRateLimitReaderGracePeriod(t *testing.T) {
	data := strings.Repeat("x", 10)
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 4096, time.Hour)
	defer func() { require.NoError(t, r.Close()) }()

	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, string(out))
}

func TestRateLimitReaderNilSafe(t *testing.T) {
	var r *RateLimitReader
	n, err := r.Read(nil)
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}

func TestDefaultHTTPClientWrapper(t *testing.T) {
	wrapper := DefaultHTTPClientWrapper()
	require.NotNil(t, wrapper.Client)
	require.NotNil(t, wrapper.Transport)
	require.Equal(t, DefaultHTTPTimeout, wrapper.Timeout)
	require.Equal(t, DefaultUserAgent, wrapper.UserAgent)
	require.Equal(t, DefaultIdleBodyTimeout, wrapper.IdleBodyTimeout)

	transport, ok := wrapper.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.DialContext)
	require.Equal(t, DefaultHeaderTimeout, transport.ResponseHeaderTimeout)
}
