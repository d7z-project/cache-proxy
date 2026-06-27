package utils

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRateLimitReaderNormalRead(t *testing.T) {
	data := "hello world this is fast enough data to not trigger rate limit"
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 1, 100*time.Millisecond)
	defer r.Close()

	time.Sleep(150 * time.Millisecond)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, string(out))
}

func TestRateLimitReaderFastEnough(t *testing.T) {
	data := strings.Repeat("x", 4096)
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 1024, 100*time.Millisecond)
	defer r.Close()

	time.Sleep(150 * time.Millisecond)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, string(out))
}

func TestRateLimitReaderTooSlow(t *testing.T) {
	data := strings.Repeat("x", 10)
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 4096, 50*time.Millisecond)
	defer r.Close()

	time.Sleep(100 * time.Millisecond)
	_, err := io.ReadAll(r)
	require.ErrorIs(t, err, ErrReadRateTooSlow)
}

func TestRateLimitReaderGracePeriod(t *testing.T) {
	data := strings.Repeat("x", 10)
	inner := io.NopCloser(strings.NewReader(data))
	r := NewRateLimitReaderWithConfig(inner, 4096, time.Hour)
	defer r.Close()

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

func TestDefaultHttpClientWrapper(t *testing.T) {
	wrapper := DefaultHttpClientWrapper()
	require.NotNil(t, wrapper.Client)
	require.NotNil(t, wrapper.Client.Transport)
	require.Equal(t, "cache-proxy", wrapper.UserAgent)

	transport, ok := wrapper.Client.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.DialContext)
	require.Equal(t, 30*time.Second, transport.ResponseHeaderTimeout)
}
