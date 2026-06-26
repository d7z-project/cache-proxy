package utils

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

type testStatsRecorder struct {
	mu       sync.Mutex
	requests []testStatsEntry
}

type testStatsEntry struct {
	instance string
	mode     string
	method   string
	status   int
}

func (r *testStatsRecorder) RecordUpstream(instance, mode, method string, status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, testStatsEntry{instance, mode, method, status})
}

func TestStatsTransportRecordsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rec := &testStatsRecorder{}
	st := NewStatsTransport(http.DefaultTransport)
	st.Bind(rec, "test-instance", "test-mode")

	client := &http.Client{Transport: st}
	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	resp.Body.Close()

	require.Len(t, rec.requests, 1)
	require.Equal(t, "test-instance", rec.requests[0].instance)
	require.Equal(t, "test-mode", rec.requests[0].mode)
	require.Equal(t, http.StatusOK, rec.requests[0].status)
}

func TestStatsTransportRecordsError(t *testing.T) {
	client := &http.Client{Transport: NewStatsTransport(http.DefaultTransport)}
	_, err := client.Get("http://127.0.0.1:0/nope")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connect")
}

func TestStatsTransportWithoutBindDoesNotPanic(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := NewStatsTransport(http.DefaultTransport)
	client := &http.Client{Transport: st}
	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	resp.Body.Close()
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
