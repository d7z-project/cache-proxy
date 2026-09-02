package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForwardReadRemovesBodyAndMutationHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "a=1", request.URL.RawQuery)
		body, _ := io.ReadAll(request.Body)
		require.Empty(t, body)
		require.Empty(t, request.Header.Get("Content-Type"))
		require.Empty(t, request.Header.Get("X-HTTP-Method-Override"))
		require.Equal(t, "application/json", request.Header.Get("Accept"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer upstream.Close()
	origin, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	inbound := httptest.NewRequest(http.MethodGet, "/object?a=1", strings.NewReader("body"))
	inbound.Header.Set("Accept", "application/json")
	inbound.Header.Set("Content-Type", "application/json")
	inbound.Header.Set("X-HTTP-Method-Override", http.MethodDelete)
	recorder := httptest.NewRecorder()
	client, err := NewClient("test", "test", nil, nil, nil)
	require.NoError(t, err)
	status, err := ForwardRead(context.Background(), client, origin, recorder, inbound, "object")
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, status)
}

func TestForwardReadRejectsUnsafeMethodBeforeUpstream(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer upstream.Close()
	origin, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	client, err := NewClient("test", "test", nil, nil, nil)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	status, err := ForwardRead(context.Background(), client, origin, recorder, httptest.NewRequest(http.MethodPatch, "/object", strings.NewReader("body")), "object")
	require.NoError(t, err)
	require.Equal(t, http.StatusMethodNotAllowed, status)
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, "GET, HEAD", recorder.Header().Get("Allow"))
	require.Equal(t, "REJECTED", recorder.Header().Get("X-Cache"))
	require.Zero(t, requests.Load())
}
