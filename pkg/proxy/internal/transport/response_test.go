package transport

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseCachePolicy(t *testing.T) {
	response := &http.Response{Header: make(http.Header)}
	require.True(t, ResponseCacheable(response, false))

	response.Header.Set("Cache-Control", "private")
	require.False(t, ResponseCacheable(response, false))
	require.True(t, ResponseCacheable(response, true))

	for name, value := range map[string]string{
		"Cache-Control": "no-store",
		"Set-Cookie":    "session=value",
		"Vary":          "*",
	} {
		response.Header = http.Header{name: {value}}
		require.False(t, ResponseCacheable(response, false), name)
	}

}

func TestEscapePathSegments(t *testing.T) {
	require.Equal(t, "scope/a%20b/%23file", EscapePathSegments("scope/a b/#file"))
}

func TestWriteResponseCopiesHeadersBodyAndCloses(t *testing.T) {
	body := &trackedBody{Reader: strings.NewReader("response")}
	response := &http.Response{
		StatusCode: http.StatusAccepted,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       body,
	}
	recorder := httptest.NewRecorder()
	status := WriteResponse(recorder, httptest.NewRequest(http.MethodGet, "https://example.test", nil), response, "BYPASS")
	require.Equal(t, http.StatusAccepted, status)
	require.Equal(t, "response", recorder.Body.String())
	require.Equal(t, "text/plain", recorder.Header().Get("Content-Type"))
	require.Equal(t, "BYPASS", recorder.Header().Get("X-Cache"))
	require.True(t, body.closed)

	headBody := &trackedBody{Reader: strings.NewReader("ignored")}
	headResponse := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: headBody}
	headRecorder := httptest.NewRecorder()
	WriteResponse(headRecorder, httptest.NewRequest(http.MethodHead, "https://example.test", nil), headResponse, "HIT")
	require.Empty(t, headRecorder.Body.String())
	require.True(t, headBody.closed)
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}
