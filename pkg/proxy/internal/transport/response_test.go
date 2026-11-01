package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
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

func TestServeStoredResponse(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	header := make(http.Header)
	header.Set("ETag", `"representation"`)
	header.Set("X-Source-ETag", `"upstream"`)
	header.Set("X-Source-Last-Modified", "Mon, 02 Nov 2026 00:00:00 GMT")
	header.Set("Cache-Control", "public, max-age=60")
	require.NoError(t, storeio.PutResponse(ctx, store, "test", "response", "https://example.test", http.StatusOK, header, "", strings.NewReader("payload")))
	for _, test := range []struct {
		name, method, header, value string
		status                      int
		body                        string
	}{
		{name: "full", method: http.MethodGet, status: http.StatusOK, body: "payload"},
		{name: "head", method: http.MethodHead, status: http.StatusOK},
		{name: "not modified", method: http.MethodGet, header: "If-None-Match", value: `"representation"`, status: http.StatusNotModified},
		{name: "precondition", method: http.MethodGet, header: "If-Match", value: `"other"`, status: http.StatusPreconditionFailed},
		{name: "range", method: http.MethodGet, header: "Range", value: "bytes=1-3", status: http.StatusPartialContent, body: "ayl"},
		{name: "invalid range", method: http.MethodGet, header: "Range", value: "bytes=100-200", status: http.StatusRequestedRangeNotSatisfiable, body: "invalid range: failed to overlap\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			object, err := storeio.OpenResponse(ctx, store, "test", "response")
			require.NoError(t, err)
			request := httptest.NewRequest(test.method, "/package.txt", nil)
			if test.header != "" {
				request.Header.Set(test.header, test.value)
			}
			recorder := httptest.NewRecorder()
			status := ServeStoredResponse(recorder, request, object, "package.txt", "HIT")
			require.Equal(t, test.status, status)
			require.Equal(t, test.status, recorder.Code)
			require.Equal(t, test.body, recorder.Body.String())
			require.Equal(t, "HIT", recorder.Header().Get("X-Cache"))
			require.Empty(t, recorder.Header().Get("X-Source-ETag"))
			require.Empty(t, recorder.Header().Get("X-Source-Last-Modified"))
			if status == http.StatusOK || status == http.StatusPartialContent {
				require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
				require.Equal(t, `"representation"`, recorder.Header().Get("ETag"))
				require.Equal(t, "public, max-age=60", recorder.Header().Get("Cache-Control"))
			}
			_, err = object.Reader.Read(make([]byte, 1))
			require.ErrorIs(t, err, blobfs.ErrReaderClosed)
		})
	}
}
