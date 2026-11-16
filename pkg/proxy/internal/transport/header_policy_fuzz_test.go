package transport

import (
	"net/http"
	"testing"

	"golang.org/x/net/http/httpguts"

	"github.com/stretchr/testify/require"
)

func FuzzHeaderBoundary(f *testing.F) {
	for _, seed := range []struct{ name, value string }{
		{"Forwarded", "for=192.0.2.1"},
		{"X-Request-ID", "request"},
		{"Content-Type", "application/octet-stream"},
		{"Connection", "X-Private"},
		{"Docker-Content-Digest", "sha256:digest"},
	} {
		f.Add(seed.name, seed.value)
	}
	f.Fuzz(func(t *testing.T, name, value string) {
		if len(name) > 128 || len(value) > 1024 || !httpguts.ValidHeaderFieldName(name) || !httpguts.ValidHeaderFieldValue(value) {
			t.Skip()
		}
		request, err := http.NewRequest(http.MethodGet, "https://example.test/artifact", nil)
		require.NoError(t, err)
		request.Header.Set(name, value)
		request.Header.Set("Connection", name)
		SanitizeUpstreamRequest(request, "fixed-agent")
		require.Equal(t, "fixed-agent", request.UserAgent())
		require.Equal(t, "1.1 gateway", request.Header.Get("Via"))
		require.Empty(t, request.Header.Get("Connection"))
		switch http.CanonicalHeaderKey(name) {
		case "User-Agent":
			require.Equal(t, "fixed-agent", request.Header.Get(name))
		case "Via":
			require.Equal(t, "1.1 gateway", request.Header.Get(name))
		default:
			require.Empty(t, request.Header.Get(name))
		}

		response := &http.Response{Header: http.Header{name: {value}}}
		SanitizeUpstreamResponse(response)
		again := &http.Response{Header: response.Header.Clone()}
		SanitizeUpstreamResponse(again)
		require.Equal(t, response.Header, again.Header)
	})
}
