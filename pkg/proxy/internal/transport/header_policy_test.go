package transport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type closeTrackingRoundTripper struct {
	closed atomic.Bool
}

func (t *closeTrackingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}

func (t *closeTrackingRoundTripper) CloseIdleConnections() {
	t.closed.Store(true)
}

func TestCopyUpstreamHeadersUsesDirectionalPolicy(t *testing.T) {
	source := http.Header{
		"Accept":                 {"application/json", "application/octet-stream"},
		"Authorization":          {"Bearer package-token"},
		"Cookie":                 {"session=package"},
		"Range":                  {"bytes=0-10"},
		"Content-Type":           {"application/x-git-upload-pack-request"},
		"Git-Protocol":           {"version=2"},
		"Connection":             {"X-Connection-Private"},
		"X-Connection-Private":   {"secret"},
		"Forwarded":              {"for=192.0.2.1"},
		"X-Forwarded-For":        {"192.0.2.1"},
		"X-Client-IP":            {"192.0.2.1"},
		"X-Request-Id":           {"request-id"},
		"X-Upstream-Host":        {"backend.internal"},
		"Sec-Fetch-Site":         {"same-origin"},
		"X-HTTP-Method-Override": {http.MethodDelete},
	}

	read := make(http.Header)
	CopyUpstreamReadHeaders(read, source, "Git-Protocol")
	require.Equal(t, source.Values("Accept"), read.Values("Accept"))
	require.Equal(t, "Bearer package-token", read.Get("Authorization"))
	require.Equal(t, "session=package", read.Get("Cookie"))
	require.Equal(t, "bytes=0-10", read.Get("Range"))
	require.Equal(t, "version=2", read.Get("Git-Protocol"))
	for _, name := range []string{"Content-Type", "Connection", "X-Connection-Private", "Forwarded", "X-Forwarded-For", "X-Client-IP", "X-Request-Id", "X-Upstream-Host", "Sec-Fetch-Site", "X-HTTP-Method-Override"} {
		require.Empty(t, read.Values(name), name)
	}

	post := make(http.Header)
	CopyUpstreamPostHeaders(post, source, "Git-Protocol")
	require.Equal(t, "application/x-git-upload-pack-request", post.Get("Content-Type"))
	require.Equal(t, "version=2", post.Get("Git-Protocol"))
	require.Empty(t, post.Get("X-HTTP-Method-Override"))
}

func TestHeaderBoundaryNormalizesEveryUpstreamHop(t *testing.T) {
	type observedRequest struct {
		host   string
		header http.Header
	}
	observed := make(chan observedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		observed <- observedRequest{host: request.Host, header: request.Header.Clone()}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", "sha256:protocol")
		w.Header().Set("Set-Cookie", "upstream=protocol")
		w.Header().Set("Server", "upstream-server")
		w.Header().Set("Via", "upstream-proxy")
		w.Header().Set("X-Cache", "upstream-hit")
		w.Header().Set("X-Envoy-Upstream-Service-Time", "7")
		_, _ = io.WriteString(w, "body")
	}))
	t.Cleanup(upstream.Close)

	client, err := NewClient("test", "file", nil, nil, nil)
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, upstream.URL+"/artifact", nil)
	require.NoError(t, err)
	request.Host = "downstream.example"
	request.Header.Set("Host", "downstream.example")
	request.Header.Set("User-Agent", "downstream-client/1")
	request.Header.Set("Via", "1.0 private-proxy")
	request.Header.Set("Forwarded", "for=192.0.2.1")
	request.Header.Set("X-Real-IP", "192.0.2.1")
	request.Header.Set("Traceparent", "00-private")
	request.Header.Set("Origin", "https://private.example")
	request.Header.Set("X-Protocol-Request", "preserved")

	response, err := client.DoRead(context.Background(), request, AdmissionForeground)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	seen := <-observed
	parsed, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	require.Equal(t, parsed.Host, seen.host)
	require.Equal(t, DefaultUserAgent, seen.header.Get("User-Agent"))
	require.Equal(t, "1.1 gateway", seen.header.Get("Via"))
	require.Equal(t, "preserved", seen.header.Get("X-Protocol-Request"))
	for _, name := range []string{"Host", "Forwarded", "X-Real-IP", "Traceparent", "Origin"} {
		require.Empty(t, seen.header.Values(name), name)
	}

	require.Equal(t, "application/octet-stream", response.Header.Get("Content-Type"))
	require.Equal(t, "sha256:protocol", response.Header.Get("Docker-Content-Digest"))
	require.Equal(t, "upstream=protocol", response.Header.Get("Set-Cookie"))
	for _, name := range []string{"Server", "Via", "X-Cache", "X-Envoy-Upstream-Service-Time"} {
		require.Empty(t, response.Header.Values(name), name)
	}
}

func TestCrossOriginRedirectRetainsOnlyRepresentationFields(t *testing.T) {
	next := httptest.NewRequest(http.MethodGet, "https://cdn.example/artifact", nil)
	next.Header = http.Header{
		"Accept":        {"application/octet-stream"},
		"Authorization": {"Bearer secret"},
		"Cookie":        {"session=secret"},
		"Git-Protocol":  {"version=2"},
		"X-Api-Key":     {"custom-secret"},
		"X-Request-Id":  {"private-id"},
	}
	previous := httptest.NewRequest(http.MethodGet, "https://registry.example/artifact", nil)
	require.NoError(t, CheckReadOnlyRedirect(next, []*http.Request{previous}))
	require.Equal(t, "application/octet-stream", next.Header.Get("Accept"))
	for _, name := range []string{"Authorization", "Cookie", "Git-Protocol", "X-Api-Key", "X-Request-Id"} {
		require.Empty(t, next.Header.Values(name), name)
	}

	sameOrigin := httptest.NewRequest(http.MethodGet, "https://registry.example/other", nil)
	sameOrigin.Header.Set("X-Api-Key", "same-origin-secret")
	require.NoError(t, CheckReadOnlyRedirect(sameOrigin, []*http.Request{previous}))
	require.Equal(t, "same-origin-secret", sameOrigin.Header.Get("X-Api-Key"))
}

func TestValidatePassHeader(t *testing.T) {
	for _, name := range []string{"X-Representation", "X-Api-Key"} {
		require.NoError(t, ValidatePassHeader(name), name)
	}
	for _, name := range []string{"", "bad header", "Accept", "Cache-Control", "User-Agent", "Content-Type", "Forwarded", "X-Forwarded-For", "X-Client-IP", "X-Request-ID", "X-Upstream-Host", "Traceparent", "Sec-Fetch-Site", "Connection", "Content-Length", "X-HTTP-Method-Override"} {
		require.Error(t, ValidatePassHeader(name), name)
	}
}

func TestRedactHTTPErrorPreservesCause(t *testing.T) {
	cause := errors.New("dial failed")
	err := &url.Error{Op: "Get", URL: "https://user:password@example.test/artifact?token=secret#fragment", Err: cause}
	redacted := RedactHTTPError(err)
	require.ErrorIs(t, redacted, cause)
	var urlErr *url.Error
	require.ErrorAs(t, redacted, &urlErr)
	require.Equal(t, "https://example.test/artifact", urlErr.URL)
	require.NotContains(t, redacted.Error(), "password")
	require.NotContains(t, redacted.Error(), "secret")
}

func TestHeaderBoundaryRejectsRequestURLUserInformation(t *testing.T) {
	called := false
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("unexpected request")
	})}
	ConfigureHeaderBoundary(client, DefaultUserAgent)
	request, err := http.NewRequest(http.MethodGet, "https://user:secret@example.test/artifact", nil)
	require.NoError(t, err)
	_, err = client.Do(request)
	require.ErrorContains(t, err, "must not contain user information")
	require.NotContains(t, err.Error(), "secret")
	require.False(t, called)
}

func TestTransportWrappersPreserveIdleConnectionCleanup(t *testing.T) {
	base := &closeTrackingRoundTripper{}
	client := &http.Client{Transport: base}
	ConfigureAdmission(client, new(UpstreamGate))
	ConfigureHeaderBoundary(client, DefaultUserAgent)
	client.CloseIdleConnections()
	require.True(t, base.closed.Load())
}

func TestCopyDownstreamResponseHeadersSanitizesHistoricalCache(t *testing.T) {
	stored := http.Header{
		"Cache-Control":    {"public, max-age=60"},
		"ETag":             {`"representation"`},
		"WWW-Authenticate": {`Bearer realm="registry"`},
		"Server":           {"historical-upstream"},
		"X-Cache":          {"historical-hit"},
		"X-Source-ETag":    {`"internal"`},
		"CF-Ray":           {"edge"},
		"X-Backend-Server": {"origin-1"},
		"X-Cache-Lookup":   {"hit"},
	}
	destination := make(http.Header)
	CopyDownstreamResponseHeaders(destination, stored)
	require.Equal(t, "public, max-age=60", destination.Get("Cache-Control"))
	require.Equal(t, `"representation"`, destination.Get("ETag"))
	require.Equal(t, `Bearer realm="registry"`, destination.Get("WWW-Authenticate"))
	for _, name := range []string{"Server", "X-Cache", "X-Source-ETag", "CF-Ray", "X-Backend-Server", "X-Cache-Lookup"} {
		require.Empty(t, destination.Values(name), name)
	}
}

func TestHeaderBoundaryHandlesNonCanonicalConnectionFields(t *testing.T) {
	request := &http.Request{Header: http.Header{
		"connection":  {"x-private"},
		"x-private":   {"secret"},
		"x-client-ip": {"192.0.2.1"},
	}}
	SanitizeUpstreamRequest(request, DefaultUserAgent)
	require.Empty(t, request.Header.Get("Connection"))
	require.Empty(t, request.Header.Get("X-Private"))
	require.Empty(t, request.Header.Get("X-Client-IP"))
	require.Equal(t, DefaultUserAgent, request.UserAgent())
}

func TestRedactedURLStringRejectsInvalidURL(t *testing.T) {
	require.Empty(t, RedactedURLString("https://example.test/%zz?secret=value"))
	require.Equal(t, "https://example.test/path", RedactedURLString("https://user:pass@example.test/path?secret=value"))
	require.False(t, strings.Contains(RedactedURLString("https://example.test/path?secret=value"), "secret"))
}
