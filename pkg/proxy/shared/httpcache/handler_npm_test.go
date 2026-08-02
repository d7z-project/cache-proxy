package httpcache

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteNPMTarballURLInvalidURL(t *testing.T) {
	result := RewriteNPMTarballURL("://invalid", []string{"https://reg.com"}, "https://proxy/npm")
	require.Equal(t, "://invalid", result)
}

func TestRewriteNPMTarballURLEmptyString(t *testing.T) {
	result := RewriteNPMTarballURL("", []string{"https://reg.com"}, "https://proxy/npm")
	require.Equal(t, "", result)
}

func TestRewriteNPMTarballURLRelativePath(t *testing.T) {
	result := RewriteNPMTarballURL("/pkg/-/pkg-1.0.tgz", []string{"https://reg.com"}, "https://proxy/npm")
	require.Equal(t, "/pkg/-/pkg-1.0.tgz", result)
}

func TestRewriteNPMTarballURLBasicRewrite(t *testing.T) {
	upstreams := []string{"https://registry.npmjs.org"}
	publicBase := "https://cache-proxy.example.com/npm-proxy"
	rawURL := "https://registry.npmjs.org/react/-/react-18.2.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://cache-proxy.example.com/npm-proxy/react/-/react-18.2.0.tgz", result)
}

func TestRewriteNPMTarballURLPreservesQueryString(t *testing.T) {
	upstreams := []string{"https://registry.npmjs.org"}
	publicBase := "https://cache-proxy.example.com/npm"
	rawURL := "https://registry.npmjs.org/react/-/react-18.2.0.tgz?version=1.0"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://cache-proxy.example.com/npm/react/-/react-18.2.0.tgz?version=1.0", result)
}

func TestRewriteNPMTarballURLHostMismatch(t *testing.T) {
	upstreams := []string{"https://registry.npmjs.org"}
	publicBase := "https://cache-proxy.example.com/npm"
	rawURL := "https://other-registry.com/pkg/-/pkg-1.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, rawURL, result)
}

func TestRewriteNPMTarballURLCaseInsensitiveHost(t *testing.T) {
	upstreams := []string{"https://Registry.NPMJS.ORG"}
	publicBase := "https://proxy/npm"
	rawURL := "https://registry.npmjs.org/react/-/react-18.2.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/react/-/react-18.2.0.tgz", result)
}

func TestRewriteNPMTarballURLUpstreamWithBasePath(t *testing.T) {
	upstreams := []string{"https://registry.example.com/npm/private/"}
	publicBase := "https://proxy/npm"
	rawURL := "https://registry.example.com/npm/private/@scope/pkg/-/pkg-1.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/@scope/pkg/-/pkg-1.0.tgz", result)
}

func TestRewriteNPMTarballURLUpstreamBasePathExactMatch(t *testing.T) {
	upstreams := []string{"https://registry.example.com/npm/private"}
	publicBase := "https://proxy/npm"
	rawURL := "https://registry.example.com/npm/private"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/", result)
}

func TestRewriteNPMTarballURLPathNotUnderUpstream(t *testing.T) {
	upstreams := []string{"https://registry.example.com/npm/private"}
	publicBase := "https://proxy/npm"
	rawURL := "https://registry.example.com/other/pkg.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, rawURL, result)
}

func TestRewriteNPMTarballURLMultipleUpstreamsFirstMatch(t *testing.T) {
	upstreams := []string{"https://reg-a.com", "https://reg-b.com"}
	publicBase := "https://proxy/npm"
	rawURL := "https://reg-b.com/pkg/-/pkg-1.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/pkg/-/pkg-1.0.tgz", result)
}

func TestRewriteNPMTarballURLInvalidUpstreamSkipped(t *testing.T) {
	upstreams := []string{"://broken", "https://reg.com"}
	publicBase := "https://proxy/npm"
	rawURL := "https://reg.com/pkg/-/pkg-1.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/pkg/-/pkg-1.0.tgz", result)
}

func TestRewriteNPMTarballURLPublicBaseTrailingSlash(t *testing.T) {
	upstreams := []string{"https://reg.com"}
	publicBase := "https://proxy/npm/"
	rawURL := "https://reg.com/pkg/-/pkg-1.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/pkg/-/pkg-1.0.tgz", result)
}

func TestRewriteNPMTarballURLScopedPackage(t *testing.T) {
	upstreams := []string{"https://registry.npmjs.org"}
	publicBase := "https://proxy/npm"
	rawURL := "https://registry.npmjs.org/@angular/core/-/core-15.0.0.tgz"

	result := RewriteNPMTarballURL(rawURL, upstreams, publicBase)
	require.Equal(t, "https://proxy/npm/@angular/core/-/core-15.0.0.tgz", result)
}

func TestRewriteNPMMetadata(t *testing.T) {
	source := `{"versions":{"1.0.0":{"dist":{"tarball":"https://registry.npmjs.org/pkg/-/pkg-1.0.0.tgz","integrity":"sha512-value"}}},"other":[true,null,42]}`
	var rewritten bytes.Buffer
	require.NoError(t, RewriteNPMMetadata(strings.NewReader(source), &rewritten, []string{"https://registry.npmjs.org"}, "https://proxy/npm"))
	require.JSONEq(t, `{"versions":{"1.0.0":{"dist":{"tarball":"https://proxy/npm/pkg/-/pkg-1.0.0.tgz","integrity":"sha512-value"}}},"other":[true,null,42]}`, rewritten.String())
}

func TestRewriteNPMMetadataRejectsInvalidJSON(t *testing.T) {
	var rewritten bytes.Buffer
	require.Error(t, RewriteNPMMetadata(strings.NewReader(`{"dist":`), &rewritten, []string{"https://registry.npmjs.org"}, "https://proxy/npm"))
}

func TestBaseURLConstructsFromHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/npm/pkg", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "cache.example.com")

	base := BaseURL(req)
	require.Equal(t, "https://cache.example.com", base)
}

func TestBaseURLFallbackToHost(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/npm/pkg", nil)
	req.Host = "localhost:8080"

	base := BaseURL(req)
	require.Equal(t, "http://localhost:8080", base)
}

func TestBaseURLDetectsTLS(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/npm/pkg", nil)
	req.TLS = nil // explicitly not TLS
	req.Host = "proxy.local"

	base := BaseURL(req)
	require.Equal(t, "http://proxy.local", base)
}

func TestPublicBaseURLWithPrefix(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/npm-proxy/react", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "cache.example.com")
	req.Header.Set("X-Cache-Proxy-Prefix", "/npm-proxy")

	base := publicBaseURL(req)
	require.Equal(t, "https://cache.example.com/npm-proxy", base)
}

func TestPublicBaseURLPrefixTrailingSlash(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/npm-proxy/react", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "cache.example.com")
	req.Header.Set("X-Cache-Proxy-Prefix", "/npm-proxy/")

	base := publicBaseURL(req)
	require.Equal(t, "https://cache.example.com/npm-proxy", base)
}
