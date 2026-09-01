package pypi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestFileRouteLeavesTargetValidationToHTTPcache(t *testing.T) {
	route := pypiFileRoute("packages/abc", "https://evil.com/malware")
	require.Equal(t, httpcache.ClassContent, route.Class)
	require.Equal(t, "https://evil.com/malware", route.TargetURL)
	require.Empty(t, route.AllowedTargetHosts)
	require.Empty(t, route.UpstreamPath)
}

func TestRouteForPathClassifiesIndexAndFiles(t *testing.T) {
	index, err := routeForPath("simple/example")
	require.NoError(t, err)
	require.Equal(t, httpcache.ClassMetadata, index.Class)
	require.Equal(t, "simple/example/", index.UpstreamPath)

	file, err := routeForPath("packages/somepkg")
	require.NoError(t, err)
	require.Equal(t, httpcache.ClassContent, file.Class)
	require.Empty(t, file.TargetURL)
	require.Equal(t, "packages/somepkg", file.UpstreamPath)
}

func TestResolverDecodesFileTarget(t *testing.T) {
	resolver := resolver{}
	route, err := resolver.Resolve(httptest.NewRequest(http.MethodGet,
		"/files/"+encodeSourceURL("https://pypi.org/packages/example-1.0.tar.gz"), nil))
	require.NoError(t, err)
	require.Equal(t, "https://pypi.org/packages/example-1.0.tar.gz", route.TargetURL)
	require.Equal(t, httpcache.ClassContent, route.Class)
}
