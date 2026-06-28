package pypi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestFileRouteLeavesForeignHostForHttpcacheValidation(t *testing.T) {
	policy := &Policy{FilePolicy: config.PolicyImmutable}
	upstreams := []string{"https://pypi.org"}

	route := fileRoute(policy, upstreams, "packages/abc", "https://evil.com/malware")
	require.Equal(t, "https://evil.com/malware", route.TargetURL)
	require.Empty(t, route.UpstreamPath)
}

func TestFileRouteAcceptsUpstreamHost(t *testing.T) {
	policy := &Policy{FilePolicy: config.PolicyImmutable}
	upstreams := []string{"https://files.pythonhosted.org"}

	route := fileRoute(policy, upstreams, "packages/abc", "https://files.pythonhosted.org/packages/abc.whl")
	require.Equal(t, "https://files.pythonhosted.org/packages/abc.whl", route.TargetURL)
	require.Equal(t, config.PolicyImmutable, route.Policy)
}

func TestFileRouteAuxiliaryOnlyWithUpstreamHost(t *testing.T) {
	policy := &Policy{
		FilePolicy:      config.PolicyImmutable,
		CompanionPolicy: config.PolicyRevalidate,
		ProxySignatures: true,
	}
	upstreams := []string{"https://pypi.org"}

	auxRoute := fileRoute(policy, upstreams, "packages/abc.asc", "https://pypi.org/packages/abc.asc")
	require.Equal(t, "https://pypi.org/packages/abc.asc", auxRoute.TargetURL)
	require.Equal(t, config.PolicyRevalidate, auxRoute.Policy)

	foreignRoute := fileRoute(policy, upstreams, "packages/abc.asc", "https://evil.com/packages/abc.asc")
	require.Equal(t, "https://evil.com/packages/abc.asc", foreignRoute.TargetURL)
	require.Equal(t, config.PolicyRevalidate, foreignRoute.Policy)
}

func TestRouteForPathDefaultGoesThroughFileRoute(t *testing.T) {
	policy := &Policy{FilePolicy: config.PolicyImmutable}
	upstreams := []string{"https://pypi.org"}

	route, err := routeForPath(policy, upstreams, "packages/somepkg")
	require.NoError(t, err)
	require.Empty(t, route.TargetURL)
	require.Equal(t, "packages/somepkg", route.UpstreamPath)
}

func TestRouteForPathURLSchemeSpoof(t *testing.T) {
	policy := &Policy{FilePolicy: config.PolicyImmutable}
	upstreams := []string{"https://pypi.org"}

	route, err := routeForPath(policy, upstreams, "http/evil.com/malware")
	require.NoError(t, err)
	require.Empty(t, route.TargetURL)
	require.Equal(t, "http/evil.com/malware", route.UpstreamPath)
}

func TestResolveWithUpstreams(t *testing.T) {
	r := &resolver{
		policy:    &Policy{FilePolicy: config.PolicyImmutable},
		upstreams: []string{"https://pypi.org"},
	}

	route, err := r.Resolve(httptest.NewRequest(http.MethodGet, "/simple/", nil))
	require.NoError(t, err)
	require.Equal(t, "pypi-simple", route.RewriteKind)

	req, _ := http.NewRequest(http.MethodGet, "/files/"+encodeSourceURL("https://pypi.org/packages/example-1.0.tar.gz"), nil)
	route, err = r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "https://pypi.org/packages/example-1.0.tar.gz", route.TargetURL)

	req, _ = http.NewRequest(http.MethodGet, "/files/"+encodeSourceURL("https://evil.com/malware.tar.gz"), nil)
	route, err = r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "https://evil.com/malware.tar.gz", route.TargetURL)
	require.Empty(t, route.UpstreamPath)
}
