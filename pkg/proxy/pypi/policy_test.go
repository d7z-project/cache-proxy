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

	route := fileRoute(policy, "packages/abc", "https://evil.com/malware")
	require.Equal(t, "https://evil.com/malware", route.TargetURL)
	require.Empty(t, route.AllowedTargetHosts)
	require.Empty(t, route.UpstreamPath)
}

func TestPyPICompanionDefaultsToImmutableJoin(t *testing.T) {
	policy := &Policy{ProxySignatures: true}
	applyDefaults(policy)
	route := fileRoute(policy, "packages/example.whl.asc", "https://files.pythonhosted.org/packages/example.whl.asc")
	require.Equal(t, config.PolicyImmutable, route.Policy)
	require.Equal(t, config.BusyPolicyJoin, route.BusyPolicy)
}

func TestValidateAllowedFileHosts(t *testing.T) {
	policy := &Policy{AllowedFileHosts: []string{"FILES.PYTHONHOSTED.ORG", "cdn.example:8443"}}
	applyDefaults(policy)
	require.NoError(t, validatePolicy(policy))
	require.Equal(t, []string{"files.pythonhosted.org", "cdn.example:8443"}, policy.AllowedFileHosts)

	for _, invalid := range []string{"https://files.pythonhosted.org", "user@example.com", "host/path"} {
		policy := &Policy{AllowedFileHosts: []string{invalid}}
		applyDefaults(policy)
		require.Error(t, validatePolicy(policy), invalid)
	}
}

func TestFileRouteClassifiesAbsoluteAuxiliaryFiles(t *testing.T) {
	policy := &Policy{
		FilePolicy:      config.PolicyImmutable,
		CompanionPolicy: config.PolicyRevalidate,
		ProxySignatures: true,
	}
	auxRoute := fileRoute(policy, "packages/abc.asc", "https://pypi.org/packages/abc.asc")
	require.Equal(t, "https://pypi.org/packages/abc.asc", auxRoute.TargetURL)
	require.Equal(t, config.PolicyRevalidate, auxRoute.Policy)

	foreignRoute := fileRoute(policy, "packages/abc.asc", "https://evil.com/packages/abc.asc")
	require.Equal(t, "https://evil.com/packages/abc.asc", foreignRoute.TargetURL)
	require.Equal(t, config.PolicyRevalidate, foreignRoute.Policy)
}

func TestRouteForPathDefaultGoesThroughFileRoute(t *testing.T) {
	policy := &Policy{FilePolicy: config.PolicyImmutable}

	route, err := routeForPath(policy, "packages/somepkg")
	require.NoError(t, err)
	require.Empty(t, route.TargetURL)
	require.Equal(t, "packages/somepkg", route.UpstreamPath)
}

func TestRouteForPathURLSchemeSpoof(t *testing.T) {
	policy := &Policy{FilePolicy: config.PolicyImmutable}

	route, err := routeForPath(policy, "http/evil.com/malware")
	require.NoError(t, err)
	require.Empty(t, route.TargetURL)
	require.Equal(t, "http/evil.com/malware", route.UpstreamPath)
}

func TestResolveWithUpstreams(t *testing.T) {
	r := &resolver{
		policy: &Policy{FilePolicy: config.PolicyImmutable},
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
