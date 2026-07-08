package npm

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestResolverClassifiesTarball(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyStale,
		TarballPolicy:      config.PolicyImmutable,
	}
	r := New(cfg)

	req, _ := http.NewRequest(http.MethodGet, "/@scope/pkg/-/pkg-1.0.0.tgz", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "npm/tarballs/@scope/pkg/-/pkg-1.0.0.tgz", route.ObjectPath)
	require.Equal(t, "@scope/pkg/-/pkg-1.0.0.tgz", route.UpstreamPath)
	require.Equal(t, config.PolicyImmutable, route.Policy)
	require.Empty(t, route.RewriteKind)
}

func TestResolverClassifiesMetadata(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyStale,
		TarballPolicy:      config.PolicyImmutable,
	}
	r := New(cfg)

	req, _ := http.NewRequest(http.MethodGet, "/@scope%2fpkg", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.True(t, httpcache.SafePath(route.ObjectPath))
	require.Equal(t, "npm-metadata", route.RewriteKind)
	require.Equal(t, config.PolicyRevalidate, route.Policy)
}

func TestResolverTarballWithQueryString(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy: config.PolicyRevalidate,
		TarballPolicy:  config.PolicyImmutable,
	}
	r := New(cfg)

	req, _ := http.NewRequest(http.MethodGet, "/pkg/-/pkg-1.0.tgz?version=1", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Contains(t, route.ObjectPath, "npm/tarballs/pkg/-/pkg-1.0.tgz/")
	require.Equal(t, "pkg/-/pkg-1.0.tgz?version=1", route.UpstreamPath)
}

func TestResolverMetadataWithQueryString(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy: config.PolicyRevalidate,
	}
	r := New(cfg)

	req, _ := http.NewRequest(http.MethodGet, "/react?version=latest", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "npm-metadata", route.RewriteKind)
	require.Equal(t, "react?version=latest", route.UpstreamPath)
}

func TestResolverNormalizesTraversalPath(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy: config.PolicyRevalidate,
	}
	r := New(cfg)

	// path.Clean resolves ".." segments, so this becomes "etc/passwd"
	req, _ := http.NewRequest(http.MethodGet, "/../etc/passwd", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "npm-metadata", route.RewriteKind)
	require.Equal(t, "etc/passwd", route.UpstreamPath)
}

func TestResolverNormalizesDoubleSlashPath(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy: config.PolicyRevalidate,
		TarballPolicy:  config.PolicyImmutable,
	}
	r := New(cfg)

	// Go URL parser interprets "//etc" as host, so Path becomes "/passwd.tgz"
	req, _ := http.NewRequest(http.MethodGet, "//etc/passwd.tgz", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, config.PolicyImmutable, route.Policy)
	require.Equal(t, "passwd.tgz", route.UpstreamPath)
}

func TestResolverMetadataFreshFor(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy:   config.PolicyRevalidate,
		MetadataFreshFor: 300,
	}
	r := New(cfg)

	req, _ := http.NewRequest(http.MethodGet, "/react", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, config.Freshness(300), route.FreshFor)
}

func TestResolverTarballNoFreshFor(t *testing.T) {
	cfg := &Policy{
		MetadataPolicy: config.PolicyRevalidate,
		TarballPolicy:  config.PolicyImmutable,
	}
	r := New(cfg)

	req, _ := http.NewRequest(http.MethodGet, "/pkg/-/pkg-1.0.tgz", nil)
	route, err := r.Resolve(req)
	require.NoError(t, err)
	require.Equal(t, config.Freshness(0), route.FreshFor)
}

func TestValidateRejectsInvalidMetadataPolicy(t *testing.T) {
	policy := &Policy{
		MetadataPolicy:     "invalid-policy",
		MetadataBusyPolicy: config.BusyPolicyStale,
		TarballPolicy:      config.PolicyImmutable,
	}
	err := validate(policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid npm metadata policy")
}

func TestValidateRejectsInvalidTarballPolicy(t *testing.T) {
	policy := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyStale,
		TarballPolicy:      "invalid-policy",
	}
	err := validate(policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid npm tarball policy")
}

func TestValidateRejectsInvalidBusyPolicy(t *testing.T) {
	policy := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: "unknown",
		TarballPolicy:      config.PolicyImmutable,
	}
	err := validate(policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid npm metadata busy policy")
}

func TestValidateRejectsTooSmallFreshFor(t *testing.T) {
	policy := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyStale,
		MetadataFreshFor:   1, // 1 nanosecond < 1 second
		TarballPolicy:      config.PolicyImmutable,
	}
	err := validate(policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fresh_for must be at least 1s")
}

func TestValidateAcceptsBusyBypass(t *testing.T) {
	policy := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyBypass,
		TarballPolicy:      config.PolicyImmutable,
	}
	err := validate(policy)
	require.NoError(t, err)
}

func TestValidateAcceptsBusyStale(t *testing.T) {
	policy := &Policy{
		MetadataPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyStale,
		TarballPolicy:      config.PolicyImmutable,
	}
	err := validate(policy)
	require.NoError(t, err)
}
