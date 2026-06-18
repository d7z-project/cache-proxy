package filerepo

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestResolverAppliesResourceDefaultsAndRuleOverrides(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/dists/stable/InRelease", nil)
	require.NoError(t, err)

	route, err := NewResolver(Config{
		ObjectRoot: "deb",
		Defaults: Defaults{
			Metadata: CacheProfile{
				Policy:     config.PolicyRevalidate,
				FreshFor:   config.Freshness(2 * time.Minute),
				BusyPolicy: config.BusyPolicyStale,
			},
			Artifact:  CacheProfile{Policy: config.PolicyImmutable},
			Auxiliary: CacheProfile{Policy: config.PolicyBypass},
		},
		Rules: []Rule{{
			Match:         "dists/**",
			ResourceClass: ResourceMetadata,
			Policy:        config.PolicyImmutable,
			FreshFor:      config.Freshness(30 * time.Second),
			ExpireAfter:   config.Expiration(time.Hour),
		}},
		Classifier: func(cleanPath string) ResourceClass {
			if cleanPath == "dists/stable/InRelease" {
				return ResourceMetadata
			}
			return ResourceAuxiliary
		},
	}).Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "deb/dists/stable/InRelease", route.ObjectPath)
	require.Equal(t, config.PolicyImmutable, route.Policy)
	require.Equal(t, config.Freshness(30*time.Second), route.FreshFor)
	require.Equal(t, config.Expiration(time.Hour), route.ExpireAfter)
}

func TestResolverRejectsInvalidPaths(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/bad\\path", nil)
	require.NoError(t, err)

	_, err = NewResolver(Config{ObjectRoot: "file"}).Resolve(req)
	require.Error(t, err)
}

func TestResolverUsesConfiguredEmptyPath(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	route, err := NewResolver(Config{
		ObjectRoot: "file",
		EmptyPath:  "index",
		Defaults: Defaults{
			Auxiliary: CacheProfile{Policy: config.PolicyBypass},
		},
		Classifier: func(string) ResourceClass { return ResourceAuxiliary },
	}).Resolve(req)
	require.NoError(t, err)
	require.Equal(t, "file/index", route.ObjectPath)
}
