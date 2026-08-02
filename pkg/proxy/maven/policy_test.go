package maven

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestResolverClassifiesReleaseAndSnapshotCompanions(t *testing.T) {
	policy := &Policy{
		ReleasePolicy:      config.PolicyImmutable,
		SnapshotPolicy:     config.PolicyRevalidate,
		ChecksumPolicy:     config.PolicyRevalidate,
		MetadataBusyPolicy: config.BusyPolicyStale,
	}
	applyDefaults(policy)
	resolver := newResolver(policy)
	tests := []struct {
		path   string
		policy string
	}{
		{path: "/org/example/lib/1.0/lib-1.0.jar.sha256", policy: config.PolicyImmutable},
		{path: "/org/example/lib/1.1-SNAPSHOT/lib-1.1-20260802.1.jar.sha256", policy: config.PolicyRevalidate},
		{path: "/org/example/lib/maven-metadata.xml.sha256", policy: config.PolicyRevalidate},
	}
	for _, test := range tests {
		route, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, test.path, nil))
		require.NoError(t, err)
		require.Equal(t, test.policy, route.Policy, test.path)
		require.Equal(t, config.BusyPolicyJoin, route.BusyPolicy, test.path)
	}
}
