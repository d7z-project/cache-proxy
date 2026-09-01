package maven

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestResolverClassifiesReleaseAndSnapshotCompanions(t *testing.T) {
	resolver := &resolver{}
	tests := []struct {
		path  string
		class httpcache.ObjectClass
	}{
		{path: "/org/example/lib/1.0/lib-1.0.jar.sha256", class: httpcache.ClassContent},
		{path: "/org/example/lib/1.1-SNAPSHOT/lib-1.1-20260802.1.jar.sha256", class: httpcache.ClassMetadata},
		{path: "/org/example/lib/maven-metadata.xml.sha256", class: httpcache.ClassMetadata},
	}
	for _, test := range tests {
		route, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, test.path, nil))
		require.NoError(t, err)
		require.Equal(t, test.class, route.Class, test.path)
	}
}
