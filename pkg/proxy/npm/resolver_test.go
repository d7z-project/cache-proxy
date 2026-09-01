package npm

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestResolverClassifiesObjects(t *testing.T) {
	resolver := New()

	tarball, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, "/@scope/pkg/-/pkg-1.0.0.tgz", nil))
	require.NoError(t, err)
	require.Equal(t, httpcache.ClassContent, tarball.Class)
	require.Equal(t, "npm/tarballs/@scope/pkg/-/pkg-1.0.0.tgz", tarball.ObjectPath)
	require.Equal(t, "@scope/pkg/-/pkg-1.0.0.tgz", tarball.UpstreamPath)

	metadata, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, "/@scope%2fpkg", nil))
	require.NoError(t, err)
	require.Equal(t, httpcache.ClassMetadata, metadata.Class)
	require.True(t, httpcache.SafePath(metadata.ObjectPath))
}

func TestResolverIncludesQueryInObjectKey(t *testing.T) {
	resolver := New()
	withQuery, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, "/react?version=latest", nil))
	require.NoError(t, err)
	withoutQuery, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, "/react", nil))
	require.NoError(t, err)
	require.NotEqual(t, withoutQuery.ObjectPath, withQuery.ObjectPath)
	require.Equal(t, "react?version=latest", withQuery.UpstreamPath)
}

func TestResolverSeparatesAbbreviatedMetadataRepresentation(t *testing.T) {
	resolver := New()
	full, err := resolver.Resolve(httptest.NewRequest(http.MethodGet, "/react", nil))
	require.NoError(t, err)

	abbreviatedRequest := httptest.NewRequest(http.MethodGet, "/react", nil)
	abbreviatedRequest.Header.Set("Accept", abbreviatedMetadataType+", application/json")
	abbreviated, err := resolver.Resolve(abbreviatedRequest)
	require.NoError(t, err)
	require.NotEqual(t, full.ObjectPath, abbreviated.ObjectPath)
	require.Empty(t, full.RequestHeaders)
	require.Equal(t, abbreviatedMetadataType, abbreviated.RequestHeaders["Accept"])
}
