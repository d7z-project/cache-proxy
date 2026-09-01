package flatpak

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

func TestResolverClassifiesProtocolObjects(t *testing.T) {
	r := resolver{}
	metadata, err := r.Resolve(httptest.NewRequest(http.MethodGet, "/summary", nil))
	require.NoError(t, err)
	require.Equal(t, httpcache.ClassMetadata, metadata.Class)
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte("object")))
	object, err := r.Resolve(httptest.NewRequest(http.MethodGet, "/objects/"+digest[:2]+"/"+digest[2:]+".commit", nil))
	require.NoError(t, err)
	require.Equal(t, httpcache.ClassContent, object.Class)
}

func TestDescriptorTransformRewritesURL(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://proxy.test/repo.flatpakrepo", nil)
	response := &utils.ResponseWrapper{StatusCode: http.StatusOK, Headers: map[string]string{}, Body: io.NopCloser(bytes.NewBufferString("[Flatpak Repo]\nUrl=https://upstream.test/repo\n"))}
	result := transformDescriptor(req, httpcache.Route{UpstreamPath: "repo.flatpakrepo"}, response)
	body, err := io.ReadAll(result.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "Url=https://proxy.test")
}

func FuzzFlatpakPathClassification(f *testing.F) {
	f.Add("summary")
	f.Add("objects/00/invalid.commit")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		_ = isMetadataPath(value)
		_ = isDescriptorPath(value)
		_ = isDeltaPath(value)
		_ = isObjectPath(value)
	})
}
