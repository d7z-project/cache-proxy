package cargo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestCargoConfigRouteDoesNotDependOnCachePublication(t *testing.T) {
	crateServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("crate-bytes"))
	}))
	defer crateServer.Close()
	indexServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/config.json" {
			http.NotFound(w, req)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"dl": crateServer.URL})
	}))
	defer indexServer.Close()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	options := &Options{}
	handler := httpcache.NewHandler("cargo", httpcache.RuntimeConfig{
		Mode: config.ModeCargo, ExpireAfter: config.DefaultRetention, MetadataTTL: time.Minute,
		Upstreams: []string{indexServer.URL}, AllowedTargetHosts: []string{strings.TrimPrefix(crateServer.URL, "http://")},
		UpstreamGate:      httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{MaxActive: 8, MaxActivePerHost: 8}),
		ResponseTransform: httpcache.CargoResponseTransform,
	}, store, newResolver(options), httpcache.NewStats(prometheus.NewRegistry()))
	t.Cleanup(func() { require.NoError(t, handler.CloseContext(context.Background())) })

	configResponse := httptest.NewRecorder()
	handler.ServeHTTP(configResponse, httptest.NewRequest(http.MethodGet, "http://cache.example/config.json", nil))
	require.Equal(t, http.StatusOK, configResponse.Code)
	var rewritten struct {
		DownloadURL string `json:"dl"`
	}
	require.NoError(t, json.Unmarshal(configResponse.Body.Bytes(), &rewritten))
	downloadURL := strings.NewReplacer("{crate}", "example", "{version}", "1.0.0").Replace(rewritten.DownloadURL)

	crateResponse := httptest.NewRecorder()
	handler.ServeHTTP(crateResponse, httptest.NewRequest(http.MethodGet, downloadURL, nil))
	require.Equal(t, http.StatusOK, crateResponse.Code)
	require.Equal(t, "crate-bytes", crateResponse.Body.String())
}

func TestCrateDownloadRouteUsesEncodedTemplate(t *testing.T) {
	token, needsChecksum, err := httpcache.EncodeCargoDownloadTemplate("https://cdn.example/{lowerprefix}/{crate}/{crate}-{version}.crate?source=index")
	require.NoError(t, err)
	require.False(t, needsChecksum)

	resolved, err := newResolver(&Options{}).Resolve(httptest.NewRequest(
		http.MethodGet,
		"/api/v1/crates/MyCrate/1.2.3/download/"+token,
		nil,
	))
	require.NoError(t, err)
	require.Equal(t, "https://cdn.example/my/cr/MyCrate/MyCrate-1.2.3.crate?source=index", resolved.TargetURL)
	require.Empty(t, resolved.AllowedTargetHosts)
}

func TestCrateDownloadRouteCarriesCargoChecksum(t *testing.T) {
	token, needsChecksum, err := httpcache.EncodeCargoDownloadTemplate("https://cdn.example/{crate}/{sha256-checksum}.crate")
	require.NoError(t, err)
	require.True(t, needsChecksum)
	checksum := strings.Repeat("ab", 32)

	resolved, err := newResolver(&Options{}).Resolve(httptest.NewRequest(
		http.MethodGet,
		"/api/v1/crates/example/1.0.0/download/"+token+"/"+checksum,
		nil,
	))
	require.NoError(t, err)
	require.Equal(t, "https://cdn.example/example/"+checksum+".crate", resolved.TargetURL)
}

func TestCrateDownloadRouteRejectsMalformedValues(t *testing.T) {
	token, _, err := httpcache.EncodeCargoDownloadTemplate("https://cdn.example/{crate}/{version}.crate")
	require.NoError(t, err)
	for _, requestPath := range []string{
		"/api/v1/crates/example/1.0.0/download/not-base64!",
		"/api/v1/crates/example/../download/" + token,
		"/api/v1/crates/example/1.0.0/download/" + token + "/unexpected",
		"/api/v1/crates/ex%7Bample/1.0.0/download/" + token,
	} {
		_, err := newResolver(&Options{}).Resolve(httptest.NewRequest(http.MethodGet, requestPath, nil))
		require.Error(t, err, requestPath)
	}
}

func TestCratePrefix(t *testing.T) {
	require.Equal(t, "1", cratePrefix("a"))
	require.Equal(t, "2", cratePrefix("ab"))
	require.Equal(t, "3/a", cratePrefix("abc"))
	require.Equal(t, "My/Cr", cratePrefix("MyCrate"))
}
