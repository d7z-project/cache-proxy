package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	apkproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/apk"
	debproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/deb"
	pacmanproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/pacman"
	rpmproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/rpm"
)

func TestAPKProxyCachesArtifactsAndRevalidatesMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/main/x86_64/APKINDEX.tar.gz":
			_, _ = io.WriteString(w, "index")
		case "/main/x86_64/busybox-1.0.apk":
			_, _ = io.WriteString(w, "apk")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	spec := apkSpec(t, "apk", "/apk", upstream.URL)
	spec.Policy = mustPolicyJSON(t, &apkproxy.Policy{MetadataFreshFor: config.Freshness(time.Minute), MetadataBusyPolicy: config.BusyPolicyStale, ArtifactPolicy: config.PolicyImmutable, AuxiliaryPolicy: config.PolicyBypass, AuxiliaryBusyPolicy: config.BusyPolicyBypass})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"apk": spec})
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)
	require.Equal(t, "apk", requestBody(t, handler, http.MethodGet, "/apk/main/x86_64/busybox-1.0.apk"))
	require.Equal(t, "apk", requestBody(t, handler, http.MethodGet, "/apk/main/x86_64/busybox-1.0.apk"))
	require.Equal(t, "index", requestBody(t, handler, http.MethodGet, "/apk/main/x86_64/APKINDEX.tar.gz"))
	require.Equal(t, "index", requestBody(t, handler, http.MethodGet, "/apk/main/x86_64/APKINDEX.tar.gz"))
	require.Equal(t, int64(2), requests.Load())
}

func TestDEBProxyClassifiesPoolAndDists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/dists/stable/InRelease":
			_, _ = io.WriteString(w, "release")
		case "/pool/main/h/hello/hello_1.0_amd64.deb":
			_, _ = io.WriteString(w, "deb")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	spec := debSpec(t, "deb", "/deb", upstream.URL)
	spec.Policy = mustPolicyJSON(t, &debproxy.Policy{MetadataFreshFor: config.Freshness(2 * time.Minute), MetadataBusyPolicy: config.BusyPolicyStale, ArtifactPolicy: config.PolicyImmutable, AuxiliaryPolicy: config.PolicyBypass, AuxiliaryBusyPolicy: config.BusyPolicyBypass})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"deb": spec})
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)
	require.Equal(t, "deb", requestBody(t, handler, http.MethodGet, "/deb/pool/main/h/hello/hello_1.0_amd64.deb"))
	require.Equal(t, "deb", requestBody(t, handler, http.MethodGet, "/deb/pool/main/h/hello/hello_1.0_amd64.deb"))
	require.Equal(t, "release", requestBody(t, handler, http.MethodGet, "/deb/dists/stable/InRelease"))
	require.Equal(t, "release", requestBody(t, handler, http.MethodGet, "/deb/dists/stable/InRelease"))
	require.Equal(t, int64(2), requests.Load())
}

func TestRPMProxyRulesCanOverrideMetadataDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/repodata/repomd.xml":
			_, _ = io.WriteString(w, "repomd")
		case "/Packages/h/hello.rpm":
			_, _ = io.WriteString(w, "rpm")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	spec := rpmSpec(t, "rpm", "/rpm", upstream.URL)
	spec.Policy = mustPolicyJSON(t, &rpmproxy.Policy{
		MetadataFreshFor:    config.Freshness(time.Minute),
		MetadataBusyPolicy:  config.BusyPolicyStale,
		ArtifactPolicy:      config.PolicyImmutable,
		AuxiliaryPolicy:     config.PolicyBypass,
		AuxiliaryBusyPolicy: config.BusyPolicyBypass,
		Rules:               []rpmproxy.Rule{{Match: "repodata/**", ResourceClass: "metadata", Policy: config.PolicyBypass}},
	})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"rpm": spec})
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)
	require.Equal(t, "rpm", requestBody(t, handler, http.MethodGet, "/rpm/Packages/h/hello.rpm"))
	require.Equal(t, "rpm", requestBody(t, handler, http.MethodGet, "/rpm/Packages/h/hello.rpm"))
	require.Equal(t, "repomd", requestBody(t, handler, http.MethodGet, "/rpm/repodata/repomd.xml"))
	require.Equal(t, "repomd", requestBody(t, handler, http.MethodGet, "/rpm/repodata/repomd.xml"))
	require.Equal(t, int64(3), requests.Load())
}

func TestPacmanProxyCachesPackageArchives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/core/os/x86_64/core.db":
			_, _ = io.WriteString(w, "db")
		case "/core/os/x86_64/pacman-1.0.pkg.tar.zst":
			_, _ = io.WriteString(w, "pkg")
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	spec := pacmanSpec(t, "pacman", "/pacman", upstream.URL)
	spec.Policy = mustPolicyJSON(t, &pacmanproxy.Policy{MetadataFreshFor: config.Freshness(time.Minute), MetadataBusyPolicy: config.BusyPolicyStale, ArtifactPolicy: config.PolicyImmutable, AuxiliaryPolicy: config.PolicyBypass, AuxiliaryBusyPolicy: config.BusyPolicyBypass})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"pacman": spec})
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)
	require.Equal(t, "pkg", requestBody(t, handler, http.MethodGet, "/pacman/core/os/x86_64/pacman-1.0.pkg.tar.zst"))
	require.Equal(t, "pkg", requestBody(t, handler, http.MethodGet, "/pacman/core/os/x86_64/pacman-1.0.pkg.tar.zst"))
	require.Equal(t, "db", requestBody(t, handler, http.MethodGet, "/pacman/core/os/x86_64/core.db"))
	require.Equal(t, "db", requestBody(t, handler, http.MethodGet, "/pacman/core/os/x86_64/core.db"))
	require.Equal(t, int64(2), requests.Load())
}
