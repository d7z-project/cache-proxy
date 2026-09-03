package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestClientReleasesAdmissionWhenBodyCloses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "body")
	}))
	defer upstream.Close()
	gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	client, err := NewClient("test", "file", nil, gate, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	require.NoError(t, err)
	response, err := client.DoRead(context.Background(), request, AdmissionForeground)
	require.NoError(t, err)
	require.Equal(t, 1, gate.Snapshot().Active)
	require.NoError(t, response.Body.Close())
	require.Zero(t, gate.Snapshot().Active)
}

func TestClientReadAPIRejectsMutationBeforeUpstream(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer upstream.Close()
	client, err := NewClient("test", "file", nil, nil, nil)
	require.NoError(t, err)

	request, err := http.NewRequest(http.MethodPut, upstream.URL, strings.NewReader("write"))
	require.NoError(t, err)
	_, err = client.DoRead(context.Background(), request, AdmissionForeground)
	require.ErrorContains(t, err, "bodyless GET or HEAD")

	request, err = http.NewRequest(http.MethodGet, upstream.URL, strings.NewReader("body"))
	require.NoError(t, err)
	_, err = client.DoRead(context.Background(), request, AdmissionForeground)
	require.ErrorContains(t, err, "bodyless GET or HEAD")
	require.Zero(t, requests.Load())
}

func TestClientCreatesOneSpoolerConcurrently(t *testing.T) {
	client, err := NewClient("test", "file", nil, nil, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	workDir := t.TempDir()
	results := make(chan *storeio.Spooler, 32)
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- client.EnsureSpooler(workDir)
		}()
	}
	group.Wait()
	close(results)
	var first *storeio.Spooler
	for spooler := range results {
		if first == nil {
			first = spooler
		}
		require.Same(t, first, spooler)
	}
}

func TestClientAppliesUserAgentAndIdleBodyTimeout(t *testing.T) {
	seenUserAgent := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		seenUserAgent <- request.UserAgent()
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, "b")
	}))
	t.Cleanup(upstream.Close)
	gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	client, err := NewClient("test", "file", &config.TransportConfig{
		UserAgent: "cache-proxy-test/1", IdleBodyTimeout: config.Duration(20 * time.Millisecond),
	}, gate, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	require.NoError(t, err)
	response, err := client.DoRead(context.Background(), request, AdmissionForeground)
	require.NoError(t, err)
	buffer := make([]byte, 1)
	_, err = io.ReadFull(response.Body, buffer)
	require.NoError(t, err)
	_, err = response.Body.Read(buffer)
	require.ErrorIs(t, err, ErrIdleBodyTimeout)
	require.Equal(t, "cache-proxy-test/1", <-seenUserAgent)
	require.Eventually(t, func() bool { return gate.Snapshot().Active == 0 }, time.Second, time.Millisecond)
}

func TestClientBoundsStalledResponseHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	t.Cleanup(upstream.Close)
	gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	client, err := NewClient("test", "file", &config.TransportConfig{HeaderTimeout: config.Duration(20 * time.Millisecond)}, gate, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	require.NoError(t, err)
	_, err = client.DoRead(context.Background(), request, AdmissionForeground)
	require.Error(t, err)
	require.Eventually(t, func() bool { return gate.Snapshot().Active == 0 }, time.Second, time.Millisecond)
}

func TestRedirectDowngradeStripsCredentials(t *testing.T) {
	client, err := NewClient("test", "file", nil, proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{}), metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	previous := httptest.NewRequest(http.MethodGet, "https://example.test/source", nil)
	next := httptest.NewRequest(http.MethodGet, "http://example.test/target", nil)
	next.Header.Set("Authorization", "Bearer secret")
	next.Header.Set("Cookie", "session=secret")
	require.NoError(t, client.httpClient.CheckRedirect(next, []*http.Request{previous}))
	require.Empty(t, next.Header.Get("Authorization"))
	require.Empty(t, next.Header.Get("Cookie"))
}

func TestReadOnlyPostRedirectCannotChangeOperation(t *testing.T) {
	previous := httptest.NewRequest(http.MethodPost, "https://registry.test/git-upload-pack", strings.NewReader("request"))
	for _, target := range []string{
		"https://registry.test/git-receive-pack",
		"https://other.test/git-upload-pack",
	} {
		next := httptest.NewRequest(http.MethodPost, target, strings.NewReader("request"))
		require.Error(t, CheckReadOnlyRedirect(next, []*http.Request{previous}))
	}
	next := httptest.NewRequest(http.MethodPost, "https://registry.test/git-upload-pack?state=next", strings.NewReader("request"))
	require.NoError(t, CheckReadOnlyRedirect(next, []*http.Request{previous}))
}

func TestReadOnlyPostRedirectCannotBecomeUpstreamWrite(t *testing.T) {
	var queryRequests, writeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/query":
			queryRequests.Add(1)
			http.Redirect(w, request, "/write", http.StatusFound)
		case "/write":
			writeRequests.Add(1)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	client, err := NewClient("test", "npm", nil, nil, nil)
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/query", strings.NewReader("query"))
	require.NoError(t, err)
	_, err = client.DoReadOnlyPost(context.Background(), request, AdmissionForeground)
	require.ErrorContains(t, err, "non-read redirect changed upstream operation")
	require.Equal(t, int32(1), queryRequests.Load())
	require.Zero(t, writeRequests.Load())
}

func TestSameOriginIncludesSchemeAndEffectivePort(t *testing.T) {
	httpsDefault, err := url.Parse("https://example.test/path")
	require.NoError(t, err)
	httpsExplicit, err := url.Parse("https://EXAMPLE.test:443/other")
	require.NoError(t, err)
	httpURL, err := url.Parse("http://example.test/path")
	require.NoError(t, err)
	httpsOtherPort, err := url.Parse("https://example.test:8443/path")
	require.NoError(t, err)
	require.True(t, SameOrigin(httpsDefault, httpsExplicit))
	require.False(t, SameOrigin(httpsDefault, httpURL))
	require.False(t, SameOrigin(httpsDefault, httpsOtherPort))
}

func TestClientsDisableCompression(t *testing.T) {
	client, err := NewClient("test", "file", nil, proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{}), metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	httpTransport, ok := client.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.True(t, httpTransport.DisableCompression)
	require.Equal(t, int64(maxResponseHeaderBytes), httpTransport.MaxResponseHeaderBytes)

	wrapper := NewUpstreamHTTPClient()
	ConfigureHTTPClient(wrapper, "test", nil)
	wrapperTransport, ok := wrapper.Transport.(*http.Transport)
	require.True(t, ok)
	require.True(t, wrapperTransport.DisableCompression)
	require.Equal(t, int64(maxResponseHeaderBytes), wrapperTransport.MaxResponseHeaderBytes)
}

func TestClientsStripCredentialsOnCrossOriginRedirect(t *testing.T) {
	var authorization, cookie string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		authorization = req.Header.Get("Authorization")
		cookie = req.Header.Get("Cookie")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	assertStripped := func(do func(*http.Request) (*http.Response, error)) {
		authorization, cookie = "unseen", "unseen"
		request, err := http.NewRequest(http.MethodGet, origin.URL, nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer secret")
		request.Header.Set("Cookie", "session=secret")
		response, err := do(request)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.Empty(t, authorization)
		require.Empty(t, cookie)
	}

	client, err := NewClient("test", "file", nil, proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{}), metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	assertStripped(func(request *http.Request) (*http.Response, error) {
		return client.DoRead(context.Background(), request, AdmissionForeground)
	})
	wrapper := NewUpstreamHTTPClient()
	ConfigureHTTPClient(wrapper, "test", nil)
	assertStripped(wrapper.Do)
}

func TestRedirectUsesAdmissionForActualHost(t *testing.T) {
	gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	snapshots := make(chan proxyruntime.UpstreamGateSnapshot, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		snapshots <- gate.Snapshot()
		_, _ = io.WriteString(w, "target")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	client, err := NewClient("test", "file", nil, gate, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, origin.URL, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.DoRead(ctx, request, AdmissionForeground)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	targetURL, err := url.Parse(target.URL)
	require.NoError(t, err)
	originURL, err := url.Parse(origin.URL)
	require.NoError(t, err)
	snapshot := <-snapshots
	require.Equal(t, 1, snapshot.Hosts[targetURL.Host].Active)
	require.Zero(t, snapshot.Hosts[originURL.Host].Active)
	require.Zero(t, gate.Snapshot().Active)
}

func TestRedirectRateLimitBelongsToActualHost(t *testing.T) {
	gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 2, MaxActivePerHost: 1})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	client, err := NewClient("test", "file", nil, gate, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, origin.URL, nil)
	require.NoError(t, err)
	response, err := client.DoRead(context.Background(), request, AdmissionForeground)
	require.NoError(t, err)
	require.Equal(t, http.StatusTooManyRequests, response.StatusCode)
	require.NoError(t, response.Body.Close())

	_, err = gate.Acquire(context.Background(), target.URL, AdmissionRefresh)
	var limited *proxyruntime.UpstreamRateLimitError
	require.ErrorAs(t, err, &limited)
	release, err := gate.Acquire(context.Background(), origin.URL, AdmissionRefresh)
	require.NoError(t, err)
	release()
}

func TestJoinURLPreservesEscapingAndQuery(t *testing.T) {
	base, err := url.Parse("https://example.test/root")
	require.NoError(t, err)
	target, err := JoinURL(base, "scope/%40name", "a=1")
	require.NoError(t, err)
	require.Equal(t, "https://example.test/root/scope/%40name?a=1", target.String())
	secretURL, err := url.Parse("https://user:secret@example.test/path?token=secret")
	require.NoError(t, err)
	require.NotContains(t, RedactedURL(secretURL), "secret")
	require.False(t, strings.Contains(RedactedURL(target), "a=1"))
}

func TestCopyEndToEndHeadersRemovesConnectionHeaders(t *testing.T) {
	source := http.Header{
		"Connection":          {"keep-alive, X-Internal"},
		"Keep-Alive":          {"timeout=5"},
		"X-Internal":          {"secret"},
		"X-Protocol-Response": {"preserved"},
	}
	destination := http.Header{}

	CopyEndToEndHeaders(destination, source)

	require.Empty(t, destination.Get("Connection"))
	require.Empty(t, destination.Get("Keep-Alive"))
	require.Empty(t, destination.Get("X-Internal"))
	require.Equal(t, "preserved", destination.Get("X-Protocol-Response"))
}
