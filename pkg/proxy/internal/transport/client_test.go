package transport

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestClientDoReadPathBuildsSanitizedUpstreamRequest(t *testing.T) {
	requests := make(chan *http.Request, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.Clone(request.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	origin, err := url.Parse(upstream.URL + "/root/")
	require.NoError(t, err)
	client, err := NewClient("test", "file", nil, nil, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)

	header := http.Header{
		"Accept":          {"application/json"},
		"Content-Type":    {"text/plain"},
		"X-Forwarded-For": {"192.0.2.1"},
	}
	response, err := client.DoReadPath(context.Background(), origin, http.MethodGet, "/scope/a b/#file", "page=1", header, AdmissionForeground)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	seen := <-requests
	require.Equal(t, "/root/scope/a%20b/%23file?page=1", seen.RequestURI)
	require.Equal(t, "application/json", seen.Header.Get("Accept"))
	require.Empty(t, seen.Header.Get("Content-Type"))
	require.Empty(t, seen.Header.Get("X-Forwarded-For"))
	require.Equal(t, DefaultUserAgent, seen.UserAgent())
	require.Equal(t, "1.1 gateway", seen.Header.Get("Via"))
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

func TestClientFailureReleasesAdmissionAcrossHTTPVersions(t *testing.T) {
	for _, protocol := range []string{"http1", "http2"} {
		for _, fault := range []string{"headers", "body", "disconnect"} {
			t.Run(protocol+"/"+fault, func(t *testing.T) {
				seen := make(chan int, 1)
				upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					seen <- r.ProtoMajor
					if fault == "disconnect" {
						panic(http.ErrAbortHandler)
					}
					if fault == "body" {
						w.Header().Set("Content-Length", "2")
						_, _ = io.WriteString(w, "a")
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
				}))
				upstream.EnableHTTP2 = protocol == "http2"
				upstream.StartTLS()
				t.Cleanup(upstream.Close)
				gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
				client, err := NewClient("test", "rpm", &config.TransportConfig{HeaderTimeout: config.Duration(200 * time.Millisecond), IdleBodyTimeout: config.Duration(100 * time.Millisecond)}, gate, metrics.NewStats(prometheus.NewRegistry()))
				require.NoError(t, err)
				boundary := client.httpClient.Transport.(*headerBoundaryRoundTripper)
				base := boundary.base.(*admissionRoundTripper).base.(*http.Transport)
				base.TLSClientConfig = upstream.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
				t.Cleanup(base.CloseIdleConnections)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
				require.NoError(t, err)
				response, err := client.DoRead(ctx, request, AdmissionForeground)
				if fault == "body" {
					require.NoError(t, err)
					_, err = io.ReadAll(response.Body)
					require.NoError(t, response.Body.Close())
					require.ErrorIs(t, err, ErrIdleBodyTimeout)
				}
				require.Error(t, err)
				var timeout net.Error
				if fault == "disconnect" {
					require.False(t, errors.As(err, &timeout) && timeout.Timeout())
				} else {
					require.ErrorAs(t, err, &timeout)
					require.True(t, timeout.Timeout())
				}
				select {
				case major := <-seen:
					if protocol == "http2" {
						require.Equal(t, 2, major)
					} else {
						require.Equal(t, 1, major)
					}
				case <-ctx.Done():
					t.Fatal("upstream did not receive request")
				}
				require.Eventually(t, func() bool { return gate.Snapshot().Active == 0 }, time.Second, time.Millisecond)
			})
		}
	}
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
	boundary, ok := client.httpClient.Transport.(*headerBoundaryRoundTripper)
	require.True(t, ok)
	base := boundary.base
	if admission, wrapped := base.(*admissionRoundTripper); wrapped {
		base = admission.base
	}
	httpTransport, ok := base.(*http.Transport)
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
	var received http.Header
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		received = req.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	assertStripped := func(do func(*http.Request) (*http.Response, error)) {
		received = nil
		request, err := http.NewRequest(http.MethodGet, origin.URL, nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer secret")
		request.Header.Set("Cookie", "session=secret")
		request.Header.Set("Git-Protocol", "version=2")
		request.Header.Set("X-Api-Key", "custom-secret")
		request.Header.Set("X-Request-ID", "private-id")
		request.Header.Set("Accept", "application/octet-stream")
		response, err := do(request)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.Equal(t, "application/octet-stream", received.Get("Accept"))
		for _, name := range []string{"Authorization", "Cookie", "Git-Protocol", "X-Api-Key", "X-Request-ID"} {
			require.Empty(t, received.Values(name), name)
		}
	}

	client, err := NewClient("test", "file", nil, proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{}), metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	assertStripped(func(request *http.Request) (*http.Response, error) {
		return client.DoRead(context.Background(), request, AdmissionForeground)
	})
	wrapper := NewUpstreamHTTPClient()
	ConfigureHTTPClient(wrapper, "test", nil)
	ConfigureHeaderBoundary(wrapper.Client, wrapper.UserAgent)
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

func TestReadNoRedirectReturnsFirstResponse(t *testing.T) {
	var followed atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/final" {
			followed.Store(true)
			_, _ = io.WriteString(w, "final")
			return
		}
		http.Redirect(w, request, "/final", http.StatusFound)
	}))
	defer upstream.Close()
	client, err := NewClient("test", "openvsx", nil, nil, metrics.NewStats(prometheus.NewRegistry()))
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, upstream.URL+"/start", nil)
	require.NoError(t, err)
	response, err := client.DoReadNoRedirect(context.Background(), request, AdmissionForeground)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusFound, response.StatusCode)
	require.Equal(t, "/final", response.Header.Get("Location"))
	require.False(t, followed.Load())
}

func TestCopyDownstreamResponseHeadersRemovesPrivateAndConnectionHeaders(t *testing.T) {
	source := http.Header{
		"Connection":          {"keep-alive, X-Internal"},
		"Keep-Alive":          {"timeout=5"},
		"X-Internal":          {"secret"},
		"X-Protocol-Response": {"preserved"},
		"Server":              {"upstream"},
	}
	destination := http.Header{}

	CopyDownstreamResponseHeaders(destination, source)

	require.Empty(t, destination.Get("Connection"))
	require.Empty(t, destination.Get("Keep-Alive"))
	require.Empty(t, destination.Get("X-Internal"))
	require.Empty(t, destination.Get("Server"))
	require.Equal(t, "preserved", destination.Get("X-Protocol-Response"))
}
