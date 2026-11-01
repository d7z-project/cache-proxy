package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestUncachedTransformValidatorsAndCleanup(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("If-None-Match"))
		require.Empty(t, r.Header.Get("Range"))
		w.Header().Set("ETag", `"upstream"`)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = io.WriteString(w, "original")
	}))
	defer upstream.Close()
	origin, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	client, err := NewClient("test", "npm", nil, nil, nil)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	root := t.TempDir()
	budget := proxyruntime.NewSpoolBudget(8192)
	spooler := storeio.NewSpooler(root, 4096, budget)
	rewrite := func(_ io.Reader, out io.Writer, _ http.Header) error {
		_, err := io.WriteString(out, "rewritten")
		return err
	}
	var etag string
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodGet} {
		req := httptest.NewRequest(method, "/metadata", nil)
		if etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		w := httptest.NewRecorder()
		status, err := ForwardTransformed(req.Context(), client, origin, w, req, "metadata", spooler, 1024, rewrite)
		require.NoError(t, err)
		require.Equal(t, w.Code, status)
		require.Equal(t, "private, no-cache", w.Header().Get("Cache-Control"))
		if etag == "" {
			require.Equal(t, "rewritten", w.Body.String())
			etag = w.Header().Get("ETag")
			require.NotEqual(t, `"upstream"`, etag)
		} else {
			require.Equal(t, http.StatusNotModified, w.Code)
			require.Empty(t, w.Body.String())
		}
		used, _ := budget.Usage()
		require.Zero(t, used)
		files, err := os.ReadDir(root)
		require.NoError(t, err)
		require.Empty(t, files)
	}
	req := httptest.NewRequest(http.MethodGet, "/metadata", nil)
	_, err = ForwardTransformed(req.Context(), client, origin, httptest.NewRecorder(), req, "metadata", spooler, 1024, func(_ io.Reader, out io.Writer, _ http.Header) error {
		_, err := io.Copy(out, strings.NewReader(strings.Repeat("x", 8192)))
		return err
	})
	require.Error(t, err)
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func TestUncachedHTTPSUsesHTTPConnectProxy(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/root/object?q=1", r.URL.RequestURI())
		_, _ = io.WriteString(w, "secure content")
	}))
	defer upstream.Close()
	target, err := url.Parse(upstream.URL + "/root/object?q=1")
	require.NoError(t, err)
	var tunnels atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != target.Host {
			http.Error(w, "unexpected proxy target", http.StatusBadRequest)
			return
		}
		remote, err := net.DialTimeout("tcp", target.Host, time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer remote.Close()
		local, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer local.Close()
		_ = local.SetDeadline(time.Now().Add(5 * time.Second))
		_ = remote.SetDeadline(time.Now().Add(5 * time.Second))
		tunnels.Add(1)
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffered.Flush()
		done := make(chan struct{})
		go func() {
			_, _ = io.Copy(remote, buffered)
			_ = remote.Close()
			close(done)
		}()
		_, _ = io.Copy(local, remote)
		_ = local.Close()
		<-done
	}))
	defer proxy.Close()
	client, err := NewClient("test", "file", &config.TransportConfig{Proxy: proxy.URL}, nil, nil)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	certificates := x509.NewCertPool()
	certificates.AddCert(upstream.Certificate())
	client.httpClient.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: certificates, MinVersion: tls.VersionTLS12}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/object", nil)
	status, err := ForwardReadTarget(req.Context(), client, target, w, req, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "secure content", w.Body.String())
	require.EqualValues(t, 1, tunnels.Load())
}

func TestUncachedDownloadCancellationAndVerification(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/wait" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
			close(canceled)
			return
		}
		_, _ = io.WriteString(w, "invalid body")
	}))
	defer upstream.Close()
	client, err := NewClient("test", "npm", nil, nil, nil)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	root := t.TempDir()
	budget := proxyruntime.NewSpoolBudget(8192)
	spooler := storeio.NewSpooler(root, 4096, budget)
	target, _ := url.Parse(upstream.URL + "/wait")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ForwardVerified(ctx, client, target, httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), nil, spooler, func(io.ReadSeeker) error { return nil })
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("request did not cancel")
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not cancel")
	}
	target.Path = "/invalid"
	w := httptest.NewRecorder()
	status, err := ForwardVerified(context.Background(), client, target, w, httptest.NewRequest(http.MethodGet, "/", nil), nil, spooler, func(io.ReadSeeker) error { return errors.New("checksum mismatch") })
	require.ErrorContains(t, err, "checksum mismatch")
	require.Zero(t, status)
	require.Empty(t, w.Body.String())
	used, _ := budget.Usage()
	require.Zero(t, used)
	files, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestUncachedTransformReturnsRewriteErrorAndReleasesSpools(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "source")
	}))
	defer upstream.Close()
	origin, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	client, err := NewClient("test", "npm", nil, nil, nil)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	root := t.TempDir()
	budget := proxyruntime.NewSpoolBudget(8192)
	spooler := storeio.NewSpooler(root, 4096, budget)

	status, err := ForwardTransformed(context.Background(), client, origin, httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), "metadata", spooler, 1024, func(_ io.Reader, output io.Writer, _ http.Header) error {
		if _, err := io.WriteString(output, "partial"); err != nil {
			return err
		}
		return errors.New("rewrite failed")
	})
	require.Zero(t, status)
	require.ErrorContains(t, err, "rewrite failed")
	used, _ := budget.Usage()
	require.Zero(t, used)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestUncachedUnverifiedDownloadStreamsWithoutSpooling(t *testing.T) {
	body := strings.Repeat("archive", 1000)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()
	client, err := NewClient("test", "pypi", nil, nil, nil)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	target, _ := url.Parse(upstream.URL)
	budget := proxyruntime.NewSpoolBudget(1)
	spooler := storeio.NewSpooler(t.TempDir(), 1, budget)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/archive", nil)
	status, err := ForwardVerified(req.Context(), client, target, w, req, nil, spooler, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, body, w.Body.String())
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func FuzzUncachedTransformLimits(f *testing.F) {
	f.Add([]byte("body"), uint8(2))
	f.Add([]byte{}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, copies uint8) {
		if len(data) > 1024 {
			t.Skip()
		}
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }))
		defer upstream.Close()
		client, err := NewClient("test", "npm", nil, nil, nil)
		require.NoError(t, err)
		defer client.CloseIdleConnections()
		origin, _ := url.Parse(upstream.URL)
		budget := proxyruntime.NewSpoolBudget(8192)
		root := t.TempDir()
		spooler := storeio.NewSpooler(root, 4096, budget)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = ForwardTransformed(ctx, client, origin, httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), "", spooler, 1024, func(source io.Reader, output io.Writer, _ http.Header) error {
			body, err := io.ReadAll(source)
			if err != nil {
				return err
			}
			for range int(copies % 8) {
				if _, err := output.Write(body); err != nil {
					return err
				}
			}
			return nil
		})
		used, _ := budget.Usage()
		require.Zero(t, used)
		files, err := os.ReadDir(root)
		require.NoError(t, err)
		require.Empty(t, files)
	})
}
