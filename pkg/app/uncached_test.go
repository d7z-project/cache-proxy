package app

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestUncachedInstancesForwardWithoutOpeningExistingStores(t *testing.T) {
	for mode := range modePlanners {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			var version atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Cache-Control", "public, max-age=86400")
				fmt.Fprintf(w, "version-%d", version.Load())
			}))
			t.Cleanup(upstream.Close)
			root := t.TempDir()
			storePath := filepath.Join(root, "instances", "test", mode, "blobs")
			require.NoError(t, os.MkdirAll(filepath.Dir(storePath), 0755))
			// A file in place of a store makes accidental store initialization fail.
			require.NoError(t, os.WriteFile(storePath, []byte("existing data"), 0600))
			for _, directory := range []string{"work", "forward/work"} {
				path := filepath.Join(filepath.Dir(storePath), directory, ".cache-proxy-tmp-download-old")
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
				require.NoError(t, os.WriteFile(path, []byte("old work"), 0600))
				old := time.Now().Add(-48 * time.Hour)
				require.NoError(t, os.Chtimes(path, old, old))
			}
			mount := "path: /test"
			if mode == config.ModeOCI {
				mount = "bind: 127.0.0.1:0"
			}
			doc, err := config.Decode(strings.NewReader(fmt.Sprintf("server:\n  backend: %s\nstorage:\n  orphan_policy: auto\ninstances:\n  - name: test\n    mode: %s\n    enabled: true\n    cache: false\n    %s\n    upstream: %s\n", root, mode, mount, upstream.URL)))
			require.NoError(t, err)
			application, err := Open(context.Background(), doc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, application.Close(context.Background())) })
			require.Empty(t, application.stores)
			require.Empty(t, application.scheduler.Snapshot())
			require.FileExists(t, filepath.Join(filepath.Dir(storePath), "work", ".cache-proxy-tmp-download-old"))
			require.NoFileExists(t, filepath.Join(filepath.Dir(storePath), "forward", "work", ".cache-proxy-tmp-download-old"))
			handler := application.entries["test"].Runtime
			target := "/unknown/resource?key=value"
			if mode == config.ModeGo {
				target = "/example.com/mod/@v/list?key=value"
			}
			if mode == config.ModeOCI {
				target = "/v2/example/image/manifests/latest?key=value"
			}
			for n := range 2 {
				version.Store(int32(n))
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
				require.Equal(t, http.StatusOK, recorder.Code)
				require.Equal(t, fmt.Sprintf("version-%d", n), recorder.Body.String())
				require.Equal(t, "BYPASS", recorder.Header().Get("X-Cache"))
			}
			before := requests.Load()
			var wg sync.WaitGroup
			codes := make(chan int, 8)
			for range 8 {
				wg.Go(func() {
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
					codes <- w.Code
				})
			}
			wg.Wait()
			close(codes)
			for code := range codes {
				require.Equal(t, http.StatusOK, code)
			}
			require.Equal(t, before+8, requests.Load())
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodPut, target, strings.NewReader("publish")))
			require.Equal(t, http.StatusMethodNotAllowed, w.Code)
			require.Equal(t, before+8, requests.Load())
			data, err := os.ReadFile(storePath)
			require.NoError(t, err)
			require.Equal(t, "existing data", string(data))
			upstream.Close()
			w = httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
			require.Equal(t, http.StatusBadGateway, w.Code)
		})
	}
}

func TestUncachedDebianFlatPathNormalizesCurrentDirectorySegment(t *testing.T) {
	upstreamPath := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		upstreamPath <- request.URL.RequestURI()
		_, _ = io.WriteString(w, "Packages")
	}))
	t.Cleanup(upstream.Close)
	doc, err := config.Decode(strings.NewReader(fmt.Sprintf("server:\n  backend: %s\ninstances:\n  - name: deb\n    mode: deb\n    enabled: true\n    cache: false\n    path: /deb\n    upstream: %s/root\n", t.TempDir(), upstream.URL)))
	require.NoError(t, err)
	application, err := Open(context.Background(), doc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, application.Close(context.Background())) })

	response := httptest.NewRecorder()
	application.entries["deb"].Runtime.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/flat/./Packages?arch=amd64", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "Packages", response.Body.String())
	require.Equal(t, "/root/flat/Packages?arch=amd64", <-upstreamPath)
}

func TestUncachedProtocolRewritingAndVerifiedDownloads(t *testing.T) {
	for _, mode := range []string{config.ModeNPM, config.ModePyPI, config.ModeCargo} {
		t.Run(mode, func(t *testing.T) {
			var revision, metadata, downloads atomic.Int32
			var corrupt atomic.Bool
			artifact := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Empty(t, r.Header.Get("Authorization"))
				require.Empty(t, r.Header.Get("Cookie"))
				downloads.Add(1)
				if corrupt.Load() {
					_, _ = io.WriteString(w, "corrupt")
					return
				}
				fmt.Fprintf(w, "archive-%d", revision.Load())
			}))
			defer artifact.Close()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer private", r.Header.Get("Authorization"))
				metadata.Add(1)
				digest := sha256.Sum256(fmt.Appendf(nil, "archive-%d", revision.Load()))
				checksum := hex.EncodeToString(digest[:])
				w.Header().Set("Cache-Control", "max-age=86400")
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case config.ModeNPM:
					integrity := sha512.Sum512(fmt.Appendf(nil, "archive-%d", revision.Load()))
					fmt.Fprintf(w, `{"name":"foo","versions":{"1.0.0":{"dist":{"tarball":%q,"integrity":"sha512-%s"}}}}`, artifact.URL+"/foo.tgz", base64.StdEncoding.EncodeToString(integrity[:]))
				case config.ModePyPI:
					fmt.Fprintf(w, `{"meta":{"api-version":"1.0"},"files":[{"filename":"foo.whl","url":%q,"hashes":{"sha256":%q}}]}`, artifact.URL+"/foo.whl", checksum)
				case config.ModeCargo:
					if r.URL.Path == "/config.json" {
						fmt.Fprintf(w, `{"dl":%q}`, artifact.URL+"/crates")
					} else {
						fmt.Fprintf(w, `{"name":"foo","vers":"1.0.0","cksum":%q}`+"\n", checksum)
					}
				}
			}))
			defer upstream.Close()
			doc, err := config.Decode(strings.NewReader(fmt.Sprintf("server:\n  backend: %s\ninstances:\n  - name: test\n    mode: %s\n    enabled: true\n    cache: false\n    path: /test\n    upstream: %s\n", t.TempDir(), mode, upstream.URL)))
			require.NoError(t, err)
			application, err := Open(context.Background(), doc)
			require.NoError(t, err)
			defer func() { require.NoError(t, application.Close(context.Background())) }()
			request := func(target, credential string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodGet, target, nil)
				req.Header.Set("Authorization", credential)
				req = proxyruntime.WithExternalBaseURL(req, "https://proxy.example/test")
				w := httptest.NewRecorder()
				application.entries["test"].Runtime.ServeHTTP(w, req)
				return w
			}
			metadataPath := "/foo"
			if mode == config.ModePyPI {
				metadataPath = "/simple/foo/"
			} else if mode == config.ModeCargo {
				metadataPath = "/config.json"
			}
			var downloadPath string
			for version := range 2 {
				revision.Store(int32(version))
				before := metadata.Load()
				w := request(metadataPath, "Bearer private")
				require.Equal(t, http.StatusOK, w.Code)
				require.Greater(t, metadata.Load(), before)
				var document struct {
					Versions map[string]struct {
						Dist struct {
							Tarball string `json:"tarball"`
						} `json:"dist"`
					} `json:"versions"`
					Files []struct {
						URL string `json:"url"`
					} `json:"files"`
					Download string `json:"dl"`
				}
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &document))
				switch mode {
				case config.ModeNPM:
					downloadPath = document.Versions["1.0.0"].Dist.Tarball
				case config.ModePyPI:
					require.Len(t, document.Files, 1)
					downloadPath = document.Files[0].URL
				case config.ModeCargo:
					downloadPath = strings.NewReplacer("{crate}", "foo", "{version}", "1.0.0").Replace(document.Download)
				}
				require.True(t, strings.HasPrefix(downloadPath, "https://proxy.example/test/-/"), downloadPath)
				downloadPath = strings.TrimPrefix(downloadPath, "https://proxy.example/test")
				before = downloads.Load()
				for range 2 {
					w = request(downloadPath, "Bearer private")
					require.Equal(t, http.StatusOK, w.Code)
					require.Equal(t, fmt.Sprintf("archive-%d", version), w.Body.String())
					require.Equal(t, "BYPASS", w.Header().Get("X-Cache"))
				}
				require.Equal(t, before+2, downloads.Load())
			}
			require.NoError(t, application.Close(context.Background()))
			application, err = Open(context.Background(), doc)
			require.NoError(t, err)
			w := request(downloadPath, "Bearer private")
			require.Equal(t, http.StatusOK, w.Code)
			if mode != config.ModeCargo {
				before := downloads.Load()
				require.Equal(t, http.StatusForbidden, request(downloadPath, "Bearer other").Code)
				require.Equal(t, before, downloads.Load())
			}
			corrupt.Store(true)
			require.Equal(t, http.StatusBadGateway, request(downloadPath, "Bearer private").Code)
		})
	}
}

func TestUncachedInstanceUsesConfiguredHTTPProxy(t *testing.T) {
	var requests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "http://unreachable.invalid/root/object?q=1", r.URL.String())
		_, _ = io.WriteString(w, "proxied")
	}))
	defer proxy.Close()
	doc, err := config.Decode(strings.NewReader(fmt.Sprintf("server:\n  backend: %s\ninstances:\n  - name: test\n    enabled: true\n    mode: rpm\n    path: /test\n    cache: false\n    upstream: http://unreachable.invalid/root\n    transport:\n      proxy: %s\n", t.TempDir(), proxy.URL)))
	require.NoError(t, err)
	application, err := Open(context.Background(), doc)
	require.NoError(t, err)
	defer application.Close(context.Background())
	w := httptest.NewRecorder()
	application.entries["test"].Runtime.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/object?q=1", nil))
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "proxied", w.Body.String())
	require.EqualValues(t, 1, requests.Load())
}

func TestAppCloseCancelsUncachedUpstreamRequest(t *testing.T) {
	upstreamStarted := make(chan struct{})
	upstreamCanceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(upstreamStarted)
		<-request.Context().Done()
		close(upstreamCanceled)
	}))
	defer upstream.Close()

	doc, err := config.Decode(strings.NewReader(fmt.Sprintf("server:\n  backend: %s\ninstances:\n  - name: test\n    enabled: true\n    mode: rpm\n    path: /test\n    cache: false\n    upstream: %s\n", t.TempDir(), upstream.URL)))
	require.NoError(t, err)
	application, err := Open(context.Background(), doc)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	application.mainServer = newHTTPServer(listener.Addr().String(), application.requests, application)
	application.ready.Store(true)
	serveDone := make(chan error, 1)
	go func() { serveDone <- application.mainServer.Serve(listener) }()

	requestDone := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + listener.Addr().String() + "/test/object")
		if err == nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-upstreamStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not reach upstream")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- application.Close(shutdownCtx) }()
	select {
	case <-upstreamCanceled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel upstream request")
	}
	require.NoError(t, <-closeDone)
	require.Error(t, <-requestDone)
	require.ErrorIs(t, <-serveDone, http.ErrServerClosed)
}
