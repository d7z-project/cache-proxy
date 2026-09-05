package deb

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func FuzzDebianIndexDescriptor(f *testing.F) {
	f.Add("flat/by-hash/SHA256/"+strings.Repeat("a", 64), "Packages.gz")
	f.Add("../by-hash/SHA512/"+strings.Repeat("b", 128), "../Packages")
	f.Add("flat/Packages.gz", "Packages.gz")
	f.Fuzz(func(t *testing.T, input, entryPath string) {
		if len(input) > 4096 || len(entryPath) > 1024 {
			t.Skip()
		}
		parsed, ok := parseByHashPath(input)
		if ok {
			require.Equal(t, input, parsed.FetchPath)
			require.True(t, len(parsed.SHA256) == 64 || len(parsed.SHA512) == 128)
			require.Empty(t, parsed.FallbackPath)
		}
		release := "Acquire-By-Hash: yes\nSHA256:\n " + strings.Repeat("a", 64) + " 7 " + entryPath + "\n"
		manifest, err := parseReleaseManifest(context.Background(), strings.NewReader(release))
		if err != nil {
			return
		}
		descriptor, found, err := manifest.resolveIndex("flat", input)
		if err != nil || !found {
			return
		}
		require.NotEmpty(t, descriptor.CachePaths)
		require.GreaterOrEqual(t, descriptor.Size, int64(0))
		matched := false
		for _, entry := range manifest.Entries {
			matched = matched || entry.Size == descriptor.Size && entry.SHA256 == descriptor.SHA256 && entry.SHA512 == descriptor.SHA512
		}
		require.True(t, matched)
	})
}

func FuzzDebianConcurrentIndexDownload(f *testing.F) {
	f.Add([]byte("index"), uint8(3), false)
	f.Add([]byte("other index"), uint8(2), true)
	f.Fuzz(func(t *testing.T, body []byte, count uint8, cancelWaiter bool) {
		if len(body) > 2048 {
			t.Skip()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		digest := sha256.Sum256(body)
		hashPath := fmt.Sprintf("/flat/by-hash/SHA256/%x", digest)
		release := fmt.Sprintf("Acquire-By-Hash: yes\nSHA256:\n %x %d Packages.gz\n", digest, len(body))
		started, unblock := make(chan struct{}), make(chan struct{})
		var startOnce, unblockOnce sync.Once
		var downloads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/flat/InRelease":
				_, _ = io.WriteString(w, release)
			case hashPath:
				downloads.Add(1)
				startOnce.Do(func() { close(started) })
				select {
				case <-unblock:
				case <-ctx.Done():
					return
				}
				_, _ = w.Write(body)
			default:
				http.NotFound(w, r)
			}
		}))
		defer server.Close()
		defer unblockOnce.Do(func() { close(unblock) })
		h := newDebianTestHandler(t, server.URL)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/flat/InRelease", nil).WithContext(ctx))
		_, err := h.metadata.Refresh(ctx, 10)
		require.NoError(t, err)
		workers := 2 + int(count%3)
		responses := make(chan *httptest.ResponseRecorder, workers)
		for i := 0; i < workers; i++ {
			request := httptest.NewRequest(http.MethodGet, hashPath, nil).WithContext(ctx)
			if i%2 == 0 {
				request = httptest.NewRequest(http.MethodGet, "/flat/Packages.gz", nil).WithContext(ctx)
			}
			if i == 1 && cancelWaiter {
				canceled, stop := context.WithCancel(ctx)
				stop()
				request = request.WithContext(canceled)
			}
			go func() { response := httptest.NewRecorder(); h.ServeHTTP(response, request); responses <- response }()
			if i == 0 {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		}
		unblockOnce.Do(func() { close(unblock) })
		for range workers {
			select {
			case response := <-responses:
				if response.Code == http.StatusOK {
					require.Equal(t, string(body), response.Body.String())
				} else {
					require.True(t, cancelWaiter)
					require.Equal(t, 502, response.Code)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		require.Equal(t, int32(1), downloads.Load())
	})
}
