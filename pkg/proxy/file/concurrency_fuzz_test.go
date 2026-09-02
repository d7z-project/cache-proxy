package file

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func FuzzFileParallelMiss(f *testing.F) {
	f.Add([]byte{}, uint8(1))
	f.Add([]byte("payload"), uint8(1))
	f.Add(bytes.Repeat([]byte("x"), 4096), uint8(16))
	f.Fuzz(func(t *testing.T, input []byte, workerInput uint8) {
		if len(input) > 32<<10 {
			t.Skip()
		}
		body := bytes.Clone(input)
		workerCount := int(workerInput%16) + 1
		var upstreamRequests atomic.Int32
		upstreamStarted := make(chan struct{})
		releaseUpstream := make(chan struct{})
		var startedOnce sync.Once
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			upstreamRequests.Add(1)
			startedOnce.Do(func() { close(upstreamStarted) })
			<-releaseUpstream
			w.Header().Set("Cache-Control", "max-age=3600")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body)
		}))
		defer upstream.Close()
		handler, _ := newTestHandler(t, upstream.URL, []Rule{{Match: "**", Policy: "http_cache"}})

		starts := make(chan struct{})
		type result struct {
			status int
			body   []byte
			cache  string
		}
		results := make(chan result, workerCount)
		var wait sync.WaitGroup
		for range workerCount {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-starts
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/parallel", nil))
				results <- result{status: response.Code, body: bytes.Clone(response.Body.Bytes()), cache: response.Header().Get("X-Cache")}
			}()
		}
		close(starts)
		select {
		case <-upstreamStarted:
		case <-time.After(2 * time.Second):
			close(releaseUpstream)
			t.Fatal("parallel miss did not reach upstream")
		}
		close(releaseUpstream)
		wait.Wait()
		close(results)
		cacheResults := make([]string, 0, workerCount)
		for result := range results {
			require.Equal(t, http.StatusOK, result.status)
			require.True(t, bytes.Equal(body, result.body))
			cacheResults = append(cacheResults, result.cache)
		}

		cached := httptest.NewRecorder()
		handler.ServeHTTP(cached, httptest.NewRequest(http.MethodGet, "/parallel", nil))
		require.Equal(t, http.StatusOK, cached.Code)
		require.True(t, bytes.Equal(body, cached.Body.Bytes()))
		require.Equal(t, int32(1), upstreamRequests.Load(), "initial cache results: %v, final cache result: %s", cacheResults, cached.Header().Get("X-Cache"))
	})
}
