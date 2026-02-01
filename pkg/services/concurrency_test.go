package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrentDownloads(t *testing.T) {
	var upstreamCalls int32
	content := []byte("concurrent-test-content")

	// 1. Setup Mock Upstream Server
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		// Simulate slow download
		time.Sleep(2000 * time.Millisecond)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer upstream.Close()

	// 2. Setup Worker and Target
	tmpDir, err := os.MkdirTemp("", "cache-proxy-concurrency-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	worker, err := NewWorker(tmpDir, time.Hour, time.Hour)
	assert.NoError(t, err)

	target := NewTarget("concurrency-target", upstream.URL)
	err = target.AddRule(".*", 10*time.Minute, 5*time.Minute)
	assert.NoError(t, err)

	err = worker.Bind("concurrency-target", target)
	assert.NoError(t, err)

	// 3. Launch multiple concurrent requests for the same path
	const numRequests = 100
	var wg sync.WaitGroup
	wg.Add(numRequests)

	results := make([][]byte, numRequests)
	errors := make([]error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			respWrapper, err := target.forward(context.Background(), "same-path", nil)
			if err != nil {
				errors[idx] = err
				return
			}

			// Use a dummy request/response to trigger FlushClose logic if needed,
			// or just read from the body directly.
			// Target.forward returns ResponseWrapper which has the Body.
			data, err := io.ReadAll(respWrapper.Body)
			_ = respWrapper.Close()

			results[idx] = data
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// 4. Assertions
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamCalls), "Should only call upstream ONCE for multiple concurrent requests")

	for i := 0; i < numRequests; i++ {
		assert.NoError(t, errors[i], "Request %d should not have error", i)
		assert.Equal(t, content, results[i], "Request %d should receive correct content", i)
	}

	// 5. Subsequent request should be a HIT
	respHit, err := target.forward(context.Background(), "same-path", nil)
	assert.NoError(t, err)
	assert.Equal(t, "HIT", respHit.Headers["X-Cache"])
	data, _ := io.ReadAll(respHit.Body)
	assert.Equal(t, content, data)
	_ = respHit.Close()
}
