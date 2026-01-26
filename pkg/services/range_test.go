package services

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRangeRequestScenarios(t *testing.T) {
	// 1. Setup Mock Upstream Server
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", "bytes 0-10/100")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("partial con")) // 11 bytes
		} else {
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			// Write 100 bytes of dummy data
			data := make([]byte, 100)
			for i := range data {
				data[i] = 'x'
			}
			_, _ = w.Write(data)
		}
	}))
	defer upstream.Close()

	// 2. Setup Worker and Target
	tmpDir, err := os.MkdirTemp("", "cache-proxy-range-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	worker, err := NewWorker(tmpDir, time.Hour, time.Hour)
	assert.NoError(t, err)

	target := NewTarget("range-target", upstream.URL)
	// Cache for 2 hours, refresh every 1 hour
	err = target.AddRule(".*", 2*time.Hour, time.Hour)
	assert.NoError(t, err)

	err = worker.Bind("range-target", target)
	assert.NoError(t, err)

	// Scenario 1: Cache Miss + Range Request -> Direct Proxy (Bypass)
	t.Run("CacheMiss_RangeRequest_Bypass", func(t *testing.T) {
		headers := map[string]string{
			"Range": "bytes=0-10",
		}

		respWrapper, err := target.forward(context.Background(), "file-1", headers)
		assert.NoError(t, err)
		assert.NotNil(t, respWrapper)
		defer respWrapper.Close()

		// Should be 206 Partial Content from upstream
		assert.Equal(t, http.StatusPartialContent, respWrapper.StatusCode)

		// Verify headers are passed through
		_, hasContentRange := respWrapper.Headers["Content-Range"]
		assert.True(t, hasContentRange, "Content-Range header should be present")
	})

	// Scenario 2: Cache Miss + Normal Request -> Download and Cache
	t.Run("CacheMiss_NormalRequest_DownloadAndCache", func(t *testing.T) {
		respWrapper, err := target.forward(context.Background(), "file-2", nil)
		assert.NoError(t, err)
		assert.NotNil(t, respWrapper)

		assert.Equal(t, http.StatusOK, respWrapper.StatusCode)
		assert.Equal(t, "WAIT", respWrapper.Headers["X-Cache"]) // First fetch is WAIT (transparent proxy + cache write)

		// Read full body to ensure it's written to cache
		body, err := io.ReadAll(respWrapper.Body)
		assert.NoError(t, err)
		assert.Equal(t, 100, len(body))

		_ = respWrapper.Close()
	})

	// Scenario 3: Cache Hit + Range Request -> Served from Cache
	// Note: We reuse "file-2" which was cached in Scenario 2.
	t.Run("CacheHit_RangeRequest_ServedFromCache", func(t *testing.T) {
		// Verify it's a hit first with a normal request (optional, but good for sanity)
		respHit, err := target.forward(context.Background(), "file-2", nil)
		assert.NoError(t, err)
		assert.Equal(t, "HIT", respHit.Headers["X-Cache"])
		_ = respHit.Close()

		// Now send a Range request
		headers := map[string]string{
			"Range": "bytes=0-9",
		}

		// Note: target.forward returns the internal object.
		// The handling of "Range" for cached objects happens in FlushClose -> http.ServeContent.
		// So target.forward itself might still return 200 OK status code in the struct,
		// but the body implements ReadSeeker.
		respWrapper, err := target.forward(context.Background(), "file-2", headers)
		assert.NoError(t, err)
		defer respWrapper.Close()

		// Simulate FlushClose behavior to verify Range handling
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/file-2", nil)
		req.Header.Set("Range", "bytes=0-9")

		err = respWrapper.FlushClose(req, recorder)
		assert.NoError(t, err)

		// http.ServeContent should handle the Range header and return 206
		assert.Equal(t, http.StatusPartialContent, recorder.Code)
		assert.Equal(t, "bytes 0-9/100", recorder.Header().Get("Content-Range"))
		assert.Equal(t, "10", recorder.Header().Get("Content-Length"))
	})
}
