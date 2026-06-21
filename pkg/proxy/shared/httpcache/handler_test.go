package httpcache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestOCIBearerTokenSingleflightAndExpiredCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tokenRequests int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenRequests, 1)
		require.Equal(t, "registry.example", r.URL.Query().Get("service"))
		require.Equal(t, "repository:library/alpine:pull", r.URL.Query().Get("scope"))
		require.Equal(t, "Basic dXNlcjpwYXNz", r.Header.Get("Authorization"))
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"token":"token-` + strconv.Itoa(int(atomic.LoadInt32(&tokenRequests))) + `","expires_in":60}`))
	}))
	defer tokenServer.Close()

	handler := NewHandler("oci", RuntimeConfig{
		Mode: config.ModeOCI,
		OCIAuth: &OCIAuthConfig{
			Type:     "basic",
			Username: "user",
			Password: "pass",
		},
	}, nil, nil, NewStats(prometheus.NewRegistry()))
	challenge := ociChallenge{
		scheme: "Bearer",
		realm:  tokenServer.URL + "/token",
		params: map[string]string{"service": "registry.example", "scope": "repository:library/alpine:pull"},
	}

	const workers = 16
	var wg sync.WaitGroup
	results := make(chan string, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := handler.ociBearerToken(ctx, challenge)
			require.NoError(t, err)
			results <- token
		}()
	}
	wg.Wait()
	close(results)
	for token := range results {
		require.Equal(t, "token-1", token)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&tokenRequests))

	handler.ociTokenMu.Lock()
	handler.ociTokens["expired"] = ociToken{value: "old", expire: time.Now().Add(-time.Minute)}
	handler.ociTokenMu.Unlock()

	token, err := handler.ociBearerToken(ctx, challenge)
	require.NoError(t, err)
	require.Equal(t, "token-1", token)

	handler.ociTokenMu.Lock()
	_, exists := handler.ociTokens["expired"]
	handler.ociTokenMu.Unlock()
	require.False(t, exists)
}
