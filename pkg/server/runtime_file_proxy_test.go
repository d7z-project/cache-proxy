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
	fileproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
)

func TestFileProxyCachesImmutableObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var upstreamRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		_, _ = io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	spec := fileSpec(t, "files", "/files", upstream.URL)
	spec.Policy = mustPolicyJSON(t, &fileproxy.Policy{
		DefaultPolicy: config.PolicyImmutable,
		BusyPolicy:    config.BusyPolicyBypass,
		Rules:         []fileproxy.Rule{},
	})
	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"files": spec})
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)
	require.Equal(t, "hello", requestBody(t, handler, http.MethodGet, "/files/a.txt"))
	require.Equal(t, "hello", requestBody(t, handler, http.MethodGet, "/files/a.txt"))
	require.Equal(t, int64(1), upstreamRequests.Load())
}
