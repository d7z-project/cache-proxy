package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

const testModulePath = "example.com/cacheproxy/runtime"

func TestGoProxyServesPathMountedRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := newGoProxyUpstream(t)
	defer upstream.Close()

	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"gomod": goSpec(t, "gomod", "/go", upstream.URL)})
	defer closeRuntime(t, rt)

	body := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/go/"+testModulePath+"/@v/list")
	require.Equal(t, "v1.0.0", body)
}

func newGoProxyUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	zipContent := testModuleZip(t)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/" + testModulePath + "/@v/list":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, "v1.0.0\n")
		case "/" + testModulePath + "/@v/v1.0.0.info":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = fmt.Fprintf(w, `{"Version":"v1.0.0","Time":"2026-06-14T00:00:00Z"}`)
		case "/" + testModulePath + "/@v/v1.0.0.mod":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, "module "+testModulePath+"\n\ngo 1.25\n")
		case "/" + testModulePath + "/@v/v1.0.0.zip":
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(zipContent)
		default:
			http.NotFound(w, req)
		}
	}))
}

func testModuleZip(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+testModulePath+"\n\ngo 1.25\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "module.go"), []byte("package runtime\n"), 0o644))
	var buf bytes.Buffer
	require.NoError(t, modzip.CreateFromDir(&buf, module.Version{Path: testModulePath, Version: "v1.0.0"}, dir))
	return buf.Bytes()
}
