package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestMavenProxyServesMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/com/google/guava/guava/maven-metadata.xml":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<metadata><artifactId>guava</artifactId></metadata>`)
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()

	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"maven": mavenSpec(t, "maven", "/maven", upstream.URL)})
	defer closeRuntime(t, rt)

	body := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/maven/com/google/guava/guava/maven-metadata.xml")
	require.Contains(t, body, "<artifactId>guava</artifactId>")
}
