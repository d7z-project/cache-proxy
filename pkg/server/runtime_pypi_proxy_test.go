package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestPyPIProxyRewritesSimpleHTMLAndServesFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var filesURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/simple/requests/":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, `<html><body><a href="`+filesURL+`">requests.whl</a></body></html>`)
		case "/packages/requests.whl":
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, "wheel-bytes")
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()
	filesURL = upstream.URL + "/packages/requests.whl"

	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"pypi": pypiSpec(t, "pypi", "/pypi", upstream.URL)})
	defer closeRuntime(t, rt)

	simpleBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/pypi/simple/requests/")
	require.Contains(t, simpleBody, "/pypi/files/")

	start := strings.Index(simpleBody, "/pypi/files/")
	require.NotEqual(t, -1, start)
	end := strings.Index(simpleBody[start:], `"`)
	require.NotEqual(t, -1, end)
	downloadPath := simpleBody[start : start+end]

	fileBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, downloadPath)
	require.Equal(t, "wheel-bytes", strings.TrimSpace(fileBody))
}

func TestPyPIProxyRewritesSimpleJSONAndNormalizesProjectName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var filesURL string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/simple/requests/":
			if strings.Contains(req.Header.Get("Accept"), "json") {
				w.Header().Set("Content-Type", "application/vnd.pypi.simple.v1+json")
				_, _ = io.WriteString(w, `{"files":[{"filename":"requests.whl","url":"`+filesURL+`"}]}`)
				return
			}
			http.NotFound(w, req)
		case "/packages/requests.whl":
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, "wheel-json-bytes")
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()
	filesURL = upstream.URL + "/packages/requests.whl"

	rt := newTestRuntime(t, ctx, map[string]config.InstanceSpec{"pypi": pypiSpec(t, "pypi", "/pypi", upstream.URL)})
	defer closeRuntime(t, rt)

	body := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, "/pypi/simple/Requests/json")
	require.Contains(t, body, `"http://example.com/pypi/files/`)

	start := strings.Index(body, "/pypi/files/")
	require.NotEqual(t, -1, start)
	end := strings.Index(body[start:], `"`)
	require.NotEqual(t, -1, end)
	downloadPath := body[start : start+end]

	fileBody := requestBody(t, http.HandlerFunc(rt.serveMain), http.MethodGet, downloadPath)
	require.Equal(t, "wheel-json-bytes", strings.TrimSpace(fileBody))
}
