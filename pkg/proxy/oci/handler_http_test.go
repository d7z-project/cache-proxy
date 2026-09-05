package oci

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestOCIUpstreamBodyLifecycle(t *testing.T) {
	for _, test := range []struct {
		name    string
		delay   time.Duration
		bodyErr error
		wantErr error
	}{
		{name: "continuous progress", delay: time.Second},
		{name: "stalled body", delay: 3 * time.Second, wantErr: transport.ErrIdleBodyTimeout},
		{name: "truncated body", delay: time.Second, bodyErr: io.ErrUnexpectedEOF, wantErr: io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const content = "abcdefghijkl"
				h := newHandler("oci", Block{Upstream: "https://registry.example"}, config.Expiration(time.Hour), nil, metrics.NewStats(prometheus.NewRegistry()), nil)
				defer func() { require.NoError(t, h.Stop(context.Background())) }()
				h.client.IdleBodyTimeout = 2 * time.Second
				var writers sync.WaitGroup
				defer writers.Wait()
				h.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
					reader, writer := io.Pipe()
					writers.Go(func() {
						defer func() { _ = writer.CloseWithError(test.bodyErr) }()
						for _, value := range []byte(content) {
							time.Sleep(test.delay)
							if _, err := writer.Write([]byte{value}); err != nil {
								return
							}
						}
					})
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: reader, Request: request}, nil
				})
				response, err := h.readUpstream(context.Background(), context.Background(), http.MethodGet, "v2/demo/blobs/digest", "", "", nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, response.Body.Close()) }()
				body, err := io.ReadAll(response.Body)
				if test.wantErr != nil {
					require.ErrorIs(t, err, test.wantErr)
				} else {
					require.NoError(t, err)
					require.Equal(t, content, string(body))
				}
			})
		})
	}
}

func TestOCITokenBodyIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHandler("oci", Block{Upstream: "https://registry.example"}, config.Expiration(time.Hour), nil, metrics.NewStats(prometheus.NewRegistry()), nil)
		defer func() { require.NoError(t, h.Stop(context.Background())) }()
		h.client.IdleBodyTimeout = 2 * time.Second
		var writers sync.WaitGroup
		defer writers.Wait()
		h.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			reader, writer := io.Pipe()
			writers.Go(func() {
				defer func() { _ = writer.Close() }()
				time.Sleep(3 * time.Second)
				_, _ = io.WriteString(writer, `{"token":"pull-token"}`)
			})
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: reader, Request: request}, nil
		})
		_, _, err := h.fetchBearerToken(context.Background(), ociChallenge{realm: "https://registry.example/token"}, time.Now())
		require.ErrorIs(t, err, transport.ErrIdleBodyTimeout)
	})
}
