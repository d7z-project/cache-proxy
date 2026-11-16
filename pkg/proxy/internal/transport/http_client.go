package transport

import (
	"io"
	"net"
	"net/http"
	"time"
)

const (
	defaultHTTPTimeout     = 30 * time.Minute
	defaultHeaderTimeout   = 30 * time.Second
	defaultIdleBodyTimeout = 5 * time.Minute

	// DefaultUserAgent identifies cache-proxy to upstream services.
	DefaultUserAgent = "cache-proxy"
)

// UpstreamHTTPClient carries the HTTP settings needed by the OCI transport,
// which performs its own authentication flow instead of using Client.
type UpstreamHTTPClient struct {
	*http.Client
	UserAgent       string
	IdleBodyTimeout time.Duration
}

func NewUpstreamHTTPClient() *UpstreamHTTPClient {
	client := &http.Client{Timeout: defaultHTTPTimeout}
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport := defaultTransport.Clone()
		transport.MaxIdleConns = 100
		transport.MaxIdleConnsPerHost = 100
		transport.IdleConnTimeout = 90 * time.Second
		transport.DialContext = (&net.Dialer{Timeout: 3 * time.Second}).DialContext
		transport.ResponseHeaderTimeout = defaultHeaderTimeout
		transport.MaxResponseHeaderBytes = maxResponseHeaderBytes
		client.Transport = transport
	}
	return &UpstreamHTTPClient{
		Client:          client,
		UserAgent:       DefaultUserAgent,
		IdleBodyTimeout: defaultIdleBodyTimeout,
	}
}

func (client *UpstreamHTTPClient) WrapBody(body io.ReadCloser) io.ReadCloser {
	return newIdleTimeoutReadCloser(body, client.IdleBodyTimeout)
}
