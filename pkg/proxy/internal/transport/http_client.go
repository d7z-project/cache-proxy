package transport

import (
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	defaultHTTPTimeout     = 30 * time.Minute
	defaultHeaderTimeout   = 30 * time.Second
	defaultIdleBodyTimeout = 5 * time.Minute

	// DefaultUserAgent identifies cache-proxy to upstream services.
	DefaultUserAgent = "cache-proxy/1"
)

// UpstreamHTTPClient carries the HTTP settings needed by the OCI transport,
// which performs its own authentication flow instead of using Client.
type UpstreamHTTPClient struct {
	*http.Client
	UserAgent           string
	UserAgentConfigured bool
	IdleBodyTimeout     time.Duration
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

// RequestUserAgent returns the User-Agent for a foreground upstream request.
// The boolean reports whether an inbound browser value was selected.
func (client *UpstreamHTTPClient) RequestUserAgent(req *http.Request) (string, bool) {
	if client.UserAgentConfigured || !IsBrowserRequest(req) {
		return client.UserAgent, false
	}
	return req.UserAgent(), true
}

func (client *UpstreamHTTPClient) WrapBody(body io.ReadCloser) io.ReadCloser {
	return newIdleTimeoutReadCloser(body, client.IdleBodyTimeout)
}

func IsBrowserRequest(req *http.Request) bool {
	if req == nil || req.UserAgent() == "" {
		return false
	}
	for _, name := range []string{"Sec-Fetch-Mode", "Sec-Fetch-Site", "Sec-Fetch-Dest", "Sec-Fetch-User"} {
		if req.Header.Get(name) != "" {
			return true
		}
	}

	userAgent := strings.ToLower(req.UserAgent())
	if !strings.Contains(userAgent, "mozilla/5.0") {
		return false
	}
	for _, product := range []string{
		"firefox/", "fxios/", "chrome/", "crios/", "chromium/", "headlesschrome/",
		"safari/", "edg/", "edga/", "edgios/", "opr/", "opera/",
	} {
		if strings.Contains(userAgent, product) {
			return true
		}
	}
	return false
}

func VariesByUserAgent(values ...string) bool {
	for _, value := range values {
		for field := range strings.SplitSeq(value, ",") {
			field = strings.TrimSpace(field)
			if field == "*" || strings.EqualFold(field, "User-Agent") {
				return true
			}
		}
	}
	return false
}
