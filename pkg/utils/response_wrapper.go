package utils

import (
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const DefaultHTTPTimeout = 30 * time.Minute
const DefaultHeaderTimeout = 30 * time.Second
const DefaultIdleBodyTimeout = 5 * time.Minute

// DefaultUserAgent identifies cache-proxy to upstream services.
const DefaultUserAgent = "cache-proxy/1"

type HTTPClientWrapper struct {
	*http.Client
	UserAgent           string
	UserAgentConfigured bool
	IdleBodyTimeout     time.Duration
}

// RequestUserAgent returns the User-Agent for a foreground upstream request.
// The boolean reports whether an inbound browser value was selected.
func (client *HTTPClientWrapper) RequestUserAgent(req *http.Request) (string, bool) {
	if client.UserAgentConfigured || !IsBrowserRequest(req) {
		return client.UserAgent, false
	}
	return req.UserAgent(), true
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

func DefaultHTTPClientWrapper() *HTTPClientWrapper {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &HTTPClientWrapper{
			Client:          &http.Client{Timeout: DefaultHTTPTimeout},
			UserAgent:       DefaultUserAgent,
			IdleBodyTimeout: DefaultIdleBodyTimeout,
		}
	}
	transport := defaultTransport.Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	transport.IdleConnTimeout = 90 * time.Second
	transport.DialContext = (&net.Dialer{Timeout: 3 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = DefaultHeaderTimeout
	transport.MaxResponseHeaderBytes = 1 << 20
	return &HTTPClientWrapper{
		Client:          &http.Client{Transport: transport, Timeout: DefaultHTTPTimeout},
		UserAgent:       DefaultUserAgent,
		IdleBodyTimeout: DefaultIdleBodyTimeout,
	}
}

func (client *HTTPClientWrapper) WrapBody(body io.ReadCloser) io.ReadCloser {
	if client == nil {
		return body
	}
	return NewIdleTimeoutReadCloser(body, client.IdleBodyTimeout)
}

func ParseFetchedAt(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse(http.TimeFormat, value)
}
