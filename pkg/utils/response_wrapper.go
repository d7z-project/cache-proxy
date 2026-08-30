package utils

import (
	"context"
	"errors"
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

type ResponseWrapper struct {
	StatusCode int
	Headers    map[string]string
	Body       io.ReadCloser
}

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

func DefaultDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, network, addr)
	}
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
	transport.DialContext = DefaultDialContext(3 * time.Second)
	transport.ResponseHeaderTimeout = DefaultHeaderTimeout
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

func (response *ResponseWrapper) FlushClose(req *http.Request, resp http.ResponseWriter) error {
	defer func() { _ = response.Close() }()
	for key, value := range response.Headers {
		resp.Header().Set(key, value)
	}
	if seeker, ok := response.Body.(io.ReadSeekCloser); ok {
		lastModified := time.Time{}
		if value := response.Headers["Last-Modified"]; value != "" {
			if parsed, err := time.Parse(http.TimeFormat, value); err == nil {
				lastModified = parsed
			}
		}
		http.ServeContent(resp, req, "", lastModified, seeker)
		return nil
	}
	resp.WriteHeader(response.StatusCode)
	if req.Method == http.MethodHead {
		return nil
	}
	buffer := make([]byte, 32<<10)
	flushed := false
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if _, err := resp.Write(buffer[:n]); err != nil {
				return err
			}
			if !flushed {
				flushed = true
				if err := http.NewResponseController(resp).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
					return err
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func (response *ResponseWrapper) Close() error {
	if response.Body != nil {
		return response.Body.Close()
	}
	return nil
}

func ParseFetchedAt(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse(http.TimeFormat, value)
}
