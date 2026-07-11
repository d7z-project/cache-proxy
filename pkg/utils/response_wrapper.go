package utils

import (
	"context"
	"io"
	"net"
	"net/http"
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

type HttpClientWrapper struct {
	*http.Client
	UserAgent       string
	IdleBodyTimeout time.Duration
}

func DefaultDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, network, addr)
	}
}

func DefaultHttpClientWrapper() *HttpClientWrapper {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &HttpClientWrapper{
			Client:          &http.Client{Timeout: DefaultHTTPTimeout},
			UserAgent:       DefaultUserAgent,
			IdleBodyTimeout: DefaultIdleBodyTimeout,
		}
	}
	transport := defaultTransport.Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	transport.MaxConnsPerHost = 200
	transport.IdleConnTimeout = 90 * time.Second
	transport.DialContext = DefaultDialContext(3 * time.Second)
	transport.ResponseHeaderTimeout = DefaultHeaderTimeout
	return &HttpClientWrapper{
		Client:          &http.Client{Transport: transport, Timeout: DefaultHTTPTimeout},
		UserAgent:       DefaultUserAgent,
		IdleBodyTimeout: DefaultIdleBodyTimeout,
	}
}

func (receiver *HttpClientWrapper) WrapBody(body io.ReadCloser) io.ReadCloser {
	if receiver == nil {
		return body
	}
	return NewIdleTimeoutReadCloser(body, receiver.IdleBodyTimeout)
}

func (receiver *ResponseWrapper) FlushClose(req *http.Request, resp http.ResponseWriter) error {
	defer receiver.Close()
	for key, value := range receiver.Headers {
		resp.Header().Set(key, value)
	}
	if seeker, ok := receiver.Body.(io.ReadSeekCloser); ok {
		lastModified := time.Time{}
		if value := receiver.Headers["Last-Modified"]; value != "" {
			if parsed, err := time.Parse(http.TimeFormat, value); err == nil {
				lastModified = parsed
			}
		}
		http.ServeContent(resp, req, "", lastModified, seeker)
		return nil
	}
	resp.WriteHeader(receiver.StatusCode)
	if req.Method == http.MethodHead {
		return nil
	}
	_, err := io.Copy(resp, receiver.Body)
	return err
}

func (receiver *ResponseWrapper) Close() error {
	if receiver.Body != nil {
		return receiver.Body.Close()
	}
	return nil
}

func ParseFetchedAt(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse(http.TimeFormat, value)
}
