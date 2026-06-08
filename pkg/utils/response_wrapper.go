package utils

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"
)

type ResponseWrapper struct {
	StatusCode int
	Headers    map[string]string
	Body       io.ReadCloser
	Closes     func()
}

type HttpClientWrapper struct {
	*http.Client
	UserAgent string
}

func DefaultDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, timeout)
	}
}

func DefaultHttpClientWrapper() *HttpClientWrapper {
	return &HttpClientWrapper{
		Client: &http.Client{
			Transport: &http.Transport{
				DialContext: DefaultDialContext(3 * time.Second),
			},
			Timeout: 30 * time.Second,
		},
		UserAgent: "curl/8.10.0",
	}
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
	var err error
	if receiver.Body != nil {
		err = receiver.Body.Close()
	}
	if receiver.Closes != nil {
		receiver.Closes()
		receiver.Closes = nil
	}
	return err
}
