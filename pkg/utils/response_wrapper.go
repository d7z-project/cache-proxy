package utils

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
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
		conn, err := net.DialTimeout(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func DefaultHttpClientWrapper() *HttpClientWrapper {
	return &HttpClientWrapper{
		Client: &http.Client{
			Transport: &http.Transport{
				Proxy:       nil,
				DialContext: DefaultDialContext(3 * time.Second),
			},
		},
		UserAgent: "curl/8.10.0",
	}
}

func (receiver *ResponseWrapper) FlushClose(req *http.Request, resp http.ResponseWriter) error {
	defer receiver.Close()
	for key, value := range receiver.Headers {
		resp.Header().Add(key, value)
	}
	if closer, ok := receiver.Body.(io.ReadSeekCloser); ok {
		var lDate time.Time
		if date, ok := receiver.Headers["Last-Modified"]; ok {
			parse, err := time.Parse(http.TimeFormat, date)
			if err != nil {
				return err
			}
			lDate = parse
		}
		http.ServeContent(resp, req, "", lDate, closer)
		return nil
	} else {
		resp.WriteHeader(receiver.StatusCode)
		_, err := io.Copy(resp, receiver.Body)
		return err
	}
}

func (receiver *ResponseWrapper) Close() error {
	err := receiver.Body.Close()
	if receiver.Closes != nil {
		receiver.Closes()
	}
	return err
}

func OpenRequest(client *HttpClientWrapper, url string, errorAccept bool) (*ResponseWrapper, error) {
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("User-Agent", client.UserAgent)
	resp, err := client.Do(request)
	if err != nil && resp == nil {
		return nil, err
	}
	if !errorAccept && resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, errors.New("Request failed with status code " + strconv.Itoa(resp.StatusCode))
	}
	result := &ResponseWrapper{
		StatusCode: resp.StatusCode,
		Headers:    make(map[string]string),
		Body:       resp.Body,
	}
	copyHeader := func(k string) {
		if data, find := resp.Header[k]; find {
			result.Headers[k] = strings.Join(data, ",")
		}
	}
	copyHeader("Content-Type")
	copyHeader("Content-Length")
	copyHeader("Last-Modified")
	return result, nil
}
