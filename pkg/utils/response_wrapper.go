package utils

import (
	"io"
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
}

func (receiver *ResponseWrapper) FlushClose(req *http.Request, resp http.ResponseWriter) error {
	defer receiver.Body.Close()
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
	return receiver.Body.Close()
}

func OpenRequest(url string, allowError bool) (*ResponseWrapper, error) {
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("User-Agent", "curl/8.10.0")
	resp, err := http.DefaultClient.Do(request)
	if err != nil || resp == nil {
		return nil, err
	}
	if !allowError && resp.StatusCode != 200 {
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
