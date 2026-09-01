package httpcache

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxRewriteBody = 50 << 20

func NPMResponseTransform(upstreams []string) func(*http.Request, Route, *utils.ResponseWrapper) *utils.ResponseWrapper {
	return func(req *http.Request, route Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
		if req.Method == http.MethodHead || response.Body == nil || !strings.HasPrefix(route.ObjectPath, "npm/metadata/") {
			return response
		}
		limited := &io.LimitedReader{R: response.Body, N: maxRewriteBody + 1}
		tempFile, err := os.CreateTemp("", "cache-proxy-npm-metadata-*")
		if err != nil {
			_ = response.Body.Close()
			return ErrorResponse(http.StatusBadGateway, err)
		}
		removeTemp := true
		defer func() {
			if removeTemp {
				_ = tempFile.Close()
				_ = os.Remove(tempFile.Name())
			}
		}()
		err = RewriteNPMMetadata(limited, tempFile, upstreams, publicBaseURL(req))
		_ = response.Body.Close()
		if err != nil || limited.N == 0 {
			return ErrorResponse(http.StatusBadGateway, errors.Join(err, errors.New("invalid npm metadata response")))
		}
		info, err := tempFile.Stat()
		if err != nil {
			return ErrorResponse(http.StatusBadGateway, err)
		}
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return ErrorResponse(http.StatusBadGateway, err)
		}
		response.Headers["Content-Length"] = strconv.FormatInt(info.Size(), 10)
		response.Body = &temporaryFileBody{File: tempFile, path: tempFile.Name()}
		removeTemp = false
		return response
	}
}

func CargoResponseTransform(req *http.Request, route Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
	if req.Method == http.MethodHead || response.Body == nil || route.UpstreamPath != "config.json" {
		return response
	}
	body, err := readRewriteBody(response.Body)
	if err != nil {
		return ErrorResponse(http.StatusBadGateway, err)
	}
	body, err = rewriteCargoConfig(req, body, route.AuthRequired)
	if err != nil {
		return ErrorResponse(http.StatusBadGateway, err)
	}
	response.Headers["Content-Type"] = "application/json"
	response.Headers["Content-Length"] = strconv.Itoa(len(body))
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response
}

func PyPIResponseTransform(upstreams []string) func(*http.Request, Route, *utils.ResponseWrapper) *utils.ResponseWrapper {
	return func(req *http.Request, route Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
		if req.Method == http.MethodHead || response.Body == nil || !strings.HasPrefix(route.ObjectPath, "pypi/simple/") {
			return response
		}
		body, err := readRewriteBody(response.Body)
		if err != nil {
			return ErrorResponse(http.StatusBadGateway, err)
		}
		body, response.Headers, err = rewritePyPISimple(req, upstreams, route, response.Headers, body)
		if err != nil {
			return ErrorResponse(http.StatusBadGateway, err)
		}
		response.Headers["Content-Length"] = strconv.Itoa(len(body))
		response.Body = io.NopCloser(bytes.NewReader(body))
		return response
	}
}

func readRewriteBody(body io.ReadCloser) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxRewriteBody+1))
	closeErr := body.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	if len(data) > maxRewriteBody {
		return nil, errors.New("response body too large to rewrite")
	}
	return data, nil
}

type temporaryFileBody struct {
	*os.File
	path string
}

func (b *temporaryFileBody) Close() error {
	err := b.File.Close()
	removeErr := os.Remove(b.path)
	return errors.Join(err, removeErr)
}
