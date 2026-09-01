package flatpak

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxDescriptorSize = 1 << 20

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeFlatpak }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	if !plan.Enabled() {
		return nil
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode: config.ModeFlatpak, ExpireAfter: plan.Retention(), MetadataTTL: plan.MetadataTTL(),
		Upstreams: plan.Upstreams(), Transport: plan.Transport(), UpstreamGate: plan.UpstreamGate(),
		VerifyFunc: verifyCacheObject, ResponseTransform: transformDescriptor,
	}, plan.Store(), resolver{}, plan.Stats())
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), handler)
}

type resolver struct{}

func (resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "" || cleanPath == "." {
		cleanPath = "summary"
	}
	if !httpcache.SafePath(cleanPath) {
		return httpcache.Route{}, errors.New("invalid flatpak request path")
	}
	class := httpcache.ClassMetadata
	objectPath := path.Join("flatpak", "metadata", httpcache.HashKey(cleanPath))
	if isObjectPath(cleanPath) || isDeltaPath(cleanPath) {
		class = httpcache.ClassContent
		objectPath = path.Join("flatpak", cleanPath)
	}
	return httpcache.Route{Class: class, ObjectPath: objectPath, UpstreamPath: cleanPath}, nil
}

func verifyCacheObject(_ *http.Request, route httpcache.Route, reader io.ReadSeeker) error {
	if !isObjectPath(route.UpstreamPath) {
		return nil
	}
	expected, extension, ok := objectDigestFromPath(route.UpstreamPath)
	if !ok {
		return fmt.Errorf("invalid flatpak object path %s", route.UpstreamPath)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind flatpak object %s: %w", route.UpstreamPath, err)
	}
	sum := sha256.New()
	if extension == ".filez" {
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("open flatpak object %s: %w", route.UpstreamPath, err)
		}
		_, copyErr := io.Copy(sum, compressed)
		closeErr := compressed.Close()
		if copyErr != nil || closeErr != nil {
			return errors.Join(copyErr, closeErr)
		}
	} else if _, err := io.Copy(sum, reader); err != nil {
		return fmt.Errorf("hash flatpak object %s: %w", route.UpstreamPath, err)
	}
	if actual := hex.EncodeToString(sum.Sum(nil)); actual != expected {
		return fmt.Errorf("flatpak object checksum mismatch for %s", route.UpstreamPath)
	}
	return nil
}

func transformDescriptor(req *http.Request, route httpcache.Route, response *utils.ResponseWrapper) *utils.ResponseWrapper {
	if !isDescriptorPath(route.UpstreamPath) || req.Method == http.MethodHead || response.StatusCode != http.StatusOK || response.Body == nil {
		return response
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDescriptorSize+1))
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || len(body) > maxDescriptorSize {
		return httpcache.ErrorResponse(http.StatusBadGateway, errors.Join(err, closeErr, errors.New("invalid flatpak descriptor")))
	}
	body = rewriteDescriptor(req, body)
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.Headers["Content-Length"] = strconv.Itoa(len(body))
	return response
}
