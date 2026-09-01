package pypi

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type Options struct {
	AllowedFileHosts []string `json:"allowedFileHosts,omitempty" yaml:"allowed_file_hosts,omitempty"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModePyPI }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var options Options
	if err := plan.Decode(&options); err != nil {
		return err
	}
	seenHosts := make(map[string]struct{}, len(options.AllowedFileHosts))
	for index, rawHost := range options.AllowedFileHosts {
		host := strings.TrimSpace(rawHost)
		parsed, err := url.Parse("//" + host)
		if err != nil || host == "" || strings.Contains(host, "://") || parsed.Host == "" ||
			parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("instance %s: invalid pypi allowed_file_hosts entry %q", plan.Name(), rawHost)
		}
		host = strings.ToLower(parsed.Host)
		if _, exists := seenHosts[host]; exists {
			return fmt.Errorf("instance %s: duplicate pypi allowed_file_hosts entry %q", plan.Name(), rawHost)
		}
		seenHosts[host] = struct{}{}
		options.AllowedFileHosts[index] = host
	}
	if !plan.Enabled() {
		return nil
	}
	upstream := strings.TrimSpace(plan.Upstreams()[0])
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:               config.ModePyPI,
		ExpireAfter:        plan.Retention(),
		MetadataTTL:        plan.MetadataTTL(),
		Upstreams:          []string{upstream},
		Transport:          plan.Transport(),
		AllowedTargetHosts: options.AllowedFileHosts,
		UpstreamGate:       plan.UpstreamGate(),
		ResponseTransform:  httpcache.PyPIResponseTransform([]string{upstream}),
	}, plan.Store(), resolver{}, plan.Stats())
	return plan.BindHTTPPath(plan.Path(), plan.Retention(), handler)
}

type resolver struct{}

func (resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	return routeForPath(strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/"))
}

func routeForPath(lookupPath string) (httpcache.Route, error) {
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "simple/"
	}
	if !httpcache.SafePath(lookupPath) {
		return httpcache.Route{}, errors.New("invalid pypi request path")
	}
	switch {
	case lookupPath == "simple" || lookupPath == "simple/":
		return httpcache.Route{Class: httpcache.ClassMetadata, ObjectPath: "pypi/simple/root.html", UpstreamPath: "simple/"}, nil
	case strings.HasPrefix(lookupPath, "simple/"):
		trimmed := strings.TrimPrefix(lookupPath, "simple/")
		if strings.HasSuffix(trimmed, "/json") {
			name := normalizeProjectName(strings.TrimSuffix(trimmed, "/json"))
			return httpcache.Route{
				Class:          httpcache.ClassMetadata,
				ObjectPath:     "pypi/simple/" + name + ".json",
				UpstreamPath:   "simple/" + name + "/",
				RequestHeaders: map[string]string{"Accept": "application/vnd.pypi.simple.v1+json"},
			}, nil
		}
		name := normalizeProjectName(strings.TrimSuffix(trimmed, "/"))
		return httpcache.Route{Class: httpcache.ClassMetadata, ObjectPath: "pypi/simple/" + name + ".html", UpstreamPath: "simple/" + name + "/"}, nil
	case strings.HasPrefix(lookupPath, "files/"):
		sourceURL, err := decodeSourceURL(path.Base(lookupPath))
		if err != nil {
			return httpcache.Route{}, err
		}
		return pypiFileRoute(lookupPath, sourceURL), nil
	default:
		return pypiFileRoute(lookupPath, lookupPath), nil
	}
}

func pypiFileRoute(lookupPath, rawURL string) httpcache.Route {
	objectPath := "pypi/files/" + path.Base(lookupPath)
	if !strings.HasPrefix(lookupPath, "files/") {
		objectPath = "pypi/files/" + encodeSourceURL(rawURL)
	}
	route := httpcache.Route{Class: httpcache.ClassContent, ObjectPath: objectPath}
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		route.TargetURL = rawURL
	} else {
		route.UpstreamPath = lookupPath
	}
	return route
}

func normalizeProjectName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	return strings.ReplaceAll(name, ".", "-")
}

func encodeSourceURL(raw string) string {
	return hex.EncodeToString([]byte(raw))
}

func decodeSourceURL(value string) (string, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
