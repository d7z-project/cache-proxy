package server

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

func validateGlobalConfig(global *config.GlobalConfig, defaultMetricsPath string, defaultGCInterval time.Duration) error {
	if global == nil {
		return errors.New("global config is nil")
	}
	if global.Version == 0 {
		global.Version = 1
	}
	if strings.TrimSpace(global.Metrics.Path) == "" {
		global.Metrics.Path = defaultMetricsPath
	}
	if err := validateMetricsPath(global.Metrics.Path); err != nil {
		return err
	}
	if global.Storage.GC.Blob <= 0 {
		global.Storage.GC.Blob = config.Duration(defaultGCInterval)
	}
	return nil
}

func validateMetricsPath(metricsPath string) error {
	if !strings.HasPrefix(metricsPath, "/") || strings.Contains(metricsPath, "//") ||
		strings.HasSuffix(metricsPath, "/") || strings.ContainsAny(metricsPath, " \t\r\n{}") {
		return fmt.Errorf("invalid metrics path %q", metricsPath)
	}
	if metricsPath == "/" || metricsPath == "/-" || strings.HasPrefix(metricsPath, "/-/api/") {
		return fmt.Errorf("metrics path %q conflicts with reserved routes", metricsPath)
	}
	return nil
}

func validateInstances(instances map[string]config.InstanceSpec, registry *proxydriver.Registry, mainBind, metricsPath string) error {
	if instances == nil {
		return nil
	}
	binds := map[string]string{}
	if mainBind != "" {
		binds[mainBind] = "main"
	}
	paths := map[string]string{}
	for name, spec := range instances {
		spec.Name = name
		next, _, err := validateInstanceSpec(spec, registry)
		if err != nil {
			return fmt.Errorf("instance %s: %w", name, err)
		}
		if next.Route.Path != "" {
			listenPath := "/" + strings.Trim(next.Route.Path, "/")
			if listenPath == "/" {
				return fmt.Errorf("instance %s: listen path %q conflicts with web ui root", name, listenPath)
			}
			if listenPath == "/-" || strings.HasPrefix(listenPath, "/-/") {
				return fmt.Errorf("instance %s: listen path %q conflicts with admin api prefix", name, listenPath)
			}
			if listenPath == metricsPath {
				return fmt.Errorf("instance %s: listen path %q conflicts with metrics path", name, listenPath)
			}
			if owner := paths[listenPath]; owner != "" {
				return fmt.Errorf("listen path %s conflicts between %s and %s", listenPath, owner, name)
			}
			paths[listenPath] = name
		}
		if next.Route.Bind != "" {
			if owner := binds[next.Route.Bind]; owner != "" {
				return fmt.Errorf("listen bind %s conflicts between %s and %s", next.Route.Bind, owner, name)
			}
			binds[next.Route.Bind] = name
		}
		instances[name] = next
	}
	return nil
}

func validateInstanceSpec(spec config.InstanceSpec, registry *proxydriver.Registry) (config.InstanceSpec, *proxydriver.ResolvedSpec, error) {
	spec.Name = strings.TrimSpace(spec.Name)
	if spec.Name == "" || strings.ContainsAny(spec.Name, `/\`) || spec.Name == "." || spec.Name == ".." {
		return config.InstanceSpec{}, nil, fmt.Errorf("invalid instance name %q", spec.Name)
	}
	spec.Route.Path = normalizeRoutePath(spec.Route.Path)
	if (spec.Route.Path == "") == (spec.Route.Bind == "") {
		return config.InstanceSpec{}, nil, errors.New("must set exactly one route path or bind")
	}
	if spec.Route.Path != "" && (!strings.HasPrefix(spec.Route.Path, "/") || strings.Contains(spec.Route.Path, "//") || strings.HasSuffix(spec.Route.Path, "/")) {
		return config.InstanceSpec{}, nil, fmt.Errorf("invalid listen path %q", spec.Route.Path)
	}
	if spec.Route.Bind != "" {
		if err := validateBindAddress(spec.Route.Bind); err != nil {
			return config.InstanceSpec{}, nil, err
		}
	}
	spec.Route.PublicURL = strings.TrimSpace(spec.Route.PublicURL)
	if spec.Route.PublicURL != "" {
		if spec.Route.Bind == "" {
			return config.InstanceSpec{}, nil, errors.New("public url requires bind route")
		}
		parsed, err := url.Parse(spec.Route.PublicURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return config.InstanceSpec{}, nil, fmt.Errorf("invalid public url %q", spec.Route.PublicURL)
		}
		if parsed.RawQuery != "" || parsed.Fragment != "" {
			return config.InstanceSpec{}, nil, fmt.Errorf("invalid public url %q", spec.Route.PublicURL)
		}
		spec.Route.PublicURL = normalizePublicURL(parsed)
	}
	if len(spec.Source.Upstreams) == 0 {
		return config.InstanceSpec{}, nil, errors.New("at least one upstream is required")
	}
	for _, rawURL := range spec.Source.Upstreams {
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return config.InstanceSpec{}, nil, fmt.Errorf("invalid upstream %q", rawURL)
		}
	}
	if err := validateTransport(spec.Source.Transport); err != nil {
		return config.InstanceSpec{}, nil, err
	}
	if spec.Meta.ExpireAfter <= 0 {
		spec.Meta.ExpireAfter = config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter))
	}
	next, resolved, err := registry.Canonicalize(spec)
	if err != nil {
		return config.InstanceSpec{}, nil, err
	}
	return next, resolved, nil
}

func normalizeRoutePath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	trimmed = "/" + strings.Trim(trimmed, "/")
	if trimmed == "/" {
		return ""
	}
	return trimmed
}

func normalizePublicURL(parsed *url.URL) string {
	clone := *parsed
	clone.Path = normalizeURLPath(parsed.EscapedPath())
	clone.RawPath = ""
	return strings.TrimRight(clone.String(), "/")
}

func normalizeURLPath(value string) string {
	if value == "" || value == "/" {
		return ""
	}
	cleaned := "/" + strings.Trim(strings.TrimSpace(value), "/")
	if cleaned == "/" {
		return ""
	}
	return cleaned
}

func validateBindAddress(bind string) error {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return fmt.Errorf("invalid listen bind %q: must be host:port format", bind)
	}
	if port == "" {
		return fmt.Errorf("invalid listen bind %q: port is required", bind)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("invalid listen bind %q: port must be 1-65535", bind)
	}
	if host != "" && host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("invalid listen bind %q: invalid host %q", bind, host)
	}
	return nil
}

func validateTransport(transport *config.TransportConfig) error {
	if transport == nil {
		return nil
	}
	if transport.Proxy != "" {
		parsed, err := url.Parse(transport.Proxy)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "socks5") {
			return fmt.Errorf("invalid upstream proxy %q", transport.Proxy)
		}
	}
	if transport.Timeout < 0 {
		return errors.New("transport timeout must not be negative")
	}
	return nil
}

func sortedInstanceNames(instances map[string]config.InstanceSpec) []string {
	names := make([]string, 0, len(instances))
	for name := range instances {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type conflictError string

func (e conflictError) Error() string { return string(e) }
