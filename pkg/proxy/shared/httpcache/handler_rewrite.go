package httpcache

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	stdpath "path"
	"regexp"
	"strings"
)

type CargoConfig struct {
	DL           string `json:"dl"`
	AuthRequired bool   `json:"auth-required,omitempty"`
}

func rewriteCargoConfig(req *http.Request, data []byte, authRequired bool) ([]byte, error) {
	cfg := CargoConfig{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	cfg.DL = joinBaseAndPath(externalBaseURL(req), "/api/v1/crates/{crate}/{version}/download")
	if authRequired {
		cfg.AuthRequired = true
	}
	return json.Marshal(cfg)
}

func rewritePyPISimple(req *http.Request, upstreams []string, route Route, headers map[string]string, data []byte) ([]byte, map[string]string, error) {
	upstreamPageURL := route.TargetURL
	if upstreamPageURL == "" && len(upstreams) > 0 {
		upstreamPageURL = strings.TrimRight(upstreams[0], "/") + "/" + strings.TrimPrefix(route.UpstreamPath, "/")
	}
	contentType := headers["Content-Type"]
	if strings.Contains(contentType, "json") {
		next, err := rewritePyPISimpleJSON(req, upstreamPageURL, data)
		if err != nil {
			return nil, nil, err
		}
		return next, map[string]string{
			"Content-Type": "application/vnd.pypi.simple.v1+json",
		}, nil
	}
	return rewritePyPISimpleHTML(proxyBaseURL(req), upstreamPageURL, data), map[string]string{
		"Content-Type": "text/html; charset=utf-8",
	}, nil
}

func rewritePyPISimpleJSON(req *http.Request, upstreamPageURL string, data []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	files, ok := payload["files"].([]any)
	if !ok {
		return data, nil
	}
	base := proxyBaseURL(req)
	for _, item := range files {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if rawURL, ok := obj["url"].(string); ok && rawURL != "" {
			obj["url"] = joinBaseAndPath(base, "/files/"+hex.EncodeToString([]byte(resolveURL(upstreamPageURL, rawURL))))
		}
	}
	return json.Marshal(payload)
}

var hrefPattern = regexp.MustCompile(`href="([^"]+)"`)

func rewritePyPISimpleHTML(base, upstreamPageURL string, data []byte) []byte {
	return hrefPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		parts := hrefPattern.FindSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		rawURL := string(parts[1])
		resolved := resolveURL(upstreamPageURL, rawURL)
		return bytes.Replace(match, parts[1], []byte(joinBaseAndPath(base, "/files/"+hex.EncodeToString([]byte(resolved)))), 1)
	})
}

func resolveURL(base, raw string) string {
	if base == "" {
		return raw
	}
	baseURL, err := url.Parse(base)
	if err != nil || baseURL == nil {
		return raw
	}
	ref, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return baseURL.ResolveReference(ref).String()
}

func BaseURL(req *http.Request) string {
	scheme := req.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if req.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := req.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = req.Host
	}
	return scheme + "://" + host
}

func externalBaseURL(req *http.Request) string {
	prefix := normalizedProxyPrefix(req.Header.Get("X-Cache-Proxy-Prefix"))
	if prefix == "" {
		prefix = normalizedProxyPrefix(strings.TrimSuffix(req.URL.Path, "/config.json"))
	}
	return BaseURL(req) + prefix
}

func proxyBaseURL(req *http.Request) string {
	prefix := normalizedProxyPrefix(req.Header.Get("X-Cache-Proxy-Prefix"))
	if prefix == "" {
		p := req.URL.Path
		if idx := strings.Index(p, "/simple/"); idx >= 0 {
			p = p[:idx]
		}
		prefix = normalizedProxyPrefix(p)
	}
	return BaseURL(req) + prefix
}

func normalizedProxyPrefix(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	cleaned := stdpath.Clean("/" + strings.TrimPrefix(trimmed, "/"))
	if cleaned == "." || cleaned == "/" {
		return ""
	}
	return cleaned
}

func joinBaseAndPath(base, suffix string) string {
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(suffix, "/")
}
