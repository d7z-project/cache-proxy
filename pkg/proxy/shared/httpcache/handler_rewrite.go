package httpcache

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	stdpath "path"
	"regexp"
	"strings"
)

const maxCargoDownloadTemplateSize = 8 << 10

var cargoTemplatePlaceholders = []string{
	"{crate}", "{version}", "{prefix}", "{lowerprefix}", "{sha256-checksum}",
}

func rewriteCargoConfig(req *http.Request, data []byte, authRequired bool) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, errors.New("cargo config must be a JSON object")
	}
	var downloadTemplate string
	if raw, ok := payload["dl"]; ok {
		if err := json.Unmarshal(raw, &downloadTemplate); err != nil {
			return nil, err
		}
	}
	if token, needsChecksum, err := EncodeCargoDownloadTemplate(downloadTemplate); err == nil {
		downloadURL := joinBaseAndPath(externalBaseURL(req), "/api/v1/crates/{crate}/{version}/download/"+token)
		if needsChecksum {
			downloadURL += "/{sha256-checksum}"
		}
		payload["dl"], _ = json.Marshal(downloadURL)
	}
	if authRequired {
		payload["auth-required"] = json.RawMessage("true")
	}
	return json.Marshal(payload)
}

// EncodeCargoDownloadTemplate validates and encodes an upstream Cargo download
// template for transport in a proxy-owned URL path.
func EncodeCargoDownloadTemplate(raw string) (string, bool, error) {
	template, needsChecksum, err := validateCargoDownloadTemplate(raw)
	if err != nil {
		return "", false, err
	}
	return base64.RawURLEncoding.EncodeToString([]byte(template)), needsChecksum, nil
}

// DecodeCargoDownloadTemplate decodes a proxy route token and repeats the
// validation performed before the token was advertised.
func DecodeCargoDownloadTemplate(token string) (string, bool, error) {
	if token == "" || len(token) > base64.RawURLEncoding.EncodedLen(maxCargoDownloadTemplateSize) {
		return "", false, errors.New("invalid cargo download template token")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", false, errors.New("invalid cargo download template token")
	}
	return validateCargoDownloadTemplate(string(decoded))
}

func validateCargoDownloadTemplate(raw string) (string, bool, error) {
	template := strings.TrimSpace(raw)
	if template == "" || len(template) > maxCargoDownloadTemplateSize {
		return "", false, errors.New("invalid cargo download template")
	}
	hasPlaceholder := false
	for _, placeholder := range cargoTemplatePlaceholders {
		hasPlaceholder = hasPlaceholder || strings.Contains(template, placeholder)
	}
	if !hasPlaceholder {
		template = strings.TrimRight(template, "/") + "/{crate}/{version}/download"
	}
	parsed, err := url.Parse(template)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.Fragment != "" || strings.ContainsAny(parsed.Host, "{}") {
		return "", false, errors.New("invalid cargo download template")
	}
	resolved := template
	for _, placeholder := range cargoTemplatePlaceholders {
		resolved = strings.ReplaceAll(resolved, placeholder, "x")
	}
	if strings.ContainsAny(resolved, "{}") {
		return "", false, errors.New("unsupported cargo download template placeholder")
	}
	return template, strings.Contains(template, "{sha256-checksum}"), nil
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
