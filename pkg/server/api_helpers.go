package server

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	stdpath "path"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func listInstanceSummaries(req *http.Request, instances map[string]config.InstanceSpec) []config.InstanceSummary {
	result := make([]config.InstanceSummary, 0, len(instances))
	for _, name := range sortedInstanceNames(instances) {
		result = append(result, instanceSummary(req, instances[name]))
	}
	return result
}

func instanceSummary(req *http.Request, spec config.InstanceSpec) config.InstanceSummary {
	summary := spec.Summary()
	switch {
	case strings.TrimSpace(spec.Route.PublicURL) != "":
		summary.EntryKind = "public_url"
		summary.EntryLabel = "公开地址"
		summary.EntryURL = strings.TrimSpace(spec.Route.PublicURL)
	case spec.Route.Path != "":
		summary.EntryKind = "path"
		summary.EntryLabel = "路径入口"
		summary.EntryURL = joinBaseAndPath(requestBaseURL(req), spec.Route.Path)
	case spec.Route.Bind != "":
		summary.EntryKind = "bind"
		summary.EntryLabel = "独立端口"
		summary.EntryURL = bindEntryURL(req, spec.Route.Bind)
	}
	return summary
}

func requestBaseURL(req *http.Request) string {
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

func joinBaseAndPath(base, rawPath string) string {
	cleanPath := normalizeDisplayPath(rawPath)
	if cleanPath == "" {
		return strings.TrimRight(base, "/")
	}
	return strings.TrimRight(base, "/") + cleanPath
}

func normalizeDisplayPath(rawPath string) string {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" {
		return ""
	}
	cleaned := stdpath.Clean("/" + strings.TrimPrefix(trimmed, "/"))
	if cleaned == "." || cleaned == "/" {
		return ""
	}
	return cleaned
}

func bindEntryURL(req *http.Request, bind string) string {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		if strings.HasPrefix(bind, "http://") || strings.HasPrefix(bind, "https://") {
			return bind
		}
		return "http://" + bind
	}
	baseHost := req.Host
	if forwarded := req.Header.Get("X-Forwarded-Host"); forwarded != "" {
		baseHost = forwarded
	}
	parsedHost, _, splitErr := net.SplitHostPort(baseHost)
	if splitErr != nil {
		parsedHost = baseHost
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = parsedHost
	case "127.0.0.1", "localhost":
	default:
		if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
			host = strings.Trim(host, "[]")
		}
	}
	scheme := "http"
	if req.TLS != nil || strings.EqualFold(req.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	entry := &url.URL{Scheme: scheme, Host: net.JoinHostPort(host, port)}
	return entry.String()
}

func collectChanged(instances map[string]config.InstanceSpec) []config.InstanceSpec {
	result := make([]config.InstanceSpec, 0, len(instances))
	for _, name := range sortedInstanceNames(instances) {
		result = append(result, instances[name])
	}
	return result
}

func parseGeneration(req *http.Request) (uint64, error) {
	value := strings.TrimSpace(req.URL.Query().Get("generation"))
	if value == "" {
		return 0, errors.New("generation is required")
	}
	return parseUint(value)
}

func parseUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("invalid generation")
	}
	return parsed, nil
}

func writeJSON(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func writeConfigError(w http.ResponseWriter, err error) {
	var conflict conflictError
	if errors.As(err, &conflict) {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeError(w, http.StatusBadRequest, err)
}
