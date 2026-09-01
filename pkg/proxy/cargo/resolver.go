package cargo

import (
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

const maxCargoRouteValueSize = 256

type resolver struct {
	options *Options
}

func newResolver(options *Options) *resolver {
	return &resolver{options: options}
}

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	lookupPath := strings.TrimPrefix(req.URL.Path, "/")
	if lookupPath != "" && !httpcache.SafePath(lookupPath) {
		return httpcache.Route{}, errors.New("invalid cargo request path")
	}
	if lookupPath == "" {
		lookupPath = "config.json"
	}
	if lookupPath == "config.json" {
		return httpcache.Route{
			ObjectPath:   "cargo/index/config.json",
			UpstreamPath: "config.json",
			Class:        httpcache.ClassMetadata,
			AuthRequired: r.options.AuthRequired,
		}, nil
	}
	if strings.HasPrefix(lookupPath, "api/v1/crates/") {
		return r.resolveCrateDownload(lookupPath)
	}
	return httpcache.Route{
		ObjectPath:   "cargo/index/" + lookupPath,
		UpstreamPath: lookupPath,
		Class:        httpcache.ClassMetadata,
	}, nil
}

func (r *resolver) resolveCrateDownload(lookupPath string) (httpcache.Route, error) {
	parts := strings.Split(strings.TrimPrefix(lookupPath, "api/v1/crates/"), "/")
	if len(parts) < 4 || len(parts) > 5 || parts[2] != "download" ||
		!validCargoRouteValue(parts[0], false) || !validCargoRouteValue(parts[1], true) {
		return httpcache.Route{}, errors.New("invalid cargo crate download route")
	}
	template, needsChecksum, err := httpcache.DecodeCargoDownloadTemplate(parts[3])
	if err != nil {
		return httpcache.Route{}, err
	}
	checksum := ""
	if needsChecksum {
		if len(parts) != 5 || !validCargoChecksum(parts[4]) {
			return httpcache.Route{}, errors.New("invalid cargo crate checksum")
		}
		checksum = parts[4]
	} else if len(parts) != 4 {
		return httpcache.Route{}, errors.New("unexpected cargo crate checksum")
	}
	prefix := cratePrefix(parts[0])
	templatePath, templateQuery, hasQuery := strings.Cut(template, "?")
	targetURL := strings.NewReplacer(
		"{crate}", parts[0],
		"{version}", parts[1],
		"{prefix}", prefix,
		"{lowerprefix}", strings.ToLower(prefix),
		"{sha256-checksum}", checksum,
	).Replace(templatePath)
	if hasQuery {
		targetURL += "?" + strings.NewReplacer(
			"{crate}", url.QueryEscape(parts[0]),
			"{version}", url.QueryEscape(parts[1]),
			"{prefix}", url.QueryEscape(prefix),
			"{lowerprefix}", url.QueryEscape(strings.ToLower(prefix)),
			"{sha256-checksum}", url.QueryEscape(checksum),
		).Replace(templateQuery)
	}
	if strings.ContainsAny(targetURL, "{}") {
		return httpcache.Route{}, errors.New("unresolved cargo download template placeholder")
	}
	parsed, err := url.Parse(targetURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return httpcache.Route{}, errors.New("invalid cargo crate target URL")
	}
	return httpcache.Route{
		ObjectPath:   path.Join("cargo/crates", strings.ToLower(parts[0]), parts[1], httpcache.HashKey(targetURL)+".crate"),
		UpstreamPath: lookupPath,
		TargetURL:    targetURL,
		Class:        httpcache.ClassContent,
	}, nil
}

func validCargoRouteValue(value string, allowVersionPunctuation bool) bool {
	if value == "" || len(value) > maxCargoRouteValueSize || value == "." || value == ".." {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '-' || character == '_' ||
			allowVersionPunctuation && (character == '.' || character == '+') {
			continue
		}
		return false
	}
	return true
}

func validCargoChecksum(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func cratePrefix(name string) string {
	characters := []rune(name)
	switch len(characters) {
	case 0:
		return ""
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3/" + string(characters[:1])
	default:
		return string(characters[:2]) + "/" + string(characters[2:4])
	}
}
