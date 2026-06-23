package app

import (
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"strings"

	httpproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type homeInstance struct {
	Name        string
	Mode        string
	URL         string
	SetupCmd    string
	Requests    string
	HitRate     string
	StatusColor string
	StatusLabel string
	StatusExtra string
}

func (a *App) serveHome(w http.ResponseWriter, req *http.Request) {
	locale := detectLocale(req)
	i18n := i18nMaps[locale]
	if i18n == nil {
		i18n = i18nMaps["en"]
	}
	baseURL := a.publicBaseURL(req)
	var ss httpproxy.StatsSnapshot
	if a.stats != nil {
		ss = a.stats.Snapshot()
	}
	instances := make([]homeInstance, 0)
	for _, name := range sortedEntryNames(a.entries) {
		entry := a.entries[name]
		if !entry.Enabled {
			continue
		}
		instURL := instURL(entry, baseURL, req)
		hi := homeInstance{
			Name:     entry.Name,
			Mode:     entry.Mode,
			URL:      instURL,
			SetupCmd: setupCommand(entry.Mode, instURL),
		}
		s := ss.Instances[entry.Name]
		hi.Requests = formatCompact(s.Requests) + " " + i18n["requests"]
		hi.HitRate = formatHitRate(s.Cache)
		hi.StatusColor, hi.StatusLabel, hi.StatusExtra = instanceStatus(s, i18n)
		instances = append(instances, hi)
	}
	langSwitch := "EN"
	if locale == "en" {
		langSwitch = "ZH"
	}
	themeSwitch := "\u591c"
	if detectTheme(req) == "dark" {
		themeSwitch = "\u65e5"
	}
	i18nJSON, _ := json.Marshal(i18n)
	renderHome(w, homeData{
		Instances:   instances,
		Count:       len(instances),
		Locale:      locale,
		Theme:       detectTheme(req),
		LangSwitch:  langSwitch,
		ThemeSwitch: themeSwitch,
		I18NJSON:    template.JS(i18nJSON),
	})
}

func (a *App) publicBaseURL(req *http.Request) string {
	if url := strings.TrimRight(a.config.Server.PublicURL, "/"); url != "" {
		return url
	}
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

func instURL(entry *proxyruntime.Entry, baseURL string, req *http.Request) string {
	if entry.Bind != "" {
		if entry.Home.DisplayURL != "" {
			return entry.Home.DisplayURL
		}
		return bindURL(req, entry.Bind)
	}
	return baseURL + entry.Path
}

func bindURL(req *http.Request, bind string) string {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return "http://" + bind
	}
	baseHost := req.Host
	if forwardedHost := req.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		baseHost = forwardedHost
	}
	if parsedHost, _, err := net.SplitHostPort(baseHost); err == nil {
		baseHost = parsedHost
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "127.0.0.1" || host == "localhost" {
		host = baseHost
	}
	scheme := "http"
	if req.TLS != nil || strings.EqualFold(req.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, port)
}

func setupCommand(mode, url string) string {
	url = strings.TrimRight(url, "/")
	switch mode {
	case "npm":
		return "npm config set registry " + url
	case "go":
		return "go env -w GOPROXY=" + url
	case "maven":
		return "<mirror>\n  <url>" + url + "</url>\n</mirror>"
	case "cargo":
		return "[source.cache-proxy]\nregistry = \"sparse+" + url + "/\""
	case "pypi":
		return "pip install --index-url " + url + "/simple <package>"
	case "oci":
		return "docker pull " + url + "/<image>:<tag>"
	case "apk":
		return url + "/v3.20/main"
	case "deb":
		return "deb " + url + " bookworm main"
	case "rpm":
		return "baseurl=" + url + "/9/BaseOS/x86_64/os"
	case "pacman":
		return "Server = " + url + "/$repo/os/$arch"
	default:
		return url
	}
}
