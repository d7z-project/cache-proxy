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
	SetupNote   string
	SetupCmd    string
	SetupCopy   string
	Requests    string
	HitRate     string
	StatusColor string
	StatusLabel string
	StatusExtra string
}

func (a *App) serveHome(w http.ResponseWriter, req *http.Request) {
	a.renderHomePage(w, req, a.homePageData(req, sortedEntries(a.entries), false))
}

func (a *App) serveBindHome(w http.ResponseWriter, req *http.Request, entry *proxyruntime.Entry) {
	a.renderHomePage(w, req, a.homePageData(req, []*proxyruntime.Entry{entry}, true))
}

func (a *App) renderHomePage(w http.ResponseWriter, req *http.Request, data homeData) {
	if req.Method == http.MethodHead {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		return
	}
	renderHome(w, data)
}

func (a *App) homePageData(req *http.Request, entries []*proxyruntime.Entry, single bool) homeData {
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
	modes := make([]string, 0)
	seenModes := map[string]struct{}{}
	for _, entry := range entries {
		if !entry.Enabled {
			continue
		}
		s := ss.Instances[entry.Name]
		hi := buildHomeInstance(entry, baseURL, req, s, i18n)
		if _, ok := seenModes[hi.Mode]; !ok {
			seenModes[hi.Mode] = struct{}{}
			modes = append(modes, hi.Mode)
		}
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
	return homeData{
		Instances:   instances,
		Modes:       modes,
		Single:      single,
		Locale:      locale,
		Theme:       detectTheme(req),
		LangSwitch:  langSwitch,
		ThemeSwitch: themeSwitch,
		I18NJSON:    template.JS(i18nJSON),
	}
}

func buildHomeInstance(entry *proxyruntime.Entry, baseURL string, req *http.Request, s httpproxy.InstanceStats, i18n map[string]string) homeInstance {
	instURL := instURL(entry, baseURL, req)
	hi := homeInstance{
		Name: entry.Name,
		Mode: entry.Mode,
		URL:  instURL,
	}
	hi.SetupNote, hi.SetupCmd = setupCommand(entry.Mode, instURL)
	if hi.SetupNote != "" {
		hi.SetupCopy = hi.SetupNote + "\n" + hi.SetupCmd
	} else {
		hi.SetupCopy = hi.SetupCmd
	}
	hi.Requests = formatCompact(s.Requests)
	hi.HitRate = formatHitRate(s.Cache)
	hi.StatusColor, hi.StatusLabel, hi.StatusExtra = instanceStatus(s, i18n)
	return hi
}

func sortedEntries(entries map[string]*proxyruntime.Entry) []*proxyruntime.Entry {
	items := make([]*proxyruntime.Entry, 0, len(entries))
	for _, name := range sortedEntryNames(entries) {
		items = append(items, entries[name])
	}
	return items
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

func setupCommand(mode, url string) (note, cmd string) {
	url = strings.TrimRight(url, "/")
	switch mode {
	case "npm":
		return "# Set the npm registry to this proxy", "npm config set registry " + url
	case "go":
		return "# Set Go module proxy", "go env -w GOPROXY=" + url
	case "maven":
		return "<!-- Replace {mirror_id} if needed -->", "<mirror>\n  <id>{mirror_id}</id>\n  <url>" + url + "</url>\n  <mirrorOf>*</mirrorOf>\n</mirror>"
	case "cargo":
		return "# Replace {source_name} and keep the proxy URL as is", "[source.{source_name}]\nregistry = \"sparse+" + url + "/\"\n\n[source.crates-io]\nreplace-with = \"{source_name}\""
	case "pypi":
		return "# Replace {package} with the package you want to install", "pip install --index-url " + url + "/simple {package}"
	case "oci":
		return "# Replace {image} and {tag}", "docker pull " + url + "/{image}:{tag}"
	case "apk":
		return "# Replace {alpine_branch} and {repository}", url + "/{alpine_branch}/{repository}"
	case "deb":
		return "# Replace {distribution} and {component}", "deb " + url + " {distribution} {component}"
	case "rpm":
		return "# Replace {releasever}, {repository}, and {arch}", "baseurl=" + url + "/{releasever}/{repository}/{arch}"
	case "pacman":
		return "# Replace {repo} and {arch}", "Server = " + url + "/{repo}/os/{arch}"
	default:
		return "# Base URL for file access", url
	}
}
