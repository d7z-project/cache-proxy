package app

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type homeRelease struct {
	Key            string
	Gen            string
	Published      string
	PublishedTitle string
	Packages       string
	Upstream       string
	StateLabel     string
	StateColor     string
	LastOK         string
	LastOKTitle    string
	LastTry        string
	LastTryTitle   string
	LastError      string
}

type homeInstance struct {
	Name             string
	Mode             string
	URL              string
	SetupNote        string
	SetupCmd         string
	SetupCopy        string
	Requests         string
	HitRate          string
	DiskUsage        string
	StatusColor      string
	StatusLabel      string
	StatusExtra      string
	StatusExtraTitle string
	HasReleases      bool
	Releases         []homeRelease
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
	var ss httpcache.StatsSnapshot
	if a.stats != nil {
		ss = a.stats.Snapshot()
	}
	var storeStats *blobfs.StatsSnapshot
	var usage map[string]int64
	if a.store != nil {
		storeStats, _ = a.store.Stats(req.Context())
		usage = a.tenantUsage(req.Context(), entryNames(entries))
	}
	if usage == nil {
		usage = make(map[string]int64)
	}
	instances := make([]homeInstance, 0)
	modes := make([]string, 0)
	seenModes := map[string]struct{}{}
	for _, entry := range entries {
		if !entry.Enabled {
			continue
		}
		s := ss.Instances[entry.Name]
		hi := buildHomeInstance(entry, baseURL, req, s, usage[entry.Name], i18n)
		if _, ok := seenModes[hi.Mode]; !ok {
			seenModes[hi.Mode] = struct{}{}
			modes = append(modes, hi.Mode)
		}
		instances = append(instances, hi)
	}
	i18nJSON, _ := json.Marshal(i18n)
	healthy := true
	var degraded int
	if storeStats != nil {
		healthy = storeStats.DegradedObjects == 0
		degraded = storeStats.DegradedObjects
	}
	return homeData{
		Instances:     instances,
		Modes:         modes,
		Single:        single,
		Locale:        locale,
		Theme:         detectTheme(req),
		I18NJSON:      template.JS(i18nJSON),
		StoreHealthy:  healthy,
		StoreDegraded: degraded,
	}
}

func buildHomeInstance(entry *proxyruntime.Entry, baseURL string, req *http.Request, s httpcache.InstanceStats, diskBytes int64, i18n map[string]string) homeInstance {
	instURL := instURL(entry, baseURL, req)
	now := time.Now()
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
	hi.DiskUsage = formatBytes(diskBytes)
	if src, ok := entry.Runtime.(proxyruntime.StatusSource); ok {
		hi.StatusColor, hi.StatusLabel, hi.StatusExtra = src.DashboardStatus()
	} else {
		hi.StatusColor, hi.StatusLabel, hi.StatusExtra, hi.StatusExtraTitle = instanceStatus(s, i18n, now)
	}
	if src, ok := entry.Runtime.(proxyruntime.RootReleaseSource); ok {
		releases := src.RootReleases()
		hi.HasReleases = true
		hi.Releases = make([]homeRelease, len(releases))
		for i, r := range releases {
			hi.Releases[i] = buildHomeRelease(r, i18n, now)
		}
	}
	return hi
}

func buildHomeRelease(r proxyruntime.RootRelease, i18n map[string]string, now time.Time) homeRelease {
	pub, pubTitle := formatRecentTime(r.Published, i18n, now)
	lastOK, lastOKTitle := formatRecentTime(r.LastSuccessAt, i18n, now)
	lastTry, lastTryTitle := formatRecentTime(r.LastRefreshAt, i18n, now)
	upstream := r.Upstream
	if u, err := url.Parse(upstream); err == nil && u.Host != "" {
		upstream = u.Host
	}
	state := releaseStateLabelKey(r)
	return homeRelease{
		Key:            r.Key,
		Gen:            releaseGenerationLabel(r, i18n),
		Published:      pub,
		PublishedTitle: pubTitle,
		Packages:       fmt.Sprintf("%d %s", r.ArtifactCount, i18nStr(i18n, "packages")),
		Upstream:       upstream,
		StateLabel:     i18nStr(i18n, "release_state_"+state),
		StateColor:     formatRootStateColor(state),
		LastOK:         lastOK,
		LastOKTitle:    lastOKTitle,
		LastTry:        lastTry,
		LastTryTitle:   lastTryTitle,
		LastError:      r.LastError,
	}
}

func releaseStateLabelKey(r proxyruntime.RootRelease) string {
	state := r.State
	switch {
	case r.Refreshing && r.HasCurrent:
		return "refreshing"
	case r.Refreshing:
		return "bootstrapping"
	case !r.HasCurrent && r.State == "blocked":
		return "failed"
	case !r.HasCurrent && (r.State == "" || r.State == "pending"):
		return "booting"
	case state == "":
		return "pending"
	default:
		return state
	}
}

func releaseGenerationLabel(r proxyruntime.RootRelease, i18n map[string]string) string {
	generation := r.Generation
	if !r.HasCurrent || generation == "" {
		generation = i18nStr(i18n, "none")
	}
	return fmt.Sprintf("%s %s", i18nStr(i18n, "release_generation"), generation)
}

func sortedEntries(entries map[string]*proxyruntime.Entry) []*proxyruntime.Entry {
	items := make([]*proxyruntime.Entry, 0, len(entries))
	for _, name := range proxyruntime.SortedNames(entries) {
		items = append(items, entries[name])
	}
	return items
}

func entryNames(entries []*proxyruntime.Entry) []string {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	return names
}

func (a *App) publicBaseURL(req *http.Request) string {
	if url := strings.TrimRight(a.config.Server.PublicURL, "/"); url != "" {
		return url
	}
	return httpcache.BaseURL(req)
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
	case "git":
		return "# Clone the repository", "git clone " + url
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
