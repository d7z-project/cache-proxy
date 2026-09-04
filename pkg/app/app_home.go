package app

import (
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	urlpkg "net/url"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
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
	DiskUsage   string
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
	var statsSnapshot metrics.StatsSnapshot
	if a.stats != nil {
		statsSnapshot = a.stats.Snapshot()
	}
	var usage map[string]int64
	if len(a.stores) > 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name)
		}
		usage = a.tenantUsage(req.Context(), names)
	}
	if usage == nil {
		usage = make(map[string]int64)
	}
	instances := make([]homeInstance, 0, len(entries))
	modes := make([]string, 0, len(entries))
	seenModes := map[string]struct{}{}
	totalCache := make(map[string]uint64)
	var totalRequests uint64
	var totalDiskBytes int64
	var activeDownloads int64
	for _, entry := range entries {
		if !entry.Enabled {
			continue
		}
		instanceStats := statsSnapshot.Instances[entry.Name]
		totalRequests += instanceStats.Requests
		totalDiskBytes += usage[entry.Name]
		activeDownloads += instanceStats.ActiveDownloads
		for result, count := range instanceStats.Cache {
			totalCache[result] += count
		}
		instanceView := buildHomeInstance(entry, baseURL, req, instanceStats, usage[entry.Name], i18n)
		if _, ok := seenModes[instanceView.Mode]; !ok {
			seenModes[instanceView.Mode] = struct{}{}
			modes = append(modes, instanceView.Mode)
		}
		instances = append(instances, instanceView)
	}
	i18nJSON, _ := json.Marshal(i18n)
	var degraded int
	for _, store := range a.stores {
		storeStats, err := store.Stats(req.Context())
		if err == nil && storeStats != nil {
			degraded += storeStats.DegradedObjects
		}
	}
	return homeData{
		Instances:     instances,
		Modes:         modes,
		Single:        single,
		Locale:        locale,
		Theme:         detectTheme(req),
		I18NJSON:      template.JS(i18nJSON),
		StoreHealthy:  degraded == 0,
		StoreDegraded: degraded,
		Languages:     supportedLocales,
		LocaleLabel:   localeLabel(locale),
		TotalRequests: formatCompact(totalRequests),
		TotalHitRate:  formatHitRate(totalCache),
		TotalDisk:     formatBytes(totalDiskBytes),
		Active:        formatCompact(uint64(max(activeDownloads, 0))),
	}
}

func buildHomeInstance(
	entry *proxyruntime.Entry,
	baseURL string,
	request *http.Request,
	stats metrics.InstanceStats,
	diskBytes int64,
	i18n map[string]string,
) homeInstance {
	instanceURL := baseURL + entry.Path
	if entry.Bind != "" {
		instanceURL = entry.DisplayURL
		if instanceURL == "" {
			instanceURL = bindDisplayURL(request, entry.Bind)
		}
	}
	instanceView := homeInstance{
		Name: entry.Name,
		Mode: entry.Mode,
		URL:  instanceURL,
	}
	instanceView.SetupNote, instanceView.SetupCmd = setupCommand(entry.Mode, instanceURL)
	if instanceView.SetupNote != "" {
		instanceView.SetupCopy = instanceView.SetupNote + "\n" + instanceView.SetupCmd
	} else {
		instanceView.SetupCopy = instanceView.SetupCmd
	}
	instanceView.Requests = formatCompact(stats.Requests)
	instanceView.HitRate = formatHitRate(stats.Cache)
	instanceView.DiskUsage = formatBytes(diskBytes)
	if statusSource, ok := entry.Runtime.(proxyruntime.StatusSource); ok {
		instanceView.StatusColor, instanceView.StatusLabel, instanceView.StatusExtra = statusSource.DashboardStatus()
	} else {
		if stats.UpstreamRequests == 0 {
			instanceView.StatusLabel = "\u2014"
		} else if float64(stats.UpstreamErrors)/float64(stats.UpstreamRequests)*100 >= 5 {
			instanceView.StatusColor = "yellow"
			instanceView.StatusLabel = i18nStr(i18n, "n_err", int(stats.UpstreamErrors))
		} else {
			instanceView.StatusColor = "green"
			instanceView.StatusLabel = i18nStr(i18n, "upstream_ok")
		}
	}
	return instanceView
}

func sortedEntries(entries map[string]*proxyruntime.Entry) []*proxyruntime.Entry {
	items := make([]*proxyruntime.Entry, 0, len(entries))
	for _, name := range proxyruntime.SortedNames(entries) {
		items = append(items, entries[name])
	}
	return items
}

func (a *App) publicBaseURL(req *http.Request) string {
	if url := strings.TrimRight(a.config.Server.PublicURL, "/"); url != "" {
		return url
	}
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + req.Host
}

func bindDisplayURL(req *http.Request, bind string) string {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return "http://" + bind
	}
	baseHost := req.Host
	if parsedHost, _, err := net.SplitHostPort(baseHost); err == nil {
		baseHost = parsedHost
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "127.0.0.1" || host == "localhost" {
		host = baseHost
	}
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, port)
}

func setupCommand(mode, instanceURL string) (note, command string) {
	instanceURL = strings.TrimRight(instanceURL, "/")
	switch mode {
	case config.ModeGit:
		return "# Clone the repository", "git clone " + instanceURL
	case config.ModeNPM:
		return "# Set the npm registry to this proxy", "npm config set registry " + instanceURL
	case config.ModeGo:
		return "# Set Go module proxy", "go env -w GOPROXY=" + instanceURL
	case config.ModeMaven:
		return "<!-- Replace {mirror_id} if needed -->", "<mirror>\n  <id>{mirror_id}</id>\n  <url>" + instanceURL + "</url>\n  <mirrorOf>*</mirrorOf>\n</mirror>"
	case config.ModeCargo:
		return "# Replace {source_name} and keep the proxy URL as is", "[source.{source_name}]\nregistry = \"sparse+" + instanceURL + "/\"\n\n[source.crates-io]\nreplace-with = \"{source_name}\""
	case config.ModePyPI:
		return "# Replace {package} with the package you want to install", "pip install --index-url " + instanceURL + "/simple {package}"
	case config.ModeOCI:
		registry := instanceURL
		if parsed, err := urlpkg.Parse(instanceURL); err == nil && parsed.Host != "" {
			registry = parsed.Host
		}
		return "# Replace {image} and {tag}\n# Docker / Podman image references must not include http:// or https://",
			"docker pull " + registry + "/{image}:{tag}\n" +
				"podman pull " + registry + "/{image}:{tag}"
	case config.ModeAPK:
		return "# Repository URL; apk fetches APKINDEX.tar.gz from this directory", instanceURL
	case config.ModeDEB:
		note := "# Standard repo: replace {distribution} and {component}\n" +
			"# Flat repo: keep ./ as the suite and use this proxy URL"
		return note, "deb " + instanceURL + " {distribution} {component}\n" +
			"deb [trusted=yes] " + instanceURL + " ./"
	case config.ModeRPM:
		return "# Repository base URL; DNF/YUM will fetch repodata/repomd.xml below it", "baseurl=" + instanceURL
	case config.ModePacman:
		return "# Repository base URL; common layouts may still include $repo/os/$arch", "Server = " + instanceURL
	case config.ModeFlatpak:
		return "# Replace {remote} with the Flatpak remote name",
			"flatpak remote-add --if-not-exists {remote} " + instanceURL + "/{remote}.flatpakrepo"
	default:
		return "# Base URL for file access", instanceURL
	}
}
