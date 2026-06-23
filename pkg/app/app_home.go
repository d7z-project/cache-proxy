package app

import (
	"html/template"
	"net"
	"net/http"
	"strings"
)

type homeInstance struct {
	Name     string
	Mode     string
	URL      string
	SetupCmd string
}

func (a *App) serveHome(w http.ResponseWriter, req *http.Request) {
	instances := make([]homeInstance, 0, len(a.entries))
	baseURL := a.publicBaseURL(req)
	for _, name := range sortedEntryNames(a.entries) {
		entry := a.entries[name]
		if !entry.Enabled {
			continue
		}
		instURL := baseURL + entry.Path
		if entry.Bind != "" {
			if entry.Home.DisplayURL != "" {
				instURL = entry.Home.DisplayURL
			} else {
				instURL = a.bindURL(req, entry.Bind)
			}
		}
		instances = append(instances, homeInstance{
			Name:     entry.Name,
			Mode:     entry.Mode,
			URL:      instURL,
			SetupCmd: setupCommand(entry.Mode, instURL),
		})
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = homeTemplate.Execute(w, map[string]any{
		"Instances":   instances,
		"Count":       len(instances),
		"Bind":        a.config.Server.Bind,
		"MetricsPath": a.config.Metrics.Path,
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

func (a *App) bindURL(req *http.Request, bind string) string {
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

var homeTemplate = template.Must(template.New("home").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>cache-proxy</title>
<style>
:root{color-scheme:light;}
*,*::before,*::after{box-sizing:border-box;margin:0}
body{font:14px/1.6 system-ui,-apple-system,sans-serif;background:#f8f9fa;color:#1a1a2e;min-height:100vh}
header{background:#fff;border-bottom:1px solid #e9ecef;padding:24px 0}
header .wrap{max-width:960px;margin:0 auto;padding:0 24px;display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px}
header h1{font-size:28px;font-weight:700;letter-spacing:-.02em}
header h1 span{font-weight:400;color:#868e96;font-size:16px;margin-left:8px}
header p{color:#868e96;font-size:13px;margin-top:2px}
summary-bar{display:flex;gap:20px;font-size:13px;color:#868e96}
summary-bar b{color:#495057;font-weight:600}
main{max-width:960px;margin:0 auto;padding:32px 24px 64px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px}
.card{background:#fff;border:1px solid #e9ecef;border-radius:12px;overflow:hidden;transition:box-shadow .15s}
.card:hover{box-shadow:0 4px 16px rgba(0,0,0,.06)}
.card-head{display:flex;align-items:center;gap:10px;padding:16px 16px 0}
.card-head .name{font-weight:600;font-size:16px}
.card-body{padding:12px 16px}
.card-body .url{display:flex;align-items:center;gap:8px}
.card-body .url a{color:#1971c2;text-decoration:none;font:13px/1.5 "SFMono-Regular",Consolas,"Liberation Mono",monospace;word-break:break-all}
.card-body .url a:hover{text-decoration:underline}
.card-body .url button{flex-shrink:0;font-size:11px;padding:3px 10px;border:1px solid #dee2e6;border-radius:6px;background:#fff;color:#495057;cursor:pointer;white-space:nowrap}
.card-body .url button:hover{background:#f1f3f5}
.card-body .url button.copied{background:#d3f9d8;border-color:#8ce99a;color:#2b8a3e}
.card-code{background:#212529;color:#e9ecef;padding:16px;font:12px/1.6 "SFMono-Regular",Consolas,"Liberation Mono",monospace;overflow:auto;white-space:pre-wrap;word-break:break-all;cursor:pointer;user-select:all;min-height:48px;transition:background .2s}
.card-code:hover{background:#2b3035}
.card-code:active{background:#343a40}
.badge{display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:600;letter-spacing:.04em;text-transform:uppercase}
.badge-npm{background:#fff0f0;color:#c92a2a}
.badge-go{background:#e3fafc;color:#0b7285}
.badge-oci{background:#f3f0ff;color:#6741d9}
.badge-maven{background:#fff4e6;color:#e8590c}
.badge-cargo{background:#f4ece6;color:#5c3d2e}
.badge-pypi{background:#e7f5ff;color:#1971c2}
.badge-file{background:#f1f3f5;color:#495057}
.badge-apk,.badge-deb,.badge-rpm,.badge-pacman{background:#ebfbee;color:#2b8a3e}
.empty{padding:48px 0;text-align:center;color:#adb5bd;font-size:15px;grid-column:1/-1}
footer{max-width:960px;margin:0 auto;padding:0 24px 32px;font-size:12px;color:#adb5bd}
</style>
</head>
<body>
<header>
  <div class="wrap">
    <div>
      <h1>cache-proxy<span>Stable</span></h1>
      <p>Caching reverse proxy for package registries</p>
    </div>
    <summary-bar>
      <div><b>{{.Count}}</b> instances</div>
      <div>metrics <b>{{.MetricsPath}}</b></div>
      <div>bind <b>{{.Bind}}</b></div>
    </summary-bar>
  </div>
</header>
<main>
  <div class="grid">
    {{range .Instances}}
    <div class="card">
      <div class="card-head">
        <span class="badge badge-{{.Mode}}">{{.Mode}}</span>
        <span class="name">{{.Name}}</span>
      </div>
      <div class="card-body">
        <div class="url">
          <a href="{{.URL}}">{{.URL}}</a>
          <button onclick="copyURL(this,'{{.URL}}')">Copy</button>
        </div>
      </div>
      <div class="card-code" onclick="copyCode(this,'{{.SetupCmd}}')">{{.SetupCmd}}</div>
    </div>
    {{else}}
    <div class="empty">No instances enabled. Add instances to your YAML config.</div>
    {{end}}
  </div>
</main>
<footer>cache-proxy · runtime defined by YAML</footer>
<script>
function copyURL(btn,txt){navigator.clipboard.writeText(txt);btn.textContent='Copied!';btn.classList.add('copied');setTimeout(()=>{btn.textContent='Copy';btn.classList.remove('copied')},1500)}
function copyCode(el,txt){navigator.clipboard.writeText(txt);el.style.background='#1b4332';setTimeout(()=>el.style.background='',600)}
</script>
</body>
</html>`))
