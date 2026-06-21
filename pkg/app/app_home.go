package app

import (
	"html/template"
	"net"
	"net/http"
	"strings"
)

func (a *App) serveHome(w http.ResponseWriter, req *http.Request) {
	type homeInstance struct {
		Name    string
		Mode    string
		Target  string
		Snippet string
	}
	type homeData struct {
		Bind        string
		MetricsPath string
		Instances   []homeInstance
	}

	instances := make([]homeInstance, 0, len(a.entries))
	for _, name := range sortedEntryNames(a.entries) {
		entry := a.entries[name]
		if !entry.Enabled {
			continue
		}
		target := requestBaseURL(req) + entry.Path
		if entry.Bind != "" {
			target = bindURL(req, entry.Bind)
		}
		instances = append(instances, homeInstance{
			Name:    entry.Name,
			Mode:    entry.Mode,
			Target:  target,
			Snippet: entry.Home.Snippet,
		})
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = homeTemplate.Execute(w, homeData{
		Bind:        a.config.Server.Bind,
		MetricsPath: a.config.Metrics.Path,
		Instances:   instances,
	})
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

var homeTemplate = template.Must(template.New("home").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>cache-proxy</title>
  <style>
    :root { color-scheme: light; --bg:#f4f1ea; --panel:#fffdf8; --ink:#1e1c1a; --muted:#6c6256; --line:#e4dacd; --accent:#0d6b5c; --accent-soft:#d9efe9; }
    * { box-sizing: border-box; }
    body { margin:0; font:15px/1.6 "Iowan Old Style","Palatino Linotype","Book Antiqua",serif; background:linear-gradient(180deg,#f7f3ec 0%,#efe7db 100%); color:var(--ink); }
    main { max-width:980px; margin:0 auto; padding:40px 20px 64px; }
    .hero, .panel { background:rgba(255,253,248,.92); border:1px solid var(--line); border-radius:20px; box-shadow:0 16px 50px rgba(73,50,20,.08); }
    .hero { padding:28px; margin-bottom:18px; }
    .hero h1 { margin:0 0 10px; font-size:40px; line-height:1.1; }
    .hero p, .panel p { margin:0; color:var(--muted); }
    .grid { display:grid; gap:18px; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); }
    .panel { padding:22px; }
    .label { display:inline-block; padding:4px 10px; border-radius:999px; background:var(--accent-soft); color:var(--accent); font-size:12px; letter-spacing:.08em; text-transform:uppercase; }
    ul { margin:14px 0 0; padding-left:18px; }
    li { margin:8px 0; }
    a { color:var(--accent); text-decoration:none; }
    code, pre { font:13px/1.5 "SFMono-Regular","Consolas","Liberation Mono",monospace; }
    details { border-top:1px solid var(--line); padding-top:12px; margin-top:12px; }
    summary { cursor:pointer; font-weight:600; }
    pre { margin:12px 0 0; padding:14px; overflow:auto; border-radius:14px; background:#1d2a28; color:#ebfff8; }
    .instance { padding:14px 0; border-top:1px solid var(--line); }
    .instance:first-of-type { border-top:0; padding-top:0; }
    .instance strong { font-size:18px; }
    .instance span { color:var(--muted); }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <span class="label">Static YAML Proxy</span>
      <h1>cache-proxy</h1>
      <p>BlobFS stores cached objects only. Runtime topology is defined entirely by one YAML file at startup.</p>
    </section>
    <section class="grid">
      <article class="panel">
        <span class="label">Endpoints</span>
        <ul>
          <li>Main bind: <code>{{ .Bind }}</code></li>
          <li>Metrics: <code>{{ .MetricsPath }}</code></li>
          <li>Configured instances: <code>{{ len .Instances }}</code></li>
        </ul>
      </article>
      <article class="panel">
        <span class="label">How To Configure</span>
        <p>Each instance defines exactly one mode block. Route ownership is decided by the mode itself during startup planning.</p>
        <details>
          <summary>Minimal startup command</summary>
          <pre>./cache-proxy -config ./cache-proxy.yaml</pre>
        </details>
      </article>
    </section>
    <section class="panel" style="margin-top:18px;">
      <span class="label">Published Routes</span>
      {{ range .Instances }}
      <div class="instance">
        <strong>{{ .Name }}</strong> <span>{{ .Mode }}</span>
        <div><a href="{{ .Target }}">{{ .Target }}</a></div>
        <details>
          <summary>YAML snippet</summary>
          <pre>{{ .Snippet }}</pre>
        </details>
      </div>
      {{ else }}
      <p style="margin-top:14px;">No proxy instances are configured.</p>
      {{ end }}
    </section>
  </main>
</body>
</html>`))
