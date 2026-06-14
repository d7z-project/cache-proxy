# cache-proxy

**此项目是 Dragon's Zone Homelab 的一部分**

`cache-proxy` is a small Go caching reverse proxy with an embedded Angular admin UI. It uses `blobfs` for both cached objects and the internal runtime configuration.

## Features

- File, OCI registry, npm registry, Go module, Maven/Gradle, Cargo, and PyPI proxy modes.
- Embedded Web UI for instance management.
- Internal config stored in BlobFS as sharded resources under the system tenant.
- One main listener for the Web UI, Admin API, path-mounted proxies, and metrics.
- Optional dedicated bind listeners per proxy instance.
- Runtime instance create, update, delete, import, and export.
- Cache policies: `bypass`, `immutable`, and `revalidate`.
- Glob-based file cache rules using `github.com/bmatcuk/doublestar/v4`.
- OCI bearer challenge handling and npm tarball URL rewriting.
- Prometheus metrics for proxy traffic and BlobFS state.

## Requirements

- Go 1.25+
- Node.js and npm

## Build

```bash
make cache-proxy
```

This builds the Angular UI first and embeds it into the Go binary.

## Run

```bash
./cache-proxy \
  -backend ./build/cache \
  -bind 127.0.0.1:18080 \
  -metrics-path /-/metrics
```

Open the admin UI:

```text
http://127.0.0.1:18080/#/dashboard
```

Path-mounted proxy instances are served on the same main listener:

```text
http://127.0.0.1:18080/files/
```

Go modules can use a path-mounted instance directly:

```bash
GOPROXY=http://127.0.0.1:18080/go go mod download
```

## Options

| Flag | Environment | Default | Description |
| --- | --- | --- | --- |
| `-backend` | `CACHE_PROXY_BACKEND` | `/tmp/cache-proxy` | BlobFS backend directory. |
| `-bind` | `CACHE_PROXY_BIND` | `127.0.0.1:18080` | Main Web/API/path-proxy address. |
| `-pass` | `CACHE_PROXY_PASS` | empty | Admin password. Empty disables authentication. |
| `-metrics-token` | `CACHE_PROXY_METRICS_TOKEN` | empty | Metrics Bearer token. Empty disables metrics authentication. |
| `-metrics-path` | `CACHE_PROXY_METRICS_PATH` | `/-/metrics` | Prometheus path on the main listener. |
| `-gc-interval` | `CACHE_PROXY_GC_INTERVAL` | `24h` | BlobFS GC interval. |

These are startup-only options. Runtime proxy instances are managed from the Web UI or Admin API.

## Proxy Modes

### File

File mode proxies normal HTTP files. It supports path mounting, dedicated bind addresses, selected request header pass-through, and glob cache rules.

### OCI

OCI mode proxies container registries. It uses a dedicated bind address because OCI clients expect registry-root paths. The backend handles bearer challenges, token expiry, and manifest `Accept` negotiation.

### npm

npm mode proxies one npm registry upstream. Package metadata is rewritten so `dist.tarball` URLs point back to the proxy and tarball downloads stay cached.

### Go

Go mode implements a pure GOPROXY cache through `github.com/goproxy/goproxy` plus an internal upstream-only fetcher. It only talks to configured HTTP GOPROXY upstreams such as `https://proxy.golang.org` and an optional proxied SumDB endpoint.

It does not:

- execute a local `go` binary
- fall back to direct VCS/source fetches
- honor `GOPRIVATE`, `GONOPROXY`, or any other direct-fetch path

That means the runtime has no dependency on local Go toolchains or VCS tools. It also means branch, commit, and other direct source queries are rejected unless an upstream GOPROXY can answer them through the standard module proxy protocol.

Go instances also support `goprivate` match rules. When a module path matches one of those patterns, this proxy immediately returns `404 Not Found` for module requests so the client can continue along its own `GOPROXY` fallback chain.

### Maven / Gradle

Maven mode proxies Maven-compatible repositories and works for common Gradle `maven {}` usage. It treats metadata and SNAPSHOT paths differently from immutable release artifacts so repository metadata can refresh without disabling artifact caching.

### Cargo

Cargo mode implements the sparse registry protocol only. It proxies `config.json`, sparse index entries, and crate downloads, and rewrites the registry download template back to the proxy itself. It does not implement the legacy git index protocol or publish APIs.

### PyPI

PyPI mode proxies the Simple API and distribution files. It rewrites package file links back to the proxy so `pip` and other installers keep downloads inside the cache path.

## Cache Policies

- `bypass`: forward to upstream without storing the response.
- `immutable`: store once and reuse the cached object.
- `revalidate`: validate with upstream before serving expired cached content.

`freshFor` skips upstream checks for recently fetched cached objects.

`busyPolicy` controls concurrent requests for the same object:

- `bypass`: send concurrent requests directly upstream.
- `stale`: serve an existing cached object while refresh is busy.

## Admin API

| Endpoint | Method | Description |
| --- | --- | --- |
| `/-/api/runtime` | `GET` | Runtime status. |
| `/-/api/global-config` | `GET`, `PUT` | Read or update global config. |
| `/-/api/instances` | `GET` | List instances. |
| `/-/api/instances/:name` | `GET`, `PUT`, `DELETE` | Read, update, or delete one instance. |
| `/-/api/instances` | `POST` | Create one instance. |
| `/-/api/instances/export` | `GET` | Export instances. |
| `/-/api/instances/import` | `POST` | Import instances. |
| `/-/api/system/reset` | `POST` | Reset runtime state. |
| `/-/api/metrics/stats` | `GET` | JSON metrics snapshot. |
| `/-/api/storage/stats` | `GET` | BlobFS stats. |
| `/-/api/storage/gc` | `POST` | Run BlobFS GC. |

## Development

```bash
go test ./...
cd web && npm run build
cd ..
go build ./...
go test -race ./pkg/server
go vet ./...
```

## Project Layout

```text
.
├── main.go
├── pkg/config
├── pkg/proxy
├── pkg/proxy/file
├── pkg/proxy/gomod
├── pkg/proxy/maven
├── pkg/proxy/cargo
├── pkg/proxy/oci
├── pkg/proxy/npm
├── pkg/proxy/pypi
├── pkg/server
├── pkg/utils
└── web
```

## License

MIT
