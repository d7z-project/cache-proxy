# cache-proxy

**此项目是 Dragon's Zone Homelab 的一部分**

`cache-proxy` is a small Go caching reverse proxy with an embedded Angular admin UI. It uses `blobfs` for both cached objects and the internal runtime configuration.

## Features

- File, OCI registry, and npm registry proxy modes.
- Embedded Web UI for instance management.
- Internal config stored in BlobFS as `_system/config.yaml`.
- Separate admin, path-proxy, metrics, and instance bind listeners.
- Runtime instance create, update, delete, import, and export.
- Cache policies: `bypass`, `immutable`, and `revalidate`.
- Glob-based file cache rules using `github.com/bmatcuk/doublestar/v4`.
- OCI bearer challenge handling and npm tarball URL rewriting.
- Prometheus metrics for proxy traffic and BlobFS state.

## Requirements

- Go 1.24+
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
  -admin 127.0.0.1:18080 \
  -proxy-bind 127.0.0.1:18081 \
  -metrics-bind 127.0.0.1:8911
```

Open the admin UI:

```text
http://127.0.0.1:18080/#/dashboard
```

The shared path proxy listens on `-proxy-bind`:

```text
http://127.0.0.1:18081/
```

## Options

| Flag | Environment | Default | Description |
| --- | --- | --- | --- |
| `-backend` | `CACHE_PROXY_BACKEND` | `/tmp/cache-proxy` | BlobFS backend directory. |
| `-admin` | `CACHE_PROXY_ADMIN` | `127.0.0.1:18080` | Admin Web/API address. |
| `-proxy-bind` | `CACHE_PROXY_PROXY_BIND` | `127.0.0.1:18081` | Shared path proxy address. |
| `-metrics-bind` | `CACHE_PROXY_METRICS_BIND` | `127.0.0.1:8911` | Prometheus address. Empty disables metrics. |
| `-metrics-path` | `CACHE_PROXY_METRICS_PATH` | `/metrics` | Prometheus path. |
| `-gc-interval` | `CACHE_PROXY_GC_INTERVAL` | `24h` | BlobFS GC interval. |

These are startup-only options. Runtime proxy instances are managed from the Web UI or Admin API.

## Proxy Modes

### File

File mode proxies normal HTTP files. It supports path mounting, dedicated bind addresses, selected request header pass-through, and glob cache rules.

### OCI

OCI mode proxies container registries. It uses a dedicated bind address because OCI clients expect registry-root paths. The backend handles bearer challenges, token expiry, and manifest `Accept` negotiation.

### npm

npm mode proxies one npm registry upstream. Package metadata is rewritten so `dist.tarball` URLs point back to the proxy and tarball downloads stay cached.

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
| `/api/runtime` | `GET` | Runtime status. |
| `/api/config` | `GET`, `PUT` | Read or replace config. |
| `/api/config/validate` | `POST` | Validate config. |
| `/api/config/reset` | `POST` | Reset config. |
| `/api/instances` | `GET` | List instances. |
| `/api/instances/export` | `GET` | Export instances. |
| `/api/instances/import` | `POST` | Import instances. |
| `/api/metrics/stats` | `GET` | JSON metrics snapshot. |
| `/api/storage/stats` | `GET` | BlobFS stats. |
| `/api/storage/gc` | `POST` | Run BlobFS GC. |

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
├── pkg/proxy/oci
├── pkg/proxy/npm
├── pkg/server
├── pkg/utils
└── web
```

## License

MIT
