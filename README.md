# cache-proxy

`cache-proxy` is a caching reverse proxy for package registries and artifact repositories. Single binary, single YAML config, BlobFS-backed storage.

## Features

- 11 proxy modes in one process: `file`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman`
- Path-mounted and dedicated-listener instances
- Per-resource cache policies (`bypass` / `immutable` / `revalidate`) with freshness and concurrency controls
- Prometheus metrics, background GC, expired-object cleanup
- Discovery-driven metadata refresh for Linux package repositories (`apk`, `deb`, `rpm`, `pacman`)

## Quick Start

```bash
make cache-proxy
./cache-proxy -config ./cache-proxy.yaml
./cache-proxy -config ./cache-proxy.yaml -validate   # dry-run only
```

Minimal config:

```yaml
server:
  bind: 127.0.0.1:18080
  backend: /var/lib/cache-proxy

metrics:
  path: /metrics

instances:
  - name: npmjs
    enabled: true
    npm:
      route: { path: /npm }
      upstream: https://registry.npmjs.org
```

## Configuration Reference

### Top-level fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `server.bind` | `host:port` | `127.0.0.1:18080` | Main HTTP listener |
| `server.backend` | path | `/tmp/cache-proxy` | BlobFS storage directory |
| `server.public_url` | URL | — | Override scheme+host shown on home page (e.g. `https://cache.home.lan`) |
| `metrics.path` | path | `/metrics` | Prometheus endpoint |
| `metrics.token` | string | — | Bearer token for `/metrics` |
| `storage.gc.blob` | duration | `24h` | BlobFS GC interval |
| `storage.cleanup.enabled` | bool | `false` | Enable stale-object cleanup |
| `storage.cleanup.interval` | duration | `6h` | Cleanup scan interval |
| `storage.cleanup.dry_run` | bool | `false` | Log deletions without removing |
| `storage.cleanup.batch_size` | int | `500` | Max objects per batch |
| `storage.cleanup.workers` | int | `0` | Concurrency (`0` = auto) |

### Value types

Values appearing across all mode tables:

| Category | Format | Examples | Special values |
| --- | --- | --- | --- |
| Duration | Go duration | `30s`, `2m`, `1h`, `24h` | — |
| Expiration | Duration | `720h` | `never` |
| Freshness | Duration | `1m`, `30s` | `forever` |
| Policy | Enum | — | `bypass` / `immutable` / `revalidate` |
| Busy policy | Enum | — | `bypass` / `stale` |

### Instance-level

Every instance block supports these fields regardless of mode:

```yaml
instances:
  - name: my-instance
    enabled: true
    <mode>:           # exactly one mode block (oci / npm / go / ...)
      expire_after: 720h
      route: { path: /mount }
      transport:
        proxy: http://127.0.0.1:7890
        ua: custom-agent/1.0
        timeout: 10s
      # ... mode-specific fields below
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Instance name (metrics label, UI) |
| `enabled` | bool | required | `false` = skip at runtime |
| `<mode>` | block | required | One of `file`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman` |
| `expire_after` | expiration | `720h` | Upper bound on cached object lifetime |
| `route.path` | path | required* | Mount under `server.bind` (* `oci` uses `bind` instead) |
| `bind` | `host:port` | required* | Dedicated listener (* `oci` only) |
| `transport.proxy` | URL | — | Outbound HTTP proxy |
| `transport.ua` | string | `curl/8.10.0` | Override User-Agent |
| `transport.timeout` | duration | `3s` | Upstream dial timeout |

---

## Mode Reference

### `file` — generic HTTP

```yaml
file:
  route: { path: /files }
  upstreams:
    - https://example.com
  pass_headers: [X-Custom]
  default_policy: revalidate
  fresh_for: 5m
  busy_policy: stale
  rules:
    - match: "releases/**/*.zip"
      policy: immutable
      expire_after: 8760h
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstreams` | `[]string` | required | Upstream base URLs, tried in order |
| `pass_headers` | `[]string` | — | Request headers forwarded to upstream |
| `default_policy` | Policy | `bypass` | Default cache policy |
| `fresh_for` | Freshness | — | How long cached response is considered fresh |
| `busy_policy` | Busy | `bypass` | Serve stale while download in progress? |
| `rules[].match` | string | required | Doublestar path pattern |
| `rules[].policy` | Policy | — | Override policy for matched paths |
| `rules[].fresh_for` | Freshness | — | Override freshness for matched paths |
| `rules[].busy_policy` | Busy | — | Override busy policy for matched paths |
| `rules[].expire_after` | Expiration | — | Override expiration for matched paths |

### `oci` — Docker / OCI registries

```yaml
oci:
  bind: 127.0.0.1:5000
  display_url: https://cache.home.lan:5000
  upstream: https://registry-1.docker.io
  default_policy: bypass
  fresh_for: 10m
  busy_policy: stale
  auth:
    type: basic
    username: my-user
    password: my-pass
  rules:
    - match: "library/*"
      policy: immutable
      expire_after: 168h
```

Rules match repository names (doublestar). Client: `docker pull cache.lan:5000/library/alpine:latest`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Single upstream registry |
| `display_url` | URL | — | URL shown on home page (overrides auto-derived address) |
| `default_policy` | Policy | `bypass` | Default cache policy |
| `fresh_for` | Freshness | — | Freshness for cached manifests/blobs |
| `busy_policy` | Busy | `bypass` | Serve stale while download in progress? |
| `auth.type` | Enum | — | `none` \| `basic` \| `bearer` |
| `auth.username` | string | — | Required for `basic` |
| `auth.password` | string | — | Required for `basic` |
| `auth.token` | string | — | Required for `bearer` |
| `rules[].match` | string | required | Doublestar repo-name pattern |
| `rules[].policy` | Policy | `bypass` | Override policy for matched repos |
| `rules[].fresh_for` | Freshness | — | Override freshness for matched repos |
| `rules[].expire_after` | Expiration | — | Override expiration for matched repos |

### `npm`

```yaml
npm:
  route: { path: /npm }
  upstream: https://registry.npmjs.org
  metadata_policy: revalidate
  metadata_fresh_for: 1m
  metadata_busy_policy: stale
  tarball_policy: immutable
```

Client: `npm config set registry http://cache.lan:8080/npm`

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Single upstream registry |
| `metadata_policy` | Policy | `revalidate` | Cache policy for package metadata |
| `metadata_fresh_for` | Freshness | — | Freshness for metadata |
| `metadata_busy_policy` | Busy | `stale` | Busy policy for metadata |
| `tarball_policy` | Policy | `immutable` | Cache policy for `.tgz` files |

### `go` — GOPROXY + SumDB

```yaml
go:
  route: { path: /go }
  proxies:
    - https://proxy.golang.org
  module_policy: revalidate
  module_fresh_for: 1m
  module_busy_policy: stale
  zip_policy: immutable
  sumdb:
    enabled: true
    name: sum.golang.org
    url: https://sum.golang.org
  sumdb_fresh_for: 30s
  sumdb_busy_policy: stale
  goprivate:
    - "*.corp.example.com"
  disable_module_fetch_header: false
```

Client: `go env -w GOPROXY=http://cache.lan:8080/go`

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `proxies` | `[]URL` | required | GOPROXY chain (must be http/https) |
| `module_policy` | Policy | `revalidate` | Policy for `.info` / `.mod` / `@latest` / `@v/list` |
| `module_fresh_for` | Freshness | `1m` | Freshness for module metadata |
| `module_busy_policy` | Busy | `stale` | Busy policy for module metadata |
| `zip_policy` | Policy | `immutable` | Policy for `.zip` archives |
| `sumdb.enabled` | bool | `true` | Proxy checksum database requests |
| `sumdb.name` | string | `sum.golang.org` | SumDB name exposed in URL prefix |
| `sumdb.url` | URL | `https://sum.golang.org` | Upstream SumDB (http/https only) |
| `sumdb_fresh_for` | Freshness | `30s` | Freshness for SumDB responses |
| `sumdb_busy_policy` | Busy | `stale` | Busy policy for SumDB |
| `goprivate` | `[]string` | — | Patterns for private modules (skipped by proxy) |
| `disable_module_fetch_header` | bool | `false` | Honor `Disable-Module-Fetch` request header |

### `maven`

```yaml
maven:
  route: { path: /maven }
  upstream: https://repo1.maven.org/maven2
  release_policy: immutable
  snapshot_policy: revalidate
  snapshot_fresh_for: 5m
  checksum_policy: revalidate
  checksum_fresh_for: 30s
  checksum_busy_policy: stale
  metadata_fresh_for: 2m
  metadata_busy_policy: stale
```

Client: set `<mirror><url>http://cache.lan:8080/maven</url></mirror>` in `~/.m2/settings.xml`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Single upstream repository root |
| `release_policy` | Policy | `immutable` | Policy for release artifacts |
| `snapshot_policy` | Policy | `revalidate` | Policy for `-SNAPSHOT` artifacts |
| `snapshot_fresh_for` | Freshness | — | Freshness for snapshot artifacts |
| `checksum_policy` | Policy | `revalidate` | Policy for `.sha1` / `.md5` / `.asc` sidecars |
| `checksum_fresh_for` | Freshness | `30s` | Freshness for checksum sidecars |
| `checksum_busy_policy` | Busy | `stale` | Busy policy for checksum sidecars |
| `metadata_fresh_for` | Freshness | — | Freshness for `maven-metadata.xml` |
| `metadata_busy_policy` | Busy | `stale` | Busy policy for metadata |

### `cargo`

```yaml
cargo:
  route: { path: /cargo }
  upstream: https://index.crates.io
  crate_policy: immutable
  index_fresh_for: 5m
  index_busy_policy: stale
  auth_required: false
```

Client: `registry = "sparse+http://cache.lan:8080/cargo/"` in `.cargo/config.toml`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Single upstream sparse index |
| `crate_policy` | Policy | `immutable` | Policy for crate download files |
| `index_fresh_for` | Freshness | — | Freshness for index entries and `config.json` |
| `index_busy_policy` | Busy | `stale` | Busy policy for index |
| `auth_required` | bool | `false` | Set `auth-required: true` in rewritten `config.json` |

### `pypi`

```yaml
pypi:
  route: { path: /pypi }
  upstream: https://pypi.org
  index_policy: revalidate
  index_fresh_for: 1m
  index_busy_policy: stale
  file_policy: immutable
  companion_policy: revalidate
  companion_fresh_for: 30s
  companion_busy_policy: stale
  proxy_json: true
  proxy_core_metadata: false
  proxy_signatures: false
```

Client: `pip install --index-url http://cache.lan:8080/pypi/simple <pkg>`

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Single upstream PyPI base URL |
| `index_policy` | Policy | `revalidate` | Policy for `/simple/` index pages |
| `index_fresh_for` | Freshness | `1m` | Freshness for index pages |
| `index_busy_policy` | Busy | `stale` | Busy policy for index pages |
| `file_policy` | Policy | `immutable` | Policy for package files |
| `companion_policy` | Policy | `revalidate` | Policy for sidecar files (signatures, metadata) |
| `companion_fresh_for` | Freshness | `30s` | Freshness for sidecar files |
| `companion_busy_policy` | Busy | `stale` | Busy policy for sidecars |
| `proxy_json` | bool | `true` | Enable JSON Simple API (`/simple/<pkg>/json`) |
| `proxy_core_metadata` | bool | `false` | Proxy `.metadata` / `.json` / `.attestation` / `.provenance` as sidecars |
| `proxy_signatures` | bool | `false` | Proxy `.asc` / `.sig` / `.minisig` as sidecars |

### `apk` — Alpine

```yaml
apk:
  route: { path: /apk }
  upstreams:
    - https://dl-cdn.alpinelinux.org/alpine
  refresh_interval: 1h
  metadata_policy: revalidate
  metadata_fresh_for: 1m
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client repo line: `http://cache.lan:8080/apk/v3.20/main`. Roots auto-discovered from `APKINDEX.tar.gz` requests.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstreams` | `[]URL` | required | Upstream mirrors, tried in order |
| `refresh_interval` | duration | `1h` | Background metadata refresh cadence |
| `pass_headers` | `[]string` | — | Request headers forwarded to upstream |
| `metadata_policy` | Policy | `revalidate` | Policy for `APKINDEX.tar.gz` and repo metadata |
| `metadata_fresh_for` | Freshness | `1m` | Freshness for metadata |
| `metadata_busy_policy` | Busy | `stale` | Busy policy for metadata |
| `metadata_expire_after` | Expiration | — | Override expiration for metadata |
| `artifact_policy` | Policy | `immutable` | Policy for `.apk` files |
| `artifact_fresh_for` | Freshness | — | Freshness for artifacts |
| `artifact_busy_policy` | Busy | `bypass` | Busy policy for artifacts |
| `artifact_expire_after` | Expiration | — | Override expiration for artifacts |
| `auxiliary_policy` | Policy | `revalidate` | Policy for signatures / checksums |
| `auxiliary_fresh_for` | Freshness | `30s` | Freshness for auxiliary files |
| `auxiliary_busy_policy` | Busy | `stale` | Busy policy for auxiliary files |
| `auxiliary_expire_after` | Expiration | — | Override expiration for auxiliary files |

### `deb` — Debian / Ubuntu

```yaml
deb:
  route: { path: /deb }
  upstreams:
    - https://deb.debian.org/debian
  refresh_interval: 1h
  metadata_policy: revalidate
  metadata_fresh_for: 2m
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `deb http://cache.lan:8080/deb bookworm main`. Roots auto-discovered from `InRelease` / `Packages*` / `Sources*`.

Same field table as `apk`, except `metadata_fresh_for` defaults to `2m`.

### `rpm` — RHEL / Rocky / Fedora

```yaml
rpm:
  route: { path: /rpm }
  upstreams:
    - https://download.rockylinux.org/pub/rocky
  refresh_interval: 1h
  metadata_policy: revalidate
  metadata_fresh_for: 1m
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `baseurl=http://cache.lan:8080/rpm/9/BaseOS/x86_64/os`. Roots auto-discovered from `repomd.xml`.

Same field table as `apk`.

### `pacman` — Arch

```yaml
pacman:
  route: { path: /pacman }
  upstreams:
    - https://geo.mirror.pkgbuild.com
  refresh_interval: 2m
  metadata_policy: revalidate
  metadata_fresh_for: 1m
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `Server = http://cache.lan:8080/pacman/$repo/os/$arch`. Roots auto-discovered from `.db` / `.files` requests.

Same field table as `apk`, except `refresh_interval` defaults to `2m`.

### Package repository lifecycle

For `apk`, `deb`, `rpm`, `pacman`: configure `upstreams` and the proxy discovers roots from client metadata requests. Each root transitions through `pending` → `active` → `suspect` → `removed` states automatically:

- New metadata requests create roots in `pending`; first successful refresh promotes to `active`.
- Transient failures keep the last snapshot and mark as `suspect` (still serves stale).
- Repeated `404` responses remove the root entirely.
- Re-requesting metadata for a removed root recreates it automatically.

## Development

```bash
make fmt
make test
```

## Deployment Notes

- Run behind a TLS-terminating reverse proxy (nginx, Caddy, HAProxy) that sets `X-Forwarded-Proto` and `X-Forwarded-Host`.
- Store credentials in the YAML config. Keep it out of version control and protect with filesystem permissions.
- Set a long random `metrics.token` if `/metrics` is reachable beyond localhost.

## License

[MIT](LICENSE)
