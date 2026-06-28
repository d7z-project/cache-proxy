# cache-proxy

A caching reverse proxy for package registries and artifact repositories. Single binary, single YAML config.

## Features

- 12 proxy modes in one process: `file`, `git`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman`
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
| `server.backend` | path | `/tmp/cache-proxy` | Storage directory |
| `server.public_url` | URL | — | Override scheme+host shown on home page (e.g. `https://cache.home.lan`) |
| `metrics.path` | path | `/metrics` | Prometheus endpoint |
| `metrics.token` | string | — | Bearer token for `/metrics` |
| `storage.gc.blob` | duration | `24h` | Storage GC interval |
| `storage.cleanup.enabled` | bool | `false` | Enable stale-object cleanup |
| `storage.cleanup.interval` | duration | `6h` | Cleanup scan interval |
| `storage.cleanup.dry_run` | bool | `false` | Log deletions without removing |
| `storage.cleanup.batch_size` | int | `500` | Max objects per batch |
| `storage.cleanup.workers` | int | `0` | Concurrency (`0` = auto) |

### Value types

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
        health:
          enabled: true
          degrade_rate: 0.1
          trip_rate: 0.3
          evaluation_window: 2m
          degrade_latency: 2s
          min_weight: 0.1
          canary_cooldown: 30s
          canary_step: 0.1
          probe_interval: 2m
          probe_timeout: 5s
          resource_block_interval: 2m
          resource_remove_age: 5m
          resource_remove_count: 5
      # ... mode-specific fields below
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Instance name (metrics label, home page) |
| `enabled` | bool | required | `false` = skip at runtime |
| `<mode>` | block | required | One of `file`, `git`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman` |
| `expire_after` | expiration | `720h` | Upper bound on cached object lifetime |
| `route.path` | path | required* | Mount under `server.bind` (* `oci` uses `bind` instead) |
| `bind` | `host:port` | required* | Dedicated listener (* `oci` only) |
| `transport.proxy` | URL | — | Outbound HTTP proxy |
| `transport.ua` | string | per mode | Override User-Agent |
| `transport.timeout` | duration | `3s` | Upstream TCP dial timeout |
| `transport.health.enabled` | bool | `true` | Enable active health monitoring and failover |
| `transport.health.degrade_rate` | float | `0.1` | Error rate threshold to reduce upstream traffic |
| `transport.health.trip_rate` | float | `0.3` | Error rate threshold to stop all traffic to upstream |
| `transport.health.evaluation_window` | duration | `2m` | Time window for error rate calculation |
| `transport.health.degrade_latency` | duration | `2s` | Latency threshold to reduce upstream traffic |
| `transport.health.min_weight` | float | `0.1` | Minimum traffic weight for a degraded upstream |
| `transport.health.canary_cooldown` | duration | `30s` | Cooldown before testing a tripped upstream |
| `transport.health.canary_step` | float | `0.1` | Traffic weight increment per successful health check |
| `transport.health.probe_interval` | duration | `2m` | Health check interval for healthy upstreams |
| `transport.health.probe_timeout` | duration | `5s` | Health check request timeout |
| `transport.health.resource_block_interval` | duration | `2m` | Blocked repo retry interval (filerepo modes) |
| `transport.health.resource_remove_age` | duration | `5m` | Minimum age before removing a missing repo |
| `transport.health.resource_remove_count` | int | `5` | Consecutive missed updates to remove a repo |

---

### Safety semantics

- Direct `TargetURL` routes are checked centrally: the target must use `http`/`https` and its host must match configured upstream hosts or a route-scoped allowlist.
- Upstream failover retries network errors, `429`, and `5xx`; `404`/`403` are returned as-is.
- Known SHA256/digest objects are streamed to clients immediately, but mismatched content is not written to cache.
- OCI manifests with `Docker-Content-Digest` and OCI blobs requested by digest are verified before cache state/object writes.

---

## Mode Reference

<details open>
<summary><b>file</b> — generic HTTP</summary>

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
| `upstreams` | `[]URL` | required | Upstream base URLs, tried in order |
| `pass_headers` | `[]string` | — | Request headers forwarded to upstream |
| `default_policy` | Policy | `bypass` | Default cache policy |
| `fresh_for` | Freshness | — | Cached response freshness |
| `busy_policy` | Busy | `bypass` | Behavior while another request is already downloading |
| `rules[].match` | glob | required | Path pattern |
| `rules[].policy` | Policy | — | Override policy for matched paths |
| `rules[].fresh_for` | Freshness | — | Override freshness for matched paths |
| `rules[].busy_policy` | Busy | — | Override busy policy for matched paths |
| `rules[].expire_after` | Expiration | — | Override expiration for matched paths |

</details>

<details open>
<summary><b>oci</b> — Docker / OCI registries</summary>

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

Client: `docker pull cache.lan:5000/library/alpine:latest`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Upstream registry |
| `display_url` | URL | — | Home page URL (overrides auto-detected address) |
| `default_policy` | Policy | `bypass` | Default cache policy |
| `fresh_for` | Freshness | — | Freshness for cached manifests and layers |
| `busy_policy` | Busy | `bypass` | Behavior while another request is downloading |
| `auth.type` | Enum | — | `none` \| `basic` \| `bearer` |
| `auth.username` | string | — | Required for `basic` |
| `auth.password` | string | — | Required for `basic` |
| `auth.token` | string | — | Required for `bearer` |
| `rules[].match` | glob | required | Repository name pattern |
| `rules[].policy` | Policy | `bypass` | Override policy for matched repos |
| `rules[].expire_after` | Expiration | — | Override expiration for matched repos |

</details>

<details>
<summary><b>npm</b></summary>

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
| `upstream` | URL | required | Upstream registry |
| `metadata_policy` | Policy | `revalidate` | Cache policy for package metadata |
| `metadata_fresh_for` | Freshness | — | Freshness for metadata |
| `metadata_busy_policy` | Busy | `stale` | Busy policy for metadata |
| `tarball_policy` | Policy | `immutable` | Cache policy for `.tgz` files |

</details>

<details>
<summary><b>go</b> — GOPROXY + SumDB</summary>

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
  sumdb_busy_policy: bypass
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
| `sumdb.name` | string | `sum.golang.org` | SumDB name in URL prefix |
| `sumdb.url` | URL | `https://sum.golang.org` | Upstream SumDB |
| `sumdb_fresh_for` | Freshness | `30s` | Freshness for SumDB responses |
| `sumdb_busy_policy` | Busy | `bypass` | Busy policy for SumDB |
| `goprivate` | `[]glob` | — | Patterns for private modules (skipped by proxy) |
| `disable_module_fetch_header` | bool | `false` | Honor `Disable-Module-Fetch` request header |

</details>

<details>
<summary><b>maven</b></summary>

```yaml
maven:
  route: { path: /maven }
  upstream: https://repo1.maven.org/maven2
  release_policy: immutable
  snapshot_policy: revalidate
  snapshot_fresh_for: 5m
  checksum_policy: revalidate
  checksum_fresh_for: 30s
  checksum_busy_policy: bypass
  metadata_fresh_for: 2m
  metadata_busy_policy: stale
```

Client: set `<mirror><url>http://cache.lan:8080/maven</url></mirror>` in `~/.m2/settings.xml`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Upstream repository root |
| `release_policy` | Policy | `immutable` | Policy for release artifacts |
| `snapshot_policy` | Policy | `revalidate` | Policy for `-SNAPSHOT` artifacts |
| `snapshot_fresh_for` | Freshness | — | Freshness for snapshot artifacts |
| `checksum_policy` | Policy | `revalidate` | Policy for `.sha1` / `.md5` / `.asc` sidecars |
| `checksum_fresh_for` | Freshness | `30s` | Freshness for checksum sidecars |
| `checksum_busy_policy` | Busy | `bypass` | Busy policy for checksum sidecars |
| `metadata_fresh_for` | Freshness | — | Freshness for `maven-metadata.xml` |
| `metadata_busy_policy` | Busy | `stale` | Busy policy for metadata |

</details>

<details>
<summary><b>cargo</b></summary>

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
| `upstream` | URL | required | Upstream sparse index |
| `crate_policy` | Policy | `immutable` | Policy for crate download files |
| `index_fresh_for` | Freshness | — | Freshness for index entries and `config.json` |
| `index_busy_policy` | Busy | `stale` | Busy policy for index |
| `auth_required` | bool | `false` | Set `auth-required: true` in returned `config.json` |

</details>

<details>
<summary><b>pypi</b></summary>

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
  companion_busy_policy: bypass
  proxy_json: true
  proxy_core_metadata: false
  proxy_signatures: false
```

Client: `pip install --index-url http://cache.lan:8080/pypi/simple <pkg>`

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Upstream PyPI base URL |
| `index_policy` | Policy | `revalidate` | Policy for `/simple/` index pages |
| `index_fresh_for` | Freshness | `1m` | Freshness for index pages |
| `index_busy_policy` | Busy | `stale` | Busy policy for index pages |
| `file_policy` | Policy | `immutable` | Policy for package files |
| `companion_policy` | Policy | `revalidate` | Policy for sidecar files |
| `companion_fresh_for` | Freshness | `30s` | Freshness for sidecar files |
| `companion_busy_policy` | Busy | `bypass` | Busy policy for sidecars |
| `proxy_json` | bool | `true` | Enable JSON API (`/simple/<pkg>/json`) |
| `proxy_core_metadata` | bool | `false` | Proxy core metadata files as sidecars |
| `proxy_signatures` | bool | `false` | Proxy signature files as sidecars |

</details>

<details>
<summary><b>apk</b> — Alpine</summary>

```yaml
apk:
  route: { path: /apk }
  upstreams:
    - https://dl-cdn.alpinelinux.org/alpine
  refresh_interval: 1h
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `http://cache.lan:8080/apk/v3.20/main`. Repositories are auto-discovered from `APKINDEX.tar.gz` requests.

Linux repository metadata is refreshed as an upstream-affine generation: all metadata, signatures, and index files for one repository root are fetched from the same upstream and published atomically only after required files and checksums validate. Clients see either the previous complete generation or the next complete generation, never a mixed set of files from different mirrors.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstreams` | `[]URL` | required | Upstream mirrors; each metadata generation uses exactly one upstream, with root-level failover |
| `refresh_interval` | duration | `1h` | Background generation refresh interval |
| `pass_headers` | `[]string` | — | Request headers forwarded to upstream |
| `artifact_policy` | Policy | `immutable` | Policy for `.apk` files |
| `artifact_fresh_for` | Freshness | — | Freshness for artifacts |
| `artifact_busy_policy` | Busy | `bypass` | Busy policy for artifacts |
| `artifact_expire_after` | Expiration | — | Override expiration for artifacts |
| `auxiliary_policy` | Policy | `revalidate` | Policy for signatures and checksums |
| `auxiliary_fresh_for` | Freshness | `30s` | Freshness for auxiliary files |
| `auxiliary_busy_policy` | Busy | `bypass` | Busy policy for auxiliary files |
| `auxiliary_expire_after` | Expiration | — | Override expiration for auxiliary files |

</details>

<details>
<summary><b>deb</b> — Debian / Ubuntu</summary>

```yaml
deb:
  route: { path: /deb }
  upstreams:
    - https://deb.debian.org/debian
  refresh_interval: 1h
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `deb http://cache.lan:8080/deb bookworm main`. Repositories auto-discovered from `InRelease` / `Packages*` / `Sources*`.

Same field table as `apk`.

</details>

<details>
<summary><b>rpm</b> — RHEL / Rocky / Fedora</summary>

```yaml
rpm:
  route: { path: /rpm }
  upstreams:
    - https://download.rockylinux.org/pub/rocky
  refresh_interval: 1h
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `baseurl=http://cache.lan:8080/rpm/9/BaseOS/x86_64/os`. Repositories auto-discovered from `repomd.xml`.

Same field table as `apk`.

</details>

<details>
<summary><b>pacman</b> — Arch</summary>

```yaml
pacman:
  route: { path: /pacman }
  upstreams:
    - https://geo.mirror.pkgbuild.com
  refresh_interval: 2m
  artifact_policy: immutable
  auxiliary_policy: revalidate
```

Client: `Server = http://cache.lan:8080/pacman/$repo/os/$arch`. Repositories auto-discovered from `.db` / `.files` requests.

Same field table as `apk`, except `refresh_interval` defaults to `2m`. Pacman strict metadata requires `.db`, `.db.sig`, `.files`, and `.files.sig` for a published generation.

</details>

<details>
<summary><b>git</b> — Git repository mirror</summary>

```yaml
git:
  route: { path: /git }
  upstream: https://github.com/user/repo.git
  auth:
    type: token
    password: $GITHUB_TOKEN
  proxy: socks5://proxy:1080
  sync_interval: 5m
  force_overwrite: true
```

Client: `git clone http://cache.lan:8080/git/`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstream` | URL | required | Remote git repository URL |
| `auth.type` | Enum | — | `none` \| `basic` \| `token` |
| `auth.username` | string | — | Username for `basic` auth |
| `auth.password` | string | — | Password/token (supports `$ENV` expansion) |
| `proxy` | URL | — | HTTP/SOCKS5 proxy for upstream connection |
| `sync_interval` | Duration | `0` | Periodic sync interval (`0` = only initial clone) |
| `force_overwrite` | bool | `true` | Force overwrite local refs on incompatible remote changes |
| `route.path` | path | required | URL prefix for git clone |

Notes:
- One remote repository per instance. Read-only mirror (no push support).
- Configurable `sync_interval` keeps the local mirror in sync with the remote; set to `0` for manual-only.
- With `force_overwrite: true` and `sync_interval > 0`, the mirror stays byte-identical to the remote including force-pushed history and deleted branches (prune).
- Repository storage is backed by the configured `server.backend` via the internal afero filesystem layer.
- Clone status and sync results are exposed as Prometheus metrics (`cache_proxy_git_clone_success_total`, `cache_proxy_git_sync_success_total`, `cache_proxy_git_last_sync_timestamp_seconds`).

</details>

### Package repository lifecycle

For `apk`, `deb`, `rpm`, `pacman`: repositories are discovered automatically from client metadata requests and tracked through a generation lifecycle:

- New metadata requests create repositories in `pending`; first successful refresh promotes to `active`.
- Each refresh chooses one upstream for the whole root. If that upstream is incomplete or fails validation, the entire staging generation is discarded and the next upstream is tried from the beginning.
- Required signatures and checksums are part of the generation. Missing or mismatched metadata does not publish.
- Transient failures keep serving the last published generation (marked `suspect`).
- Repeated missed updates remove the repository entirely.
- Re-requesting metadata for a removed repo recreates it automatically.

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
