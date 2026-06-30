# cache-proxy

A caching reverse proxy for package registries and artifact repositories. Single binary, single YAML config.

## Features

- 12 proxy modes in one process: `file`, `git`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman`
- Path-mounted and dedicated-listener instances
- Per-resource cache policies (`bypass` / `immutable` / `revalidate`) with freshness and concurrency controls
- Prometheus metrics, background GC, expired-object cleanup via unified scheduler
- Discovery-driven metadata refresh for Linux package repositories (`apk`, `deb`, `rpm`, `pacman`), with background-only refresh via event bus + scheduler

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
| `storage.cleanup.dry_run` | bool | `false` | Run scheduled cache cleanup in dry-run mode without deleting files |
| `storage.cleanup.batch_size` | int | `500` | Maximum objects deleted per cleanup batch |
| `storage.orphan_policy` | string | — | Home page orphan index cleanup policy (`auto` enables automatic orphan removal on startup) |
| `storage.download.max_active` | int | `64` | Global concurrent cache-fill downloads |
| `storage.download.max_active_per_instance` | int | `8` | Concurrent cache-fill downloads per instance |

### Value types

| Category | Format | Examples | Special values |
| --- | --- | --- | --- |
| Duration | Go duration | `30s`, `2m`, `1h`, `24h` | — |
| Expiration | Duration | `720h` | `never` |
| Freshness | Duration | `1m`, `30s` | `forever` |
| Policy | Enum | — | `bypass` / `immutable` / `revalidate` |
| Busy policy | Enum | — | `bypass` / `stale` |

### Shared mode fields

Most HTTP-backed mode blocks (`file`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman`) support these common fields. `git` has its own block shape and does not support `expire_after` or `transport`.

```yaml
instances:
  - name: my-instance
    enabled: true
    <mode>:           # exactly one mode block (file / oci / npm / go / ...)
      expire_after: 720h
      route: { path: /mount }
      transport:
        proxy: http://127.0.0.1:7890
        ua: custom-agent/1.0
        dial_timeout: 3s
        header_timeout: 30s
        idle_body_timeout: 5m
        max_request_duration: 30m
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
| `expire_after` | expiration | `720h` | Upper bound on cached object lifetime (not supported by `git`) |
| `route.path` | path | required* | Mount under `server.bind` (* `oci` uses `bind` instead) |
| `bind` | `host:port` | required* | Dedicated listener (* `oci` only) |
| `transport.proxy` | URL | — | Outbound HTTP proxy (not supported by `git`; use `git.proxy`) |
| `transport.ua` | string | per mode | Override User-Agent |
| `transport.dial_timeout` | duration | `3s` | TCP/TLS connection timeout |
| `transport.header_timeout` | duration | `30s` | Time allowed to receive upstream response headers |
| `transport.idle_body_timeout` | duration | `5m` | Maximum idle gap between upstream response body reads |
| `transport.max_request_duration` | duration | `30m` | Optional total upstream request duration cap |
| `transport.max_idle_conns` | int | transport default | Maximum idle upstream connections across all hosts |
| `transport.max_conns_per_host` | int | transport default | Maximum total upstream connections per host |
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

`transport.health` is a patch over the built-in defaults: unspecified fields keep their default values.

Storage writes are bounded internally to protect memory during concurrent large downloads.

---

### Safety semantics

- Direct `TargetURL` routes are checked centrally: the target must use `http`/`https` and its host must match configured upstream hosts or a route-scoped allowlist.
- Upstream failover retries network errors, `429`, and `5xx`; `404`/`403` are returned as-is.
- Known SHA256/digest objects are streamed to clients immediately, but mismatched content is not written to cache.
- OCI manifests with `Docker-Content-Digest` and OCI blobs requested by digest are verified before cache state/object writes.
- Reload was removed; restart the process to pick up configuration changes.

### Background tasks

All periodic maintenance runs through a unified single-threaded scheduler:

- **Blob GC**: system-level storage garbage collection
- **Expire cleanup**: per-instance cache expiration (all proxy modes)
- **Metadata refresh**: background download + generation publish for Linux repo modes
- **Metadata GC**: old generation cleanup for Linux repo modes

Tasks are registered during startup (Plan phase). Metadata refresh tasks are dynamically registered when a new repository is discovered via client metadata requests. The scheduler persists task state to `_scheduler/tasks.yaml` and restores it on restart.
- Linux repository metadata is served only from a validated snapshot. If a repository has not finished its first refresh, metadata requests bypass directly to upstream while triggering a background refresh task via the event bus.

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

Behavior:
- Metadata is served only from the current validated generation.
- Artifact and auxiliary downloads stay ordinary proxy/cache requests.
- Cleanup uses a short-lived sorted path set rebuilt from the current generation when needed; no long-lived package index is persisted.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `upstreams` | `[]URL` | required | Upstream mirrors; each metadata generation uses exactly one upstream, with root-level failover |
| `refresh_interval` | duration | `1h` | Background generation refresh interval |
| `cleanup_interval` | duration | `6h` | Interval for indexed cache cleanup task |
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

Client: `deb http://cache.lan:8080/deb bookworm main`. Repositories are discovered from `Release` / `InRelease`; package and source indexes are derived from the Release checksum section and refreshed in the background.

A Debian snapshot is published only after at least one `Packages*` or `Sources*` index is fetched and validated. Before the first snapshot, metadata requests bypass directly to upstream and trigger background refresh.

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

Same field table as `apk`, except `refresh_interval` defaults to `2m`. `.db` is required for a published generation; `.db.sig`, `.files`, and `.files.sig` are cached when available.

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
| `operation_timeout` | Duration | `0` | Per clone/fetch operation timeout (`0` = no extra timeout) |
| `force_overwrite` | bool | `true` | Force overwrite local refs on incompatible remote changes |
| `route.path` | path | required | URL prefix for git clone |

Notes:
- One remote repository per instance. Read-only mirror (no push support).
- Configurable `sync_interval` keeps the local mirror in sync with the remote; set to `0` for manual-only.
- With `force_overwrite: true` and `sync_interval > 0`, the mirror stays byte-identical to the remote including force-pushed history and deleted branches (prune).
- Clone status and sync results are exposed as Prometheus metrics (`cache_proxy_git_clone_success_total`, `cache_proxy_git_sync_success_total`, `cache_proxy_git_last_sync_timestamp_seconds`).

</details>

### Package repository lifecycle

For `apk`, `deb`, `rpm`, `pacman`: repositories are discovered automatically from client metadata requests.

- Metadata is refreshed in the background by the unified scheduler and served only after validation.
- If no validated snapshot is available yet, metadata requests bypass directly to upstream while a background refresh is triggered.
- Refresh failures keep serving the last valid snapshot when one exists.
- Package and auxiliary downloads remain best-effort reverse proxy/cache requests and do not depend on a package index hit.
- Cleanup removes cached indexed objects by rebuilding a sorted current-generation path set from local metadata when needed.
- Re-requesting metadata for a removed repository recreates it automatically.

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
