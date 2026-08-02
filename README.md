# cache-proxy

A caching reverse proxy for package registries and artifact repositories. Single binary, single YAML config.

## Features

- 13 proxy modes in one process: `file`, `git`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `flatpak`, `apk`, `deb`, `rpm`, `pacman`
- Path-mounted and dedicated-listener instances
- Per-resource cache policies: `bypass`, `immutable`, `revalidate`
- Background blob GC and expired-object cleanup
- Background metadata refresh for Flatpak/OSTree and Linux repositories (`flatpak`, `apk`, `deb`, `rpm`, `pacman`)
- Prometheus metrics and built-in home page
- Built-in server status modal with persisted disk history and recent scheduler/upstream events

## Quick Start

```bash
make cache-proxy
./cache-proxy -config ./cache-proxy.yaml
./cache-proxy -config ./cache-proxy.yaml -validate
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

## Configuration

Top-level fields:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `server.bind` | `host:port` | `127.0.0.1:18080` | Main HTTP listener |
| `server.backend` | path | `/tmp/cache-proxy` | Storage directory |
| `server.public_url` | URL | — | Public base URL shown on the home page |
| `server.status.disk_sample_interval` | duration | `15m` | Disk usage sampling interval for the home page status modal |
| `server.status.disk_history_window` | duration | `24h` | Persisted disk history retention window for the home page status modal |
| `server.status.event_limit` | int | `500` | Persisted scheduler/upstream event retention limit for the home page status modal |
| `metrics.path` | path | `/metrics` | Prometheus endpoint |
| `metrics.token` | string | — | Optional bearer token for `/metrics` |
| `storage.gc.blob` | duration | `24h` | Blob storage GC interval |
| `storage.cleanup.dry_run` | bool | `false` | Run scheduled cleanup without deleting files |
| `storage.cleanup.batch_size` | int | `500` | Maximum deletions per cleanup batch |
| `storage.orphan_policy` | string | — | Home page orphan cleanup policy (`auto`) |
| `storage.download.max_active` | int | `256` | Process-wide concurrent upstream response bodies |
| `storage.download.max_active_per_host` | int | `4` | Concurrent upstream transfers to one normalized host across all instances |
| `storage.download.request_interval_per_host` | duration | `125ms` | Minimum interval between request starts to one normalized host |
| `storage.download.foreground_queue_wait` | duration | `3s` | Maximum time a client request waits for upstream admission |
| `storage.download.hosts.<host>.max_active` | int | global host limit | Exact-host concurrent transfer override |
| `storage.download.hosts.<host>.request_interval` | duration | global interval | Exact-host request interval override; `0s` disables proactive pacing |

The upstream gate is process-wide and shared by metadata, artifacts, repository refreshes, health probes, OCI token requests, and bypass requests. It limits active response bodies and spaces request starts per host, which prevents npm/Maven-style small-file bursts without imposing a fixed burst size on multi-file repository refreshes. Client requests have a bounded queue wait, repository refreshes wait within their scheduler task context, and probes skip immediately when capacity or pacing is unavailable. Only an actual upstream `429` activates a host-wide cooldown; `Retry-After` is honored and that refresh does not fan out to another mirror.

An exact-host override is useful when a deliberately short freshness policy targets a private upstream that is known to tolerate a higher request rate:

```yaml
storage:
  download:
    max_active: 256
    max_active_per_host: 4
    request_interval_per_host: 125ms
    foreground_queue_wait: 3s
    hosts:
      packages.d7z.net:
        max_active: 32
        request_interval: 0s
```

Value types:

| Type | Examples | Special values |
| --- | --- | --- |
| `duration` | `30s`, `5m`, `24h` | — |
| `expiration` | `720h` | `never` |
| `freshness` | `30s`, `5m` | `forever` |

Busy policies are `join` (subscribe to the active cache fill), `stale` (serve a cached object immediately, otherwise join), and `bypass` (open an independent upstream request). Package artifacts default to `join`.

Shared instance shape:

```yaml
instances:
  - name: example
    enabled: true
    <mode>:
      route: { path: /mount }
      expire_after: 720h
      transport:
        proxy: http://127.0.0.1:7890
        ua: custom-agent/1.0
```

Notes:

- Each instance must define exactly one mode block.
- Most modes use `route.path`; `oci` uses `bind`.
- `git` has its own block shape and does not use `expire_after` or `transport`.
- Browser `User-Agent` values are forwarded on foreground upstream requests. Other clients and internal refresh,
  health-probe, and OCI token requests use `cache-proxy/1`; `transport.ua` overrides all of these behaviors for an instance.
- Without `transport.ua`, responses declaring `Vary: User-Agent` or `Vary: *` are not stored, preventing
  default-UA and browser-specific content from sharing a cache entry. Browsers refresh legacy entries once
  when those entries predate User-Agent variance tracking.
- `transport.health` exists for upstream health tuning; active probes default to `probe_interval: 2m`,
  reject intervals below `30s`, and are shared by upstream host to avoid bursty checks.
- Active health probes use discovered Linux repository metadata targets; upstream roots without metadata targets
  are not probed actively.
- The built-in home page fetches status data from `/-/status/summary`, `/-/status/disk`, and `/-/status/events`.
- Linux repository modes expose discovered repository roots on the home page, including the root path, primary metadata paths, refresh state, and mode-specific attributes.
- Status history is persisted in bounded form and trimmed by `server.status.disk_history_window` and `server.status.event_limit`.

## Mode Overview

| Mode | Typical use | Key fields |
| --- | --- | --- |
| `file` | Generic HTTP file cache | `upstreams`, `default_policy`, `rules[]` |
| `oci` | Docker / OCI registry cache | `bind`, `upstream`, `auth`, `rules[]` |
| `npm` | npm registry mirror | `upstream`, `metadata_*`, `tarball_policy` |
| `go` | GOPROXY + SumDB | `proxies`, `module_*`, `zip_policy`, `sumdb` |
| `maven` | Maven repository cache | `upstream`, `release_policy`, `snapshot_*`, `checksum_*`, `metadata_*` |
| `cargo` | crates.io sparse index cache | `upstream`, `crate_policy`, `index_*` |
| `pypi` | PyPI simple index + files | `upstream`, `index_*`, `file_policy`, `companion_*` |
| `flatpak` | Flatpak / OSTree repository cache | `upstreams`, `refresh_interval`, `descriptor_rewrite`, `verify_*` |
| `apk` | Alpine repositories | `upstreams`, `refresh_interval`, `cleanup_interval`, `artifact_*`, `auxiliary_*` |
| `deb` | Debian / Ubuntu repositories | `upstreams`, `refresh_interval`, `cleanup_interval`, `artifact_*`, `auxiliary_*` |
| `rpm` | RPM repositories | `upstreams`, `refresh_interval`, `cleanup_interval`, `artifact_*`, `auxiliary_*` |
| `pacman` | Arch repositories | `upstreams`, `refresh_interval`, `cleanup_interval`, `artifact_*`, `auxiliary_*` |
| `git` | Read-only Git mirror | `upstream`, `auth`, `proxy`, `sync_interval` |

Client examples:

- npm: `npm config set registry http://cache.lan:8080/npm`
- Go: `go env -w GOPROXY=http://cache.lan:8080/go`
- Cargo: `registry = "sparse+http://cache.lan:8080/cargo/"`
- PyPI: `pip install --index-url http://cache.lan:8080/pypi/simple <pkg>`
- Flatpak: `flatpak remote-add --if-not-exists flathub http://cache.lan:8080/flathub/flathub.flatpakrepo`
- APK: `/etc/apk/repositories` entry `http://cache.lan:8080/apk`
- Debian distribution repo: `deb http://cache.lan:8080/deb bookworm main`
- Debian flat repo: `deb [trusted=yes] http://cache.lan:8080/deb ./`
- RPM: `baseurl=http://cache.lan:8080/rpm`
- Pacman: `Server = http://cache.lan:8080/pacman`
- OCI: `docker pull cache.lan:5000/library/alpine:latest`

## Mode Reference

<details>
<summary><b>file</b> - generic HTTP file cache</summary>

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

Use this mode for ordinary HTTP content where different path groups may need different cache policies.

Revalidation uses one conditional `GET`. A `304` advances the cached object's freshness without rewriting its body; a changed response is streamed once to all concurrent callers. During a cacheable fill, same-object followers join the active stream instead of opening duplicate upstream requests. When stale content exists, upstream rate limiting and transient validation failures serve that stale object.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstreams` | `[]URL` | required | Upstream base URLs, tried in order |
| `pass_headers` | `[]string` | — | Request headers forwarded upstream; use `transport.ua` for `User-Agent` |
| `default_policy` | policy | `bypass` | Default cache policy |
| `fresh_for` | freshness | — | Freshness for cached responses |
| `busy_policy` | busy policy | `bypass` | Behavior while another request is already downloading |
| `rules[].match` | glob | required | Path pattern |
| `rules[].policy` | policy | — | Policy override |
| `rules[].fresh_for` | freshness | — | Freshness override |
| `rules[].busy_policy` | busy policy | — | Busy policy override |
| `rules[].expire_after` | expiration | — | Expiration override |

</details>

<details>
<summary><b>oci</b> - Docker / OCI registry cache</summary>

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

Use this mode for a dedicated registry listener. Clients point Docker or other OCI tooling at the bound address. Cache publication is serialized per repository reference so a manifest and its persisted state are updated together. Canonical SHA256 blobs are verified and stored by digest independently from mutable tag state, so the same blob is reused across tags and repositories.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bind` | `host:port` | required | Dedicated listener |
| `display_url` | URL | — | Home page URL override |
| `upstream` | URL | required | Upstream registry |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `default_policy` | policy | `bypass` | Default cache policy |
| `fresh_for` | freshness | — | Freshness for cached manifests |
| `busy_policy` | busy policy | `join` | Busy policy while downloading |
| `auth.type` | enum | — | `none`, `basic`, `bearer` |
| `auth.username` | string | — | Required for `basic` |
| `auth.password` | string | — | Required for `basic` |
| `auth.token` | string | — | Required for `bearer` |
| `rules[].match` | glob | required | Repository pattern |
| `rules[].policy` | policy | `bypass` | Policy override |
| `rules[].expire_after` | expiration | — | Expiration override |

</details>

<details>
<summary><b>npm</b> - npm registry mirror</summary>

```yaml
npm:
  route: { path: /npm }
  upstream: https://registry.npmjs.org
  metadata_policy: revalidate
  metadata_fresh_for: 1m
  metadata_busy_policy: stale
  tarball_policy: immutable
```

Use this mode for npm metadata and package tarballs behind a single prefix. Concurrent cold requests for the same metadata or tarball join one cache fill; stale metadata is served immediately when available, otherwise followers wait for the bounded fill instead of opening duplicate upstream requests. Metadata tarball URLs are rewritten consistently for cold, refreshed, and cached responses. The npm abbreviated metadata media type is forwarded upstream and stored as a separate cache representation.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstream` | URL | required | Upstream registry |
| `metadata_policy` | policy | `revalidate` | Policy for package metadata |
| `metadata_fresh_for` | freshness | `1m` | Freshness for metadata |
| `metadata_busy_policy` | busy policy | `stale` | Busy policy for metadata |
| `tarball_policy` | policy | `immutable` | Policy for tarballs |

</details>

<details>
<summary><b>go</b> - GOPROXY + SumDB</summary>

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
  sumdb_busy_policy: join
  goprivate:
    - "*.corp.example.com"
```

Use this mode to proxy public module traffic while allowing selected private module prefixes to bypass the proxy.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `proxies` | `[]URL` | required | GOPROXY chain |
| `module_policy` | policy | `revalidate` | Policy for mutable `@latest` and `@v/list` endpoints |
| `module_fresh_for` | freshness | `1m` | Freshness for module metadata |
| `module_busy_policy` | busy policy | `stale` | Busy policy for module metadata |
| `zip_policy` | policy | `immutable` | Policy for versioned `.info`, `.mod`, and `.zip` files |
| `sumdb.enabled` | bool | `true` | Enable SumDB proxying |
| `sumdb.name` | string | `sum.golang.org` | SumDB name in request path |
| `sumdb.url` | URL | `https://sum.golang.org` | Upstream SumDB |
| `sumdb_fresh_for` | freshness | `30s` | Freshness for SumDB responses |
| `sumdb_busy_policy` | busy policy | `join` | Busy policy for SumDB |
| `goprivate` | `[]glob` | — | Private module patterns that bypass proxying |
| `disable_module_fetch_header` | bool | `false` | Honor `Disable-Module-Fetch` request header |

</details>

<details>
<summary><b>maven</b> - Maven repository cache</summary>

```yaml
maven:
  route: { path: /maven }
  upstream: https://repo1.maven.org/maven2
  release_policy: immutable
  snapshot_policy: revalidate
  snapshot_fresh_for: 5m
  checksum_policy: revalidate
  checksum_fresh_for: 30s
  checksum_busy_policy: join
  metadata_fresh_for: 2m
  metadata_busy_policy: stale
```

Use this mode for Maven Central or an internal Maven repository. Release artifacts and their checksum/signature sidecars follow `release_policy`; snapshot sidecars follow `snapshot_policy`; only `maven-metadata.xml` sidecars use `checksum_policy`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstream` | URL | required | Upstream repository root |
| `release_policy` | policy | `immutable` | Policy for release artifacts |
| `snapshot_policy` | policy | `revalidate` | Policy for snapshot artifacts |
| `snapshot_fresh_for` | freshness | — | Freshness for snapshot artifacts |
| `metadata_fresh_for` | freshness | — | Freshness for `maven-metadata.xml` |
| `metadata_busy_policy` | busy policy | `stale` | Busy policy for metadata |
| `checksum_policy` | policy | `revalidate` | Policy for `maven-metadata.xml` checksum/signature sidecars |
| `checksum_fresh_for` | freshness | `30s` | Freshness for checksum sidecars |
| `checksum_busy_policy` | busy policy | `join` | Busy policy for checksum sidecars |

</details>

<details>
<summary><b>cargo</b> - crates.io sparse index cache</summary>

```yaml
cargo:
  route: { path: /cargo }
  upstream: https://index.crates.io
  crate_policy: immutable
  index_fresh_for: 5m
  index_busy_policy: stale
  auth_required: false
```

Use this mode for Cargo sparse index traffic and crate downloads.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstream` | URL | required | Upstream sparse index |
| `crate_policy` | policy | `immutable` | Policy for crate files |
| `index_fresh_for` | freshness | — | Freshness for sparse index entries |
| `index_busy_policy` | busy policy | `stale` | Busy policy for sparse index entries |
| `auth_required` | bool | `false` | Return `auth-required: true` in generated config |

</details>

<details>
<summary><b>pypi</b> - PyPI simple index and files</summary>

```yaml
pypi:
  route: { path: /pypi }
  upstream: https://pypi.org
  index_policy: revalidate
  index_fresh_for: 1m
  index_busy_policy: stale
  file_policy: immutable
  companion_policy: immutable
  companion_fresh_for: 30s
  companion_busy_policy: join
  proxy_json: true
  proxy_core_metadata: false
  proxy_signatures: false
```

Use this mode for `/simple/` indexes and package file downloads, with optional sidecar proxying.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstream` | URL | required | Upstream PyPI base URL |
| `index_policy` | policy | `revalidate` | Policy for simple index pages |
| `index_fresh_for` | freshness | `1m` | Freshness for simple index pages |
| `index_busy_policy` | busy policy | `stale` | Busy policy for index pages |
| `file_policy` | policy | `immutable` | Policy for package files |
| `companion_policy` | policy | `immutable` | Policy for versioned sidecar files |
| `companion_fresh_for` | freshness | `30s` | Freshness for sidecars |
| `companion_busy_policy` | busy policy | `join` | Busy policy for sidecars |
| `proxy_json` | bool | `true` | Enable `/simple/<pkg>/json` |
| `proxy_core_metadata` | bool | `false` | Proxy metadata sidecars |
| `proxy_signatures` | bool | `false` | Proxy signature sidecars |

</details>

<details>
<summary><b>flatpak</b> - Flatpak / OSTree repository cache</summary>

```yaml
flatpak:
  route: { path: /flathub }
  upstreams:
    - https://dl.flathub.org/repo
  refresh_interval: 5m
  cleanup_interval: 6h
  descriptor_rewrite: true
  verify_objects: true
  cache_deltas: true
  delta_expire_after: 720h
```

Use this mode for Flatpak repositories backed by OSTree. `summary`, `summary.sig`, and `config` are refreshed as metadata generations from one upstream. OSTree objects are cached outside generations and verified before immutable cache writes. Static deltas are cached as opaque immutable files by path and are validated by Flatpak/OSTree clients when applied.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstreams` | `[]URL` | required | Upstream OSTree repository URLs |
| `refresh_interval` | duration | `5m` | Background `summary` refresh interval |
| `cleanup_interval` | duration | `6h` | Expired-object cleanup interval |
| `metadata_fresh_for` | freshness | `1m` | Freshness for metadata fallback cache |
| `metadata_busy_policy` | busy policy | `stale` | Busy policy for metadata fallback cache |
| `descriptor_rewrite` | bool | `true` | Rewrite `.flatpakrepo` / `.flatpakref` URLs to the proxy |
| `verify_objects` | bool | `true` | Verify OSTree objects before immutable cache writes |
| `cache_deltas` | bool | `true` | Cache `deltas/**` as opaque immutable files |
| `delta_expire_after` | expiration | inherits `expire_after` | Maximum delta object lifetime; `never` is rejected when delta caching is enabled |

</details>

<details>
<summary><b>apk</b> - Alpine repository cache</summary>

```yaml
apk:
  route: { path: /apk }
  upstreams:
    - https://dl-cdn.alpinelinux.org/alpine
  refresh_interval: 1h
  cleanup_interval: 6h
  artifact_policy: immutable
  auxiliary_policy: immutable
```

Use this mode for Alpine repositories discovered from `APKINDEX.tar.gz` requests. The repository root is the directory that contains `APKINDEX.tar.gz`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `upstreams` | `[]URL` | required | Upstream mirrors |
| `refresh_interval` | duration | `1h` | Background metadata refresh interval |
| `cleanup_interval` | duration | `6h` | Indexed cleanup interval |
| `artifact_policy` | policy | `immutable` | Policy for package files in the stable content namespace |
| `artifact_fresh_for` | freshness | — | Freshness for package files |
| `artifact_busy_policy` | busy policy | `join` | Busy policy for package files |
| `artifact_expire_after` | expiration | — | Expiration override for package files |
| `auxiliary_policy` | policy | `immutable` | Policy for versioned package sidecars in the stable content namespace |
| `auxiliary_fresh_for` | freshness | `30s` | Freshness for auxiliary files |
| `auxiliary_busy_policy` | busy policy | `join` | Busy policy for auxiliary files |
| `auxiliary_expire_after` | expiration | — | Expiration override for auxiliary files |

</details>

<details>
<summary><b>deb</b> - Debian / Ubuntu repository cache</summary>

```yaml
deb:
  route: { path: /deb }
  upstreams:
    - https://deb.debian.org/debian
  refresh_interval: 1h
  cleanup_interval: 6h
  artifact_policy: immutable
  auxiliary_policy: immutable
```

Use this mode for Debian-style repositories discovered from `Release`, `InRelease`, `Packages*`, and `Sources*`
metadata requests. Both standard `dists/<suite>/...` layouts and flat repositories are supported.

Standard apt sources use `deb <proxy-url> <suite> <component>`. Flat repositories use
`deb [trusted=yes] <proxy-url> ./`. In both cases, `<proxy-url>` is the cache-proxy HTTP(S) URL, not a
local filesystem path.

Same field set as `apk`.

</details>

<details>
<summary><b>rpm</b> - RPM repository cache</summary>

```yaml
rpm:
  route: { path: /rpm }
  upstreams:
    - https://download.rockylinux.org/pub/rocky
  refresh_interval: 1h
  cleanup_interval: 6h
  artifact_policy: immutable
  auxiliary_policy: immutable
```

Use this mode for RPM repositories discovered from `repodata/repomd.xml`.

Same field set as `apk`.

</details>

<details>
<summary><b>pacman</b> - Arch repository cache</summary>

```yaml
pacman:
  route: { path: /pacman }
  upstreams:
    - https://geo.mirror.pkgbuild.com
  refresh_interval: 2m
  cleanup_interval: 6h
  artifact_policy: immutable
  auxiliary_policy: immutable
```

Use this mode for Arch repositories discovered from repository database requests such as `.db` and `.db.tar.*`.

Same field set as `apk`, except `refresh_interval` defaults to `2m`.

</details>

<details>
<summary><b>git</b> - read-only Git mirror</summary>

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

Use this mode for a single upstream Git repository mirrored behind an HTTP path.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `upstream` | URL | required | Remote Git repository |
| `auth.type` | enum | — | `basic` or `token` |
| `auth.username` | string | — | Username for `basic` auth |
| `auth.password` | string | — | Password or token, supports `$ENV` expansion |
| `proxy` | URL | — | HTTP or SOCKS5 proxy for upstream access |
| `sync_interval` | duration | `0` | Periodic sync interval; `0` means no background sync |
| `operation_timeout` | duration | `0` | Per clone/fetch timeout |
| `force_overwrite` | bool | `true` | Overwrite local refs after upstream force-pushes |

</details>

## Flatpak / OSTree Mode

`flatpak` uses background `summary` refresh and a dedicated cache layout:

- `summary`, `summary.sig`, and `config` are published as a metadata generation from one upstream.
- OSTree objects under `objects/` are cached outside metadata generations for reuse across updates.
- Verifiable OSTree objects must pass checksum validation before immutable cache writes.
- Static deltas under `deltas/` are cached as opaque immutable files by path; cache-proxy does not parse or verify delta contents, and Flatpak/OSTree clients validate them while applying updates.
- `.flatpakrepo` and `.flatpakref` descriptors are revalidated and can be rewritten to keep clients on the proxy.

## Linux Repository Modes

`apk`, `deb`, `rpm`, and `pacman` use background metadata refresh:

- Repositories are discovered from client metadata requests.
- Discovered repositories are persisted, and startup reconciles refresh tasks from the persisted repository set.
- Metadata is published only after a full generation is fetched and validated.
- The current generation is the authoritative serving view for repository metadata, including companion files such as signatures and checksums.
- If no local generation exists yet, metadata requests bypass to upstream and trigger background refresh.
- Metadata and its signatures/checksums are generation-scoped and fixed to one upstream. Package artifacts and package sidecars use an instance-wide stable content namespace, so a metadata refresh does not change their cache keys.
- Artifact and sidecar downloads are independent requests; they are not blocked by index misses or refresh failure. Protocol inspectors classify these resources before the generic cache executes their policy.
- The current cleanup indexes are loaded only during cleanup and combined to retain shared content such as Debian `pool/` files. They are never runtime download allowlists.
- Metadata refreshes and probes share the same per-host admission and `429` cooldown as client downloads; a rate-limited refresh is rescheduled for the advertised retry time.
- Cleanup expires stable content absent from all current cleanup indexes while preserving discovered-root state. Metadata GC removes superseded generation objects, cleanup indexes, and snapshot descriptors together.

## Operations

- Run behind a TLS-terminating reverse proxy if exposed outside localhost.
- Keep config files private; they may contain upstream credentials.
- Set `metrics.token` if `/metrics` is reachable by other hosts.
- Restart the process to apply configuration changes.

## Development

```bash
make fmt
make test
```

## License

[MIT](LICENSE)
