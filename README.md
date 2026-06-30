# cache-proxy

A caching reverse proxy for package registries and artifact repositories. Single binary, single YAML config.

## Features

- 12 proxy modes in one process: `file`, `git`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman`
- Path-mounted and dedicated-listener instances
- Per-resource cache policies: `bypass`, `immutable`, `revalidate`
- Background blob GC and expired-object cleanup
- Background metadata refresh for Linux repositories (`apk`, `deb`, `rpm`, `pacman`)
- Prometheus metrics and built-in home page

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
| `metrics.path` | path | `/metrics` | Prometheus endpoint |
| `metrics.token` | string | — | Optional bearer token for `/metrics` |
| `storage.gc.blob` | duration | `24h` | Blob storage GC interval |
| `storage.cleanup.dry_run` | bool | `false` | Run scheduled cleanup without deleting files |
| `storage.cleanup.batch_size` | int | `500` | Maximum deletions per cleanup batch |
| `storage.orphan_policy` | string | — | Home page orphan cleanup policy (`auto`) |
| `storage.download.max_active` | int | `64` | Global concurrent cache-fill downloads |
| `storage.download.max_active_per_instance` | int | `8` | Concurrent cache-fill downloads per instance |

Value types:

| Type | Examples | Special values |
| --- | --- | --- |
| `duration` | `30s`, `5m`, `24h` | — |
| `expiration` | `720h` | `never` |
| `freshness` | `30s`, `5m` | `forever` |

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
- `transport.health` exists for upstream health tuning, but most deployments should use defaults.

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
- Debian: `deb http://cache.lan:8080/deb bookworm main`
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

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstreams` | `[]URL` | required | Upstream base URLs, tried in order |
| `pass_headers` | `[]string` | — | Request headers forwarded upstream |
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

Use this mode for a dedicated registry listener. Clients point Docker or other OCI tooling at the bound address.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bind` | `host:port` | required | Dedicated listener |
| `display_url` | URL | — | Home page URL override |
| `upstream` | URL | required | Upstream registry |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `default_policy` | policy | `bypass` | Default cache policy |
| `fresh_for` | freshness | — | Freshness for cached manifests |
| `busy_policy` | busy policy | `bypass` | Busy policy while downloading |
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

Use this mode for npm metadata and package tarballs behind a single prefix.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `upstream` | URL | required | Upstream registry |
| `metadata_policy` | policy | `revalidate` | Policy for package metadata |
| `metadata_fresh_for` | freshness | — | Freshness for metadata |
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
  sumdb_busy_policy: bypass
  goprivate:
    - "*.corp.example.com"
```

Use this mode to proxy public module traffic while allowing selected private module prefixes to bypass the proxy.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `expire_after` | expiration | `720h` | Maximum object lifetime |
| `proxies` | `[]URL` | required | GOPROXY chain |
| `module_policy` | policy | `revalidate` | Policy for module metadata endpoints |
| `module_fresh_for` | freshness | `1m` | Freshness for module metadata |
| `module_busy_policy` | busy policy | `stale` | Busy policy for module metadata |
| `zip_policy` | policy | `immutable` | Policy for module zip files |
| `sumdb.enabled` | bool | `true` | Enable SumDB proxying |
| `sumdb.name` | string | `sum.golang.org` | SumDB name in request path |
| `sumdb.url` | URL | `https://sum.golang.org` | Upstream SumDB |
| `sumdb_fresh_for` | freshness | `30s` | Freshness for SumDB responses |
| `sumdb_busy_policy` | busy policy | `bypass` | Busy policy for SumDB |
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
  checksum_busy_policy: bypass
  metadata_fresh_for: 2m
  metadata_busy_policy: stale
```

Use this mode for Maven Central or an internal Maven repository with different behavior for releases, snapshots, metadata, and checksums.

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
| `checksum_policy` | policy | `revalidate` | Policy for checksum and signature sidecars |
| `checksum_fresh_for` | freshness | `30s` | Freshness for checksum sidecars |
| `checksum_busy_policy` | busy policy | `bypass` | Busy policy for checksum sidecars |

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
  companion_policy: revalidate
  companion_fresh_for: 30s
  companion_busy_policy: bypass
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
| `companion_policy` | policy | `revalidate` | Policy for sidecar files |
| `companion_fresh_for` | freshness | `30s` | Freshness for sidecars |
| `companion_busy_policy` | busy policy | `bypass` | Busy policy for sidecars |
| `proxy_json` | bool | `true` | Enable `/simple/<pkg>/json` |
| `proxy_core_metadata` | bool | `false` | Proxy metadata sidecars |
| `proxy_signatures` | bool | `false` | Proxy signature sidecars |

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
  auxiliary_policy: revalidate
```

Use this mode for Alpine repositories discovered from `APKINDEX.tar.gz` requests.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `route.path` | path | required | URL mount path |
| `upstreams` | `[]URL` | required | Upstream mirrors |
| `refresh_interval` | duration | `1h` | Background metadata refresh interval |
| `cleanup_interval` | duration | `6h` | Indexed cleanup interval |
| `artifact_policy` | policy | `immutable` | Policy for package files |
| `artifact_fresh_for` | freshness | — | Freshness for package files |
| `artifact_busy_policy` | busy policy | `bypass` | Busy policy for package files |
| `artifact_expire_after` | expiration | — | Expiration override for package files |
| `auxiliary_policy` | policy | `revalidate` | Policy for signatures and checksums |
| `auxiliary_fresh_for` | freshness | `30s` | Freshness for auxiliary files |
| `auxiliary_busy_policy` | busy policy | `bypass` | Busy policy for auxiliary files |
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
  auxiliary_policy: revalidate
```

Use this mode for Debian-style repositories discovered from `Release` and `InRelease`.

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
  auxiliary_policy: revalidate
```

Use this mode for RPM repositories discovered from `repomd.xml`.

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
  auxiliary_policy: revalidate
```

Use this mode for Arch repositories discovered from `.db` and `.files` requests.

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

## Linux Repository Modes

`apk`, `deb`, `rpm`, and `pacman` use background metadata refresh:

- Repositories are discovered from client metadata requests.
- Metadata is published only after a full generation is fetched and validated.
- If no local generation exists yet, metadata requests bypass to upstream and trigger background refresh.
- Artifact and auxiliary downloads stay normal proxy/cache requests; they are not blocked by index misses.
- Cleanup rebuilds a short-lived sorted path set from the current generation when needed. No package index is persisted for runtime validation.

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
