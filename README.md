# cache-proxy

> This project is part of Dragon's Zone HomeLab.

`cache-proxy` is a Go caching reverse proxy for package registries, artifact repositories, and generic file origins. It uses a static YAML config file and BlobFS-backed storage.

## Features

- Multiple proxy modes in one process: `file`, `oci`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, `pacman`
- BlobFS-backed cache storage
- Static YAML configuration
- Path-mounted and dedicated-listener instances
- Prometheus metrics
- Background cache cleanup
- Metadata-driven refresh and cleanup for package repository modes

## Installation

Build from source:

```bash
make cache-proxy
```

Run:

```bash
./cache-proxy -config ./cache-proxy.yaml
```

Validate a config file without starting the server:

```bash
./cache-proxy -config ./cache-proxy.yaml -validate
```

## Quick Start

Minimal config:

```yaml
server:
  bind: 127.0.0.1:18080 # Main HTTP listener for route-mounted instances
  backend: /tmp/cache-proxy # BlobFS storage directory

metrics:
  path: /metrics # Prometheus endpoint path
  token: "" # Optional bearer token for metrics

storage:
  gc:
    blob: 24h # BlobFS GC interval
  cleanup:
    enabled: false # Enable expired/orphaned cache cleanup
    interval: 6h # Cleanup scan interval
    dry_run: false # Report cleanup candidates without deleting them
    batch_size: 500 # Objects processed per cleanup batch
    workers: 0 # Worker count; 0 uses the default behavior

instances:
  - name: npmjs # Instance name shown in metrics and UI
    enabled: true # Disabled instances are ignored at runtime
    npm:
      route:
        path: /npm # Published under server.bind
      upstream: https://registry.npmjs.org # Upstream registry base URL
```

Then point clients to the published route:

- npm: `http://127.0.0.1:18080/npm`
- Go proxy: `http://127.0.0.1:18080/go`
- OCI registry: bind directly to its configured listener

## Configuration

Each instance enables exactly one mode.

- Use `route.path` to publish an instance under the main `server.bind` HTTP listener.
- Use `bind` for modes that expose their own listener, currently `oci`.
- Use `transport.proxy` to send upstream HTTP requests through an outbound proxy.

### Top-Level Fields

| Field | Description |
| --- | --- |
| `server.bind` | Main HTTP listener address |
| `server.backend` | BlobFS storage directory |
| `metrics.path` | Prometheus endpoint path on the main listener |
| `metrics.token` | Optional bearer token for the metrics endpoint |
| `storage.gc.blob` | BlobFS garbage collection interval |
| `storage.cleanup.enabled` | Enable cleanup of expired or orphaned cache objects |
| `storage.cleanup.interval` | Cleanup scan interval |
| `storage.cleanup.dry_run` | Report deletions without removing objects |
| `storage.cleanup.batch_size` | Maximum objects processed per cleanup batch |
| `storage.cleanup.workers` | Cleanup worker count; `0` uses the default worker behavior |

### Common Value Types

#### Policies

The following cache policy fields share the same allowed values:

- `bypass`: always fetch from upstream
- `immutable`: treat cached content as immutable
- `revalidate`: reuse cache but revalidate based on freshness rules

Fields using these values include:

- `file.default_policy`
- `oci.default_policy`
- `npm.metadata_policy`
- `npm.tarball_policy`
- `go.module_policy`
- `go.zip_policy`
- `maven.release_policy`
- `maven.snapshot_policy`
- `maven.checksum_policy`
- `cargo.crate_policy`
- `pypi.index_policy`
- `pypi.file_policy`
- `pypi.companion_policy`
- `apk|deb|rpm|pacman` package policy fields such as `metadata_policy`, `artifact_policy`, `auxiliary_policy`

#### Busy Policies

Busy policy fields support:

- `bypass`: do not serve stale cache while refresh/download is in progress
- `stale`: serve stale cache while refresh/download is in progress

Fields using these values include:

- `file.busy_policy`
- `oci.busy_policy`
- `npm.metadata_busy_policy`
- `go.module_busy_policy`
- `go.sumdb_busy_policy`
- `maven.metadata_busy_policy`
- `maven.checksum_busy_policy`
- `cargo.index_busy_policy`
- `pypi.index_busy_policy`
- `pypi.companion_busy_policy`
- `apk|deb|rpm|pacman` package busy policy fields

#### Durations

Duration fields use Go duration syntax such as:

- `30s`
- `2m`
- `1h`
- `24h`

#### Expiration

Expiration fields such as `expire_after` support:

- a Go duration such as `720h`
- `never`

#### Freshness

Freshness fields such as `*_fresh_for` support:

- a Go duration such as `1m`
- `forever`

### Supported Modes

| Mode | Publish style | Typical upstream |
| --- | --- | --- |
| `file` | `route.path` | Static files or generic HTTP origins |
| `oci` | `bind` | OCI / Docker registries |
| `npm` | `route.path` | npm registry |
| `go` | `route.path` | GOPROXY chain |
| `maven` | `route.path` | Maven repository |
| `cargo` | `route.path` | crates.io index |
| `pypi` | `route.path` | PyPI |
| `apk` | `route.path` | Alpine repositories |
| `deb` | `route.path` | Debian / Ubuntu repositories |
| `rpm` | `route.path` | RPM repositories |
| `pacman` | `route.path` | Arch Linux repositories |

## Mode Examples

### file

Use `file` for generic HTTP content with optional path-based rules.

```yaml
instances:
  - name: files
    enabled: true
    file:
      expire_after: 720h
      route:
        path: /files
      upstreams:
        - https://example.com
      transport:
        proxy: http://127.0.0.1:7890
      default_policy: revalidate
      busy_policy: stale
      rules:
        - match: "releases/**/*.zip"
          policy: immutable
        - match: "feeds/**"
          policy: revalidate
```

Relevant options:

- `default_policy`: `bypass`, `immutable`, `revalidate`
- `busy_policy`: `bypass`, `stale`
- `rules[].match`: doublestar path pattern
- `rules[].policy`: `bypass`, `immutable`, `revalidate`
- `rules[].busy_policy`: `bypass`, `stale`
- `rules[].expire_after`: duration or `never`

### oci

`oci.rules` matches repository names, not HTTP paths.

```yaml
instances:
  - name: registry # Instance name
    enabled: true
    oci:
      expire_after: 720h # Retention upper bound for cached OCI content
      bind: 127.0.0.1:5000 # Dedicated listener address
      upstream: https://registry-1.docker.io # Upstream registry
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      default_policy: bypass # Default policy for unmatched repositories
      busy_policy: stale # Serve stale while refresh is in progress
      auth:
        type: basic # none | basic | bearer
        username: my-user # Required for basic auth
        password: my-password # Required for basic auth
      rules:
        - match: "library/*" # Repository-name pattern
          policy: immutable # Immutable public images
          expire_after: 168h # Optional per-rule retention override
        - match: "internal/**"
          policy: revalidate # Mutable internal images
```

Relevant options:

- `default_policy`: `bypass`, `immutable`, `revalidate`
- `busy_policy`: `bypass`, `stale`
- `auth.type`: `none`, `basic`, `bearer`
- `rules[].match`: doublestar repository pattern
- `rules[].policy`: `bypass`, `immutable`, `revalidate`
- `rules[].expire_after`: duration or `never`

### npm

```yaml
instances:
  - name: npmjs # Instance name
    enabled: true
    npm:
      expire_after: 720h # Retention upper bound
      route:
        path: /npm # Published under server.bind
      upstream: https://registry.npmjs.org # Upstream registry
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      metadata_policy: revalidate # Registry metadata changes over time
      metadata_busy_policy: stale # Serve stale metadata during refresh
      tarball_policy: immutable # Tarballs are usually immutable
```

Relevant options:

- `metadata_policy`: `bypass`, `immutable`, `revalidate`
- `metadata_busy_policy`: `bypass`, `stale`
- `tarball_policy`: `bypass`, `immutable`, `revalidate`

### go

```yaml
instances:
  - name: golang # Instance name
    enabled: true
    go:
      expire_after: 720h # Retention upper bound
      route:
        path: /go # Published under server.bind
      proxies:
        - https://proxy.golang.org # GOPROXY chain entry
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      sumdb:
        enabled: true # Enable checksum database proxying
        name: sum.golang.org # SumDB name exposed to clients
        url: https://sum.golang.org # Upstream SumDB URL
      goprivate:
        - "*.corp.example.com" # Patterns treated as private modules
      module_policy: revalidate # Version metadata can change
      module_busy_policy: stale # Serve stale metadata during refresh
      zip_policy: immutable # Module zip archives are immutable
      sumdb_busy_policy: stale # Serve stale SumDB data during refresh
```

Relevant options:

- `module_policy`: `bypass`, `immutable`, `revalidate`
- `module_busy_policy`: `bypass`, `stale`
- `zip_policy`: `bypass`, `immutable`, `revalidate`
- `sumdb.enabled`: `true`, `false`
- `sumdb_busy_policy`: `bypass`, `stale`
- `disable_module_fetch_header`: `true`, `false`

### maven

```yaml
instances:
  - name: central # Instance name
    enabled: true
    maven:
      expire_after: 720h # Retention upper bound
      route:
        path: /maven # Published under server.bind
      upstream: https://repo1.maven.org/maven2 # Upstream repository root
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      metadata_busy_policy: stale # Serve stale maven-metadata.xml during refresh
      checksum_policy: revalidate # Checksums and signatures can be updated
      checksum_busy_policy: stale # Serve stale checksum sidecars during refresh
      release_policy: immutable # Release artifacts should not change
      snapshot_policy: revalidate # Snapshot artifacts are mutable
```

Relevant options:

- `release_policy`: `bypass`, `immutable`, `revalidate`
- `snapshot_policy`: `bypass`, `immutable`, `revalidate`
- `checksum_policy`: `bypass`, `immutable`, `revalidate`
- `metadata_busy_policy`: `bypass`, `stale`
- `checksum_busy_policy`: `bypass`, `stale`

### cargo

```yaml
instances:
  - name: crates # Instance name
    enabled: true
    cargo:
      expire_after: 720h # Retention upper bound
      route:
        path: /cargo # Published under server.bind
      upstream: https://index.crates.io # Cargo sparse index upstream
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      index_busy_policy: stale # Serve stale index during refresh
      crate_policy: immutable # Crate archives are immutable
      auth_required: false # Require auth for crate downloads
```

Relevant options:

- `index_busy_policy`: `bypass`, `stale`
- `crate_policy`: `bypass`, `immutable`, `revalidate`
- `auth_required`: `true`, `false`

### pypi

```yaml
instances:
  - name: python # Instance name
    enabled: true
    pypi:
      expire_after: 720h # Retention upper bound
      route:
        path: /pypi # Published under server.bind
      upstream: https://pypi.org # Upstream PyPI base URL
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      index_policy: revalidate # Simple API pages change over time
      index_busy_policy: stale # Serve stale indexes during refresh
      file_policy: immutable # Package files are immutable
      companion_policy: revalidate # Sidecar metadata may change
      companion_busy_policy: stale # Serve stale sidecars during refresh
      proxy_json: true # Enable JSON Simple API proxying
      proxy_core_metadata: false # Proxy core metadata sidecars
      proxy_signatures: false # Proxy signature sidecars
```

Relevant options:

- `index_policy`: `bypass`, `immutable`, `revalidate`
- `file_policy`: `bypass`, `immutable`, `revalidate`
- `companion_policy`: `bypass`, `immutable`, `revalidate`
- `index_busy_policy`: `bypass`, `stale`
- `companion_busy_policy`: `bypass`, `stale`
- `proxy_json`: `true`, `false`
- `proxy_core_metadata`: `true`, `false`
- `proxy_signatures`: `true`, `false`

### apk

```yaml
instances:
  - name: alpine # Instance name
    enabled: true
    apk:
      expire_after: 720h # Retention upper bound
      route:
        path: /apk # Published under server.bind
      repositories:
        - url: https://dl-cdn.alpinelinux.org/alpine # Repository root
          branches: [v3.20] # Branch list
          repos: [main] # Repository names
          architectures: [x86_64] # Architectures to mirror
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 1h # Background metadata refresh cadence
      refresh_timeout: 2m # Timeout for a single refresh cycle
      metadata_policy: revalidate # APKINDEX metadata changes over time
      artifact_policy: immutable # .apk artifacts are immutable
```

### deb

```yaml
instances:
  - name: debian # Instance name
    enabled: true
    deb:
      expire_after: 720h # Retention upper bound
      route:
        path: /deb # Published under server.bind
      repositories:
        - url: https://deb.debian.org/debian # Repository root
          suites: [bookworm] # Suites to mirror
          components: [main] # Components to mirror
          architectures: [amd64] # Binary architectures
          source: true # Also proxy source package metadata
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 1h # Background metadata refresh cadence
      refresh_timeout: 2m # Timeout for a single refresh cycle
      metadata_policy: revalidate # Release and package indexes change over time
      artifact_policy: immutable # Package files are immutable
```

### rpm

```yaml
instances:
  - name: rocky # Instance name
    enabled: true
    rpm:
      expire_after: 720h # Retention upper bound
      route:
        path: /rpm # Published under server.bind
      repositories:
        - url: https://download.rockylinux.org/pub/rocky # Repository root
          paths:
            - 9/BaseOS/x86_64/os # Subpath under the repository root
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 1h # Background metadata refresh cadence
      refresh_timeout: 2m # Timeout for a single refresh cycle
      metadata_policy: revalidate # repodata changes over time
      artifact_policy: immutable # .rpm artifacts are immutable
```

### pacman

```yaml
instances:
  - name: archlinux # Instance name
    enabled: true
    pacman:
      expire_after: 720h # Retention upper bound
      route:
        path: /pacman # Published under server.bind
      repositories:
        - url: https://mirror.rackspace.com/archlinux # Repository root
          repos: [core] # Repository names
          architectures: [x86_64] # Architectures to mirror
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 2m # Background metadata refresh cadence
      refresh_timeout: 2m # Timeout for a single refresh cycle
      metadata_policy: revalidate # Sync databases change over time
      artifact_policy: immutable # Package files are immutable
```

### Package Repository Notes

The `apk`, `deb`, `rpm`, and `pacman` modes derive repository metadata from `repositories` and keep cache cleanup aligned with the latest successful metadata snapshot.

Relevant options:

- `refresh_interval`: background metadata refresh cadence
- `refresh_timeout`: timeout for a single refresh cycle
- `metadata_policy`, `artifact_policy`, `auxiliary_policy`: `bypass`, `immutable`, `revalidate`
- `metadata_busy_policy`, `artifact_busy_policy`, `auxiliary_busy_policy`: `bypass`, `stale`
- `deb.repositories[].source`: `true`, `false`

Default refresh intervals when `refresh_interval` is unset:

- `apk`: `1h`
- `deb`: `1h`
- `rpm`: `1h`
- `pacman`: `2m`

## Development

```bash
make fmt
make test
```

## License

This project is licensed under the terms of the [LICENSE](LICENSE).
