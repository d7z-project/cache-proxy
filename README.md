# cache-proxy

`cache-proxy` is a Go caching reverse proxy with static YAML configuration and BlobFS-backed cache storage.

## Features

- File, OCI registry, npm registry, Go module, Maven, Cargo (Rust), PyPI, APK, DEB, RPM, and Pacman proxy modes.
- One YAML config file defines listeners, storage, metrics, cleanup, and proxy instances.
- Path-mounted instances and dedicated bind listeners.
- Prometheus metrics for proxy traffic and BlobFS state.
- Background BlobFS GC and cache cleanup.
- Metadata-driven refresh and cleanup for APK, DEB, RPM, and Pacman package repositories.

## Build

```bash
make cache-proxy
```

## Run

```bash
./cache-proxy -config ./cache-proxy.yaml
```

Open the root page in a browser to inspect the published routes and example YAML snippets:

```text
http://127.0.0.1:18080/
```

Validate config without starting:

```bash
./cache-proxy -config ./cache-proxy.yaml -validate
```

## Network Configuration

There are two unrelated configuration concerns:

- Instance publish mode
  Use `route.path` when the instance should be mounted under the main `server.bind` HTTP listener.
  Use `bind` when the instance should open its own dedicated TCP listener.
- Outbound HTTP proxy
  Use `transport.proxy` when upstream HTTP requests from that instance must go through an HTTP proxy.
  This affects egress requests only. It does not change whether the instance is published by `route.path` or `bind`.

Current built-in publish modes:

- `oci` uses `bind`.
- `file`, `npm`, `go`, `maven`, `cargo`, `pypi`, `apk`, `deb`, `rpm`, and `pacman` use `route.path`.

## Home Page URLs

The root page (`/`) shows one access URL for each configured instance.

- For `route.path` instances, the page builds the URL from the current request host plus the configured path.
- For `bind` instances, the page builds the URL from the configured bind port. If the bind host is `0.0.0.0`, `::`, `127.0.0.1`, or `localhost`, the page reuses the current request host.

When `cache-proxy` is behind another reverse proxy, forward these headers to make the displayed URLs correct:

- `X-Forwarded-Host`
- `X-Forwarded-Proto`

Example:

```text
X-Forwarded-Host: cache.example.com
X-Forwarded-Proto: https
```

Then the home page will render published URLs like `https://cache.example.com/files` instead of local listener addresses.

## Example Config

Base document:

```yaml
server:
  bind: 127.0.0.1:18080 # Main HTTP listener for path-mounted instances
  backend: /tmp/cache-proxy # BlobFS cache directory

metrics:
  path: /metrics # Prometheus endpoint on the main listener
  token: "" # Optional bearer token

storage:
  gc:
    blob: 24h # BlobFS GC interval
  cleanup:
    enabled: false
    interval: 6h
    dry_run: false
    batch_size: 500
    workers: 0

instances: []
```

<details>
<summary><code>file</code></summary>

```yaml
instances:
  - name: files
    enabled: true
    file:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /files # Published under server.bind
      upstreams:
        - https://example.com # Upstream origin
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      default_policy: revalidate # Default cache policy for all fetched paths
      busy_policy: stale # Serve stale cached responses while revalidating
      rules:
        - match: "releases/**/*.zip" # Match path relative to the upstream root
          policy: immutable # Immutable archives can be cached aggressively
        - match: "feeds/**"
          policy: revalidate # Mutable indexes should always be revalidated
```

</details>

<details>
<summary><code>oci</code></summary>

```yaml
instances:
  - name: registry
    enabled: true
    oci:
      expire_after: 720h # Cache retention upper bound for cached OCI objects
      bind: 127.0.0.1:5000 # Dedicated listener instead of route.path
      upstream: https://registry-1.docker.io # Registry upstream
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      default_policy: bypass # Default policy for repositories not matched by rules
      busy_policy: stale # Serve stale cached responses while revalidating
      rules:
        - match: "library/*" # Match Docker Hub-style repository names
          policy: immutable # Immutable blobs/manifests for stable repos
          expire_after: 168h # Optional per-rule retention override
        - match: "internal/**"
          policy: revalidate # Always revalidate mutable internal repositories
```

`oci.rules` matches repository names, not HTTP paths. Use it when one registry needs different cache behavior for different repositories.

</details>

<details>
<summary><code>npm</code></summary>

```yaml
instances:
  - name: npmjs
    enabled: true
    npm:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /npm # Published under server.bind
      upstream: https://registry.npmjs.org # Single registry upstream
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      metadata_policy: revalidate # Registry metadata changes over time
      metadata_busy_policy: stale # Serve stale metadata during refresh
      tarball_policy: immutable # Package tarballs are content-addressed
```

</details>

<details>
<summary><code>go</code></summary>

```yaml
instances:
  - name: golang
    enabled: true
    go:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /go # Published under server.bind
      proxies:
        - https://proxy.golang.org # GOPROXY chain
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      sumdb:
        enabled: true # Verify modules with checksum database
        name: sum.golang.org # SumDB name presented to the Go toolchain
        url: https://sum.golang.org # Upstream SumDB base URL
      module_policy: revalidate # Version lists and metadata may change
      zip_policy: immutable # Module zips are immutable once published
```

</details>

<details>
<summary><code>maven</code></summary>

```yaml
instances:
  - name: central
    enabled: true
    maven:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /maven # Published under server.bind
      upstream: https://repo1.maven.org/maven2 # Single repository upstream
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      release_policy: immutable # Release artifacts should not change
      snapshot_policy: revalidate # Snapshot artifacts are mutable
      checksum_policy: revalidate # Checksum sidecars may be updated upstream
```

</details>

<details>
<summary><code>cargo</code> (Rust)</summary>

```yaml
instances:
  - name: crates
    enabled: true
    cargo:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /cargo # Published under server.bind
      upstream: https://index.crates.io # Single Cargo index upstream
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      crate_policy: immutable # Crate archives are immutable once published
```

</details>

<details>
<summary><code>pypi</code></summary>

```yaml
instances:
  - name: python
    enabled: true
    pypi:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /pypi # Published under server.bind
      upstream: https://pypi.org # Single PyPI upstream
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      index_policy: revalidate # Simple index pages and metadata can change
      file_policy: immutable # Distribution files are immutable
```

</details>

<details>
<summary><code>apk</code></summary>

```yaml
instances:
  - name: alpine
    enabled: true
    apk:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /apk # Published under server.bind
      repositories:
        - url: https://dl-cdn.alpinelinux.org/alpine # Repository root
          branches: [v3.20] # Alpine branch list
          repos: [main] # main/community/testing
          architectures: [x86_64] # Architecture list
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 1h # Background metadata refresh cadence
      refresh_timeout: 2m # Per-refresh timeout
      metadata_policy: revalidate # APKINDEX metadata changes over time
      artifact_policy: immutable # .apk artifacts are immutable
```

</details>

<details>
<summary><code>deb</code></summary>

```yaml
instances:
  - name: debian
    enabled: true
    deb:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /deb # Published under server.bind
      repositories:
        - url: https://deb.debian.org/debian # Repository root
          suites: [bookworm] # One or more suites
          components: [main] # Components to mirror
          architectures: [amd64] # Binary architectures
          source: true # Also proxy source packages
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 1h # Background metadata refresh cadence
      refresh_timeout: 2m # Per-refresh timeout
      metadata_policy: revalidate # Release and package indexes change over time
      artifact_policy: immutable # Package files are immutable
```

</details>

<details>
<summary><code>rpm</code></summary>

```yaml
instances:
  - name: rocky
    enabled: true
    rpm:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /rpm # Published under server.bind
      repositories:
        - url: https://download.rockylinux.org/pub/rocky # Repository root
          paths:
            - 9/BaseOS/x86_64/os # Repo subpaths under the root
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 1h # Background metadata refresh cadence
      refresh_timeout: 2m # Per-refresh timeout
      metadata_policy: revalidate # repodata can change as the repo updates
      artifact_policy: immutable # .rpm artifacts are immutable
```

</details>

<details>
<summary><code>pacman</code></summary>

```yaml
instances:
  - name: archlinux
    enabled: true
    pacman:
      expire_after: 720h # Cache retention upper bound
      route:
        path: /pacman # Published under server.bind
      repositories:
        - url: https://mirror.rackspace.com/archlinux # Repository root
          repos: [core] # core/extra/multilib
          architectures: [x86_64] # Architecture list
      transport:
        proxy: http://127.0.0.1:7890 # Optional outbound HTTP proxy
      refresh_interval: 2m # Background metadata refresh cadence
      refresh_timeout: 2m # Per-refresh timeout
      metadata_policy: revalidate # Sync databases change over time
      artifact_policy: immutable # Package files are immutable
```

</details>

## Package Repository Refresh And Cleanup

For `apk`, `deb`, `rpm`, and `pacman`, cached package files are no longer removed by a plain artifact TTL.

- Repository metadata is derived automatically from each mode's `repositories` declarations.
- Refresh runs automatically on startup and in the background. There is no manual startup refresh switch.
- Default refresh intervals remain mode-specific when `refresh_interval` is unset: `apk`, `deb`, and `rpm` refresh every `1h`; `pacman` refreshes every `2m`.
- Cleanup follows the latest successfully refreshed metadata snapshot.

## Development

```bash
make fmt
make test
```
