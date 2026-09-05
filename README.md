# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

A self-hosted, read-only caching proxy for package registries, Linux
repositories, container images, Git repositories, and HTTP files.

Run multiple isolated instances in one process, reduce repeated downloads,
and monitor cache activity through a built-in dashboard and Prometheus metrics.
Each instance uses protocol-specific caching and one configured upstream.

[Installation](#installation) | [Quick Start](#quick-start) |
[Configuration](#configuration) | [Client Setup](#client-setup) |
[Operations](#operations) | [Development](#development)

## Features

- Protocol-aware metadata refresh and content verification.
- Shared downloads for concurrent requests to the same cached object.
- Global and per-host concurrency limits, request pacing, and upstream cooldowns.
- Atomic metadata snapshots for Linux and Flatpak/OSTree repositories.
- Persistent storage, background cleanup, and policy-controlled offline reuse.
- Dashboard with client configuration, maintenance errors, and storage statistics.

### Supported Modes

| Mode | Content |
|---|---|
| `file` | HTTP files selected by cache rules |
| `npm` | Package metadata and tarballs; audit queries pass through |
| `go` | Go modules and an optional checksum database |
| `maven` | Metadata, artifacts, signatures, and checksums |
| `cargo` | Sparse indexes, crates, and fetch-only Git indexes |
| `pypi` | Simple HTML/JSON indexes and distributions |
| `deb` | Debian standard, nested, and flat repositories |
| `apk` | Alpine indexes, packages, and sidecars |
| `rpm` | rpm-md metadata and RPM packages |
| `pacman` | Databases, packages, deltas, and sidecars |
| `flatpak` | Flatpak/OSTree summaries, objects, and static deltas |
| `oci` | OCI Distribution manifests and blobs |
| `git` | Git smart HTTP backed by a local bare mirror |

Repository layouts, architectures, metadata paths, and compression variants
are handled by the selected mode. Repository modes do not need custom path rules.
Git instances each mirror one repository.

## Installation

### Container

Images are published to GitHub Container Registry. The `main` tag follows the
default branch; release images use release version tags.

```bash
docker pull ghcr.io/d7z-project/cache-proxy:main
```

### Binary

Download a Linux binary and `SHA256SUMS` from
[GitHub Releases](https://github.com/d7z-project/cache-proxy/releases).
Available binary architectures are `amd64`, `arm64`, and `loong64`.
Verify the checksum, make the binary executable, and run it with `-config`.

### From Source

Requires Go 1.26 or newer and Make.

```bash
git clone https://github.com/d7z-project/cache-proxy.git
cd cache-proxy
make cache-proxy
cp config.example.yaml cache-proxy.yaml
./cache-proxy -config cache-proxy.yaml -validate
./cache-proxy -config cache-proxy.yaml
```

The example enables Debian and npm on `127.0.0.1:18080` and stores data in
`./data`. Other modes can be enabled individually.

## Quick Start

For a local npm cache, create `cache-proxy.yaml`:

```yaml
server:
  bind: 0.0.0.0:18080
  backend: /data
  public_url: http://127.0.0.1:18080

instances:
  - name: npm
    enabled: true
    mode: npm
    path: /npm
    upstream: https://registry.npmjs.org
```

Start the container with a persistent data volume:

```bash
docker run -d --name cache-proxy \
  -p 127.0.0.1:18080:18080 \
  -v "$PWD/cache-proxy.yaml:/etc/cache-proxy.yaml:ro" \
  -v cache-proxy-data:/data \
  ghcr.io/d7z-project/cache-proxy:main \
  -config /etc/cache-proxy.yaml
```

Open [the dashboard](http://127.0.0.1:18080/) and configure npm:

```bash
npm config set registry http://127.0.0.1:18080/npm
```

This publishes the service only on the host's loopback interface. For shared
access, configure the published address, `server.public_url`, TLS, and access
controls for your deployment.

## Configuration

[config.example.yaml](config.example.yaml) documents all settings, their
purposes, defaults, and examples for every mode. Example values are explicitly
distinguished from application defaults.

Configuration uses strict YAML: unknown fields, invalid URLs, duplicate instance
names, and conflicting listeners fail validation. Durations use Go syntax
(`500ms`, `15m`, `24h`); sizes accept binary units such as `MiB` and `GiB`.

```bash
./cache-proxy -config cache-proxy.yaml -validate
```

### Instances

| Setting | Purpose |
|---|---|
| `name` | Required unique identifier using letters, digits, underscores, or hyphens |
| `enabled` | Enable the instance; defaults to `false` |
| `mode` | Required protocol mode from the table above |
| `upstream` | Required single HTTP(S) upstream base URL |
| `path` | Mount on the main HTTP listener |
| `bind` | Use a dedicated listener; required for OCI |
| `display_url` | Public client address for a dedicated OCI listener |
| `transport` | Outbound proxy, User-Agent, connection limits, and timeouts |
| `refresh.interval` | Mutable freshness limit and repository polling interval |
| `options` | Mode-specific configuration |

Specify exactly one of `path` or `bind`. Upstream high availability can be
provided by DNS or a load balancer at the configured address. HTTP transport
supports HTTP, HTTPS, SOCKS5, and SOCKS5H outbound proxies.

### Refresh and Rate Limits

Set `refresh: {interval: 30m}` on an instance to adjust mutable metadata
freshness. The minimum interval is `1s`.

| Content | Default interval |
|---|---|
| Linux repository metadata and Flatpak summaries | 15m |
| npm, PyPI, Cargo, Go and Maven mutable metadata; Flatpak mutable refs | 1m |
| OCI tags | 2m |
| Maven SNAPSHOT files | 5m |
| Git mirror synchronization | 5m, through `options.sync_interval` |

For file `http_cache` rules, `refresh.interval` is a freshness ceiling and a
fallback when upstream omits a lifetime. Without it, caching requires explicit
upstream freshness.

Repository polling is staggered across roots; other mutable objects refresh
on demand. Shorter upstream cache policies take precedence. Refresh settings
do not extend protocol expiry, signed URL validity, or immutable object retention.

Use `storage.download` to control global concurrency, per-host concurrency,
and request pacing. The `hosts` mapping overrides limits for a hostname with an
optional port. Limits apply across instances, including redirect and authentication
requests. Real upstream `429` responses establish cooldowns that honor
`Retry-After`. Tune both refresh intervals and download limits to upstream quotas.

### Mode Options

| Mode | Options |
|---|---|
| `file` | `pass_headers`; `rules` with `match` and `policy` |
| `git` | `auth`, `sync_interval`, `operation_timeout` |
| `go` | `sumdb`, `goprivate`, `disable_module_fetch_header` |
| `oci` | `auth` |

File policies are `http_cache`, `immutable`, and `passthrough`; the last
matching rule wins. Git auth supports `basic` and `token`; OCI auth supports
`none`, `basic`, and `bearer`. Credential fields support environment variable
expansion. Other modes use the shared instance settings.

## Client Setup

The dashboard provides client configuration for each enabled instance. Replace
the example hostname with your public address and enable the corresponding
instances in your configuration.

```bash
npm config set registry https://mirrors.example.com/npm
go env -w GOPROXY=https://mirrors.example.com/go
pip install --index-url https://mirrors.example.com/pypi/simple PACKAGE
git clone https://mirrors.example.com/git/cache-proxy.git
```

For Cargo, add the following to `.cargo/config.toml`. The sparse registry URL
must end in a slash:

```toml
[source.cache-proxy]
registry = "sparse+https://mirrors.example.com/cargo/"

[source.crates-io]
replace-with = "cache-proxy"
```

Linux package managers use the instance URL in place of the upstream base.
For example, standard and flat Debian sources in `/etc/apt/sources.list.d/cache-proxy.list`:

```text
deb https://mirrors.example.com/debian bookworm main
deb [signed-by=/usr/share/keyrings/vendor-archive-keyring.gpg] https://mirrors.example.com/debian-flat ./
```

These require instances mounted at `/debian` and `/debian-flat`, respectively.
Retain the upstream signing configuration; the flat example assumes the vendor's
key has been installed at the specified path.

OCI uses a dedicated listener. For the example configuration's Docker Hub instance,
expose that listener through a TLS endpoint and set `display_url` to its public
address, such as `https://registry.example.com`. Clients then pull through it:

```bash
docker pull registry.example.com/library/alpine:3.20
```

## Operations

### Deployment and Storage

Use a dedicated persistent backend volume with one writer process. Instance data
is stored under `<backend>/instances/<name>/<mode>/` in `blobs`, `state`, and
`work` directories. Send `SIGINT` or `SIGTERM` for graceful shutdown.

Backend formats are internal and have no compatibility guarantee across builds.
Invalid cache records are treated as misses and cleaned up. Plan for cache
repopulation when upgrading.

`storage.gc.blob` controls unreferenced blob retention, while `storage.cleanup`
controls cleanup batching and dry runs. `storage.orphan_policy` selects reporting
or automatic cleanup of inactive instance data. Object size and temporary download
limits are not a total backend disk quota; monitor available storage.

Terminate TLS and configure client access controls at your ingress. The optional
`metrics.token` protects only the Prometheus endpoint, not the dashboard or proxy
routes. Upstream credentials belong to individual instances.

### Freshness and Offline Use

Mutable metadata uses conditional upstream validation. Cached content can remain
available during transient failures when request, upstream, and protocol policies
allow reuse. Downloads for the same cached object share an upstream transfer.

Explicit `no-cache` or `max-age=0` requests require validation even when data is
cached. APT normally requests index validation; offline use requires client settings
that permit cache reuse. Existing repository metadata requests that require refresh
wait up to 30 seconds, or the client's shorter deadline, and return `504` on timeout.

Repository metadata is published as a verified atomic snapshot. A failed refresh
leaves committed metadata and independently cached packages intact. Debian
`Valid-Until` limits reuse even after successful conditional validation. Atomic
publication keeps each snapshot coherent, but unversioned URLs do not provide
snapshot isolation across separate client requests.

Debian repositories advertising `Acquire-By-Hash: yes` refresh only Release
metadata and signatures. Indexes are downloaded on demand and verified against
their declared size and strong checksums before serving; unrelated architectures,
translations, and compression variants are not prefetched. Canonical index requests
use the current Release's content identity, preferring SHA512 over SHA256. If an
index's by-hash URL returns `403` or `404`, a uniquely identified canonical index
can be used only after the same verification. Repositories without by-hash retain
complete metadata snapshots. This selection is automatic for standard, nested,
and flat layouts. Only downloaded indexes are available offline; a historical
hash removed upstream cannot be recovered unless already cached.

First-time repository anchor requests stream from upstream while the cache is
populated. Upstream `no-store` responses are not retained. Repository-tree modes preserve
root and directory requests and pass safe, unclassified same-origin resources
through; Go and OCI retain their protocol endpoint boundaries.

### Monitoring

| Endpoint | Purpose |
|---|---|
| `/` | Dashboard, instance status, and client configuration |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Disk usage history |
| `/-/status/events` | Maintenance results and errors; accepts `limit` |
| `/-/status/network` | Instance and upstream request statistics |
| `/metrics` | Prometheus metrics; configurable through `metrics.path` |

Review maintenance errors in the dashboard or events API, and process logs with
`docker logs cache-proxy` for container deployments. Upstream statistics group
requests by origin without including credentials or resource paths.

## Development

Architecture, engineering, and testing requirements are documented in [AGENTS.md](AGENTS.md).
Protocol implementations live in `pkg/proxy/<mode>`.
Changes to cache behavior should include protocol tests and the relevant native-client E2E case.

```bash
make fmt
make vet
make test
make test-race
make test-fuzz
```

Run `make tidy` after dependency changes and `make cache-proxy` for a static build.

Measure status-history read allocations with
`go test ./pkg/app -run '^$' -bench '^BenchmarkStatusHistoryReads$' -benchmem`.

End-to-end tests require Docker or Podman on native Linux. Proxy, fixture, probes,
and package clients run in host-network containers.

```bash
make test-e2e
E2E_RUNTIME=podman E2E_SUITE=deb make test-e2e
```

`E2E_SUITE` accepts any supported mode; omitting it runs every mode. Cases cover
cold downloads, warm reuse, upstream updates, and persistent-cache offline restart.
Debian standard and flat repositories are tested separately, each with and without
by-hash support, including unused-index request counters.

## License

[MIT](LICENSE)
