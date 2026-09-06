# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

A self-hosted, read-only caching proxy for package registries, Linux
repositories, container images, Git repositories, and HTTP files.

Reduce repeated downloads across machines and CI jobs, and monitor cache activity
through a built-in dashboard and Prometheus metrics. A single process can serve
multiple registries, each with its own upstream, cache, and refresh settings.

[Installation](#installation) | [Quick Start](#quick-start) |
[Configuration](#configuration) | [Client Setup](#client-setup) |
[Operations](#operations) | [Testing](#testing)

## Features

- Use native package clients with protocol-aware caching and content verification.
- Reuse cached downloads across clients and service restarts.
- Share upstream transfers when multiple clients request the same object.
- Control metadata freshness, download concurrency, and upstream request rates.
- Inspect storage usage, network activity, maintenance errors, and client setup
  instructions in the dashboard.

### Supported Modes

| Mode | Content |
|---|---|
| `file` | HTTP files selected by cache rules |
| `npm` | Package metadata and tarballs; audit queries pass through |
| `go` | Go modules and an optional checksum database |
| `maven` | Metadata, artifacts, signatures, and checksums |
| `cargo` | Sparse and Git registry indexes, and crates |
| `pypi` | Simple HTML/JSON indexes and distributions |
| `deb` | Debian standard, nested, and flat repositories |
| `apk` | Alpine package indexes, packages, and signatures |
| `rpm` | rpm-md metadata and RPM packages |
| `pacman` | Arch Linux package databases, packages, deltas, and signatures |
| `flatpak` | Flatpak/OSTree summaries, objects, and static deltas |
| `oci` | OCI Distribution manifests and blobs |
| `git` | Git repositories over smart HTTP |

Each instance connects to one HTTP(S) upstream. Repository modes handle their
supported layouts automatically; custom cache rules are available for `file`
instances. Each `git` instance mirrors one repository. All modes provide read-only
upstream access.

## Installation

### Container

Use Docker or Podman with the image published to GitHub Container Registry:

```bash
docker pull ghcr.io/d7z-project/cache-proxy:main
```

The `main` tag follows the default branch. Pin a release image tag for a
repeatable deployment. See [Quick Start](#quick-start) for a complete container setup.

### Binary

Download a Linux binary and `SHA256SUMS` from
[GitHub Releases](https://github.com/d7z-project/cache-proxy/releases).
Available binary architectures are `amd64`, `arm64`, and `loong64`.
Verify the checksum and make the downloaded binary executable. Use
[config.example.yaml](config.example.yaml) as a starting configuration:

```bash
chmod +x cache-proxy-amd64
./cache-proxy-amd64 -config config.example.yaml -validate
./cache-proxy-amd64 -config config.example.yaml
```

Replace `amd64` with your architecture. The example enables Debian and npm on
`127.0.0.1:18080` and stores data in `./data`.

### From Source

Requires Go 1.26 or newer and Make.

```bash
git clone https://github.com/d7z-project/cache-proxy.git
cd cache-proxy
make cache-proxy
```

Run the resulting `./cache-proxy` with `-config` as shown above.

## Quick Start

This example runs an npm cache accessible from the local machine.
Create `cache-proxy.yaml`:

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

Open [the dashboard](http://127.0.0.1:18080/) to view the instance and its status.
Configure npm to download through the cache:

```bash
npm config set registry http://127.0.0.1:18080/npm
```

This publishes the service only on the host's loopback interface. For shared
access, configure the published address, `server.public_url`, TLS, and access
controls for your deployment.

## Configuration

See [config.example.yaml](config.example.yaml) for the complete configuration
reference, including defaults and an instance example for every mode. Only Debian
and npm are enabled in that file; enable and customize the instances you need.

Configuration uses strict YAML: unknown fields, invalid URLs, duplicate instance
names, and conflicting listeners fail validation. Durations use Go syntax
(`500ms`, `15m`, `24h`); sizes accept binary units such as `MiB` and `GiB`.

```bash
./cache-proxy -config cache-proxy.yaml -validate
```

### Listeners and Instances

`server.bind` sets the main listener, `server.backend` sets the persistent data
directory, and `server.public_url` sets the public address used in dashboard
client instructions.

| Setting | Purpose |
|---|---|
| `name` | Unique identifier using letters, digits, underscores, or hyphens |
| `enabled` | Enable the instance; defaults to `false` |
| `mode` | Protocol mode from [Supported Modes](#supported-modes) |
| `upstream` | Single HTTP(S) upstream base URL |
| `path` | Mount on the main HTTP listener |
| `bind` | Use a dedicated listener; required for OCI |
| `display_url` | Public client address for a dedicated OCI listener |
| `transport` | Outbound proxy, User-Agent, connection limits, and timeouts |
| `refresh.interval` | Repository background polling interval; mutable-object freshness setting |
| `options` | Mode-specific configuration |

Each instance requires `name`, `mode`, `upstream`, and exactly one of `path` or
`bind`. Use a dedicated listener for OCI. Upstream high availability can be
provided by DNS or a load balancer at the configured address.

Use `transport.proxy` for an outbound HTTP, HTTPS, SOCKS5, or SOCKS5H proxy.
Without an explicit setting, HTTP transport follows the proxy environment
variables. Git and OCI upstream credentials are configured in `options.auth`;
credential fields support environment variable expansion.

### Refresh and Rate Limits

Set `refresh: {interval: 30m}` on an instance to adjust its repository
background metadata polling interval or mutable-object freshness setting. The
minimum interval is `1s`.

| Content | Default interval |
|---|---|
| Linux repository metadata and Flatpak summaries (background polling) | 15m |
| npm, PyPI, Cargo, Go and Maven mutable metadata; Flatpak mutable refs | 1m |
| OCI tags | 2m |
| Maven SNAPSHOT files | 5m |
| Git mirror synchronization | 5m, through `options.sync_interval` |

For repository metadata, background polling is scheduled independently of HTTP
freshness. Response reuse still follows the upstream policy, with
`refresh.interval` as the local freshness ceiling and fallback.
Polling is staggered with a stable positive offset from `0` to
`min(interval / 20, 1m)`, so a `1h` interval schedules the next check 60 to 61
minutes after the latest successful validation or publication. A shorter upstream
`max-age` does not accelerate an idle background poll. The interval applies to
each discovered repository root, not to the instance's combined event stream.
A client request that requires validation, an initial fill, or a missing object
can still trigger an earlier request.
The scheduler executes tasks serially; a busy scheduler can delay a due check.

Other mutable objects refresh on demand. For file `http_cache` rules,
`refresh.interval` remains a freshness ceiling and fallback when the upstream
omits a lifetime; without it, caching requires explicit upstream freshness.
Refresh settings do not extend protocol expiry, signed URL validity, or immutable
object retention.

Use `storage.download` to control global concurrency, per-host concurrency,
and request pacing. The `hosts` mapping overrides limits for a hostname with an
optional port. Limits apply across instances, including redirect and authentication
requests. Upstream `429` responses establish cooldowns that honor
`Retry-After`. Tune both refresh intervals and download limits to upstream quotas.

## Client Setup

The dashboard provides client configuration for every enabled instance, including
Maven, Alpine, RPM, Pacman, and Flatpak. The examples below cover common clients.
Replace the hostname with your public address and enable instances at the matching
paths before using them.

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
Keep the distribution, components, and signing keys appropriate to your repository.
For example, standard and flat Debian sources in
`/etc/apt/sources.list.d/cache-proxy.list`:

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

### Security

For shared access, terminate TLS and configure client authentication or network
access controls at your ingress. The optional `metrics.token` protects only the
Prometheus endpoint, not the dashboard or proxy routes. Upstream credentials
authenticate cache-proxy to the upstream; they do not restrict client access.

### Storage and Upgrades

Use a dedicated persistent backend volume with one writer process. Instance data
is stored under `<backend>/instances/<name>/<mode>/`. Send `SIGINT` or `SIGTERM`
for graceful shutdown.

Backend formats are internal and have no compatibility guarantee across builds.
Invalid cache records are treated as misses and cleaned up. Plan for cache
repopulation when upgrading.

`storage.gc.blob` controls unreferenced blob retention, while `storage.cleanup`
controls cleanup batching and dry runs. `storage.orphan_policy` selects reporting
or automatic cleanup of inactive instance data. Object size and temporary download
limits are not a total backend disk quota; monitor available storage.

### Freshness and Offline Use

Mutable metadata uses conditional upstream validation. Cached content can remain
available during transient failures when request, upstream, and protocol policies
allow reuse. Offline availability is limited to content already cached and still
eligible for reuse; this service is not a complete upstream mirror.

Explicit `no-cache` or `max-age=0` requests require validation even when data is
cached. APT normally requests index validation; offline use requires client settings
that permit cache reuse. Existing repository metadata requests that require refresh
wait up to 30 seconds, or the client's shorter deadline, and return `504` on timeout.

Repository metadata is verified before an updated snapshot replaces the cached
one. A failed refresh leaves the committed cache intact, subject to its expiry
and validation requirements. Debian `Valid-Until` remains effective. Unversioned
repository URLs can still change between separate client requests.

For Debian repositories with `Acquire-By-Hash: yes`, indexes are downloaded and
verified on demand, reducing refresh traffic for unused architectures and index
variants. Other Debian repositories cache complete metadata snapshots. The mode
selects this behavior automatically for standard, nested, and flat layouts.

Upstream `no-store` responses are not retained. Repository-tree modes pass root,
directory, and unclassified same-origin resource requests through to the upstream;
Go and OCI expose their protocol endpoints only.

### Monitoring

| Endpoint | Purpose |
|---|---|
| `/` | Dashboard, instance status, and client configuration |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Disk usage history |
| `/-/status/events` | Maintenance results and errors, including target, cause, phase, and queue time; accepts `limit` |
| `/-/status/network` | Instance and upstream request statistics |
| `/metrics` | Prometheus metrics; configurable through `metrics.path` |

Review maintenance errors in the dashboard or events API. For container logs:

```bash
docker logs --tail 100 -f cache-proxy
```

Each metadata event represents work for a concrete repository root.
Events distinguish upstream checks (`unchanged` or `staged`) from
snapshot publication (`published`). The target and reason identify which root
ran and whether it was a periodic check, client validation, initial fill,
recovery, or retry. Queue time and execution time are reported separately.

For repeated `429` responses, review per-host limits and metadata refresh intervals.
For timeouts, check upstream reachability and instance transport settings.
For storage errors, check backend permissions, free space, and download limits.

## Testing

Run the local checks with Go 1.26 or newer and Make:

```bash
make fmt
make vet
make test
make test-race
make test-fuzz
```

End-to-end tests require Docker or Podman on native Linux and run the service
and native package clients in host-network containers:

```bash
make test-e2e
E2E_RUNTIME=podman E2E_SUITE=deb make test-e2e
```

`E2E_SUITE` accepts any supported mode; omitting it runs every mode.

Report bugs through [issues](https://github.com/d7z-project/cache-proxy/issues),
including your mode, relevant configuration without credentials, and error logs.
Pull requests should include tests for the affected behavior.

## License

[MIT](LICENSE)
