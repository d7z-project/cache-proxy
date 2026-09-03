# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

`cache-proxy` is a self-hosted, read-only caching proxy for package registries,
Linux repositories, OCI registries, Flatpak/OSTree repositories, Git smart
HTTP, and regular HTTP files.

Each mode implements its upstream protocol directly. Mutable metadata is
refreshed without mixing repository states, immutable content is verified when
a digest is available, and concurrent requests share in-progress downloads.

## Features

- Protocol-aware caching for thirteen package and repository formats.
- Streaming downloads with process-wide size, spool, and concurrency limits.
- Atomic metadata publication for Linux repositories and Flatpak/OSTree.
- Digest verification before immutable objects are stored.
- Conditional revalidation for mutable metadata and stale responses during
  transient upstream failures.
- Persistent per-instance storage with bounded cleanup jobs.
- Strict YAML configuration and one configured upstream per instance.
- Prometheus metrics, JSON status endpoints, and a built-in dashboard.
- Read-only upstream access; publication and deletion operations are rejected
  locally.

## Supported Modes

| Mode | Protocol and cached content |
|---|---|
| `file` | HTTP files with configurable cache policies |
| `npm` | Packuments, verified tarballs, and audit queries |
| `go` | Go module proxy and optional checksum database |
| `maven` | Maven metadata, artifacts, and checksums |
| `cargo` | Sparse indexes, verified crates, and fetch-only Git indexes |
| `pypi` | PyPI Simple HTML/JSON and verified distributions |
| `deb` | Debian standard, nested, and flat repositories with transparent auxiliary files |
| `apk` | Alpine indexes, packages, and package sidecars |
| `rpm` | rpm-md metadata and RPM artifacts |
| `pacman` | Pacman databases, packages, deltas, and sidecars |
| `flatpak` | Flatpak/OSTree summaries, objects, and static deltas |
| `oci` | Pull-only OCI Distribution manifests and blobs |
| `git` | Fetch-only Git smart HTTP backed by local bare mirrors |

Repository layout, metadata roots, compression variants, and object types are
detected by the selected mode. Per-repository path rules are not required.

## Installation

### Container

The `main` image tracks the default branch. Release tags are published to the
same registry.

```bash
docker pull ghcr.io/d7z-project/cache-proxy:main
```

### Release Binary

Linux binaries for `amd64`, `arm64`, and `loong64` are attached to each
[GitHub release](https://github.com/d7z-project/cache-proxy/releases).

### Build From Source

Building requires Go 1.25 or newer.

```bash
git clone https://github.com/d7z-project/cache-proxy.git
cd cache-proxy
make cache-proxy
```

## Quick Start

Create `cache-proxy.yaml`:

```yaml
server:
  bind: 0.0.0.0:18080
  backend: /data
  public_url: http://127.0.0.1:18080
  status:
    disk_sample_interval: 15m
    disk_history_window: 24h
    event_limit: 500

metrics:
  path: /metrics
  token: ""

storage:
  gc:
    blob: 24h
  cleanup:
    dry_run: false
    batch_size: 500
  orphan_policy: report
  download:
    max_active: 256
    max_active_per_host: 16
    min_interval: 5ms
    max_cache_object_size: 2GiB
    max_active_spool_size: 8GiB

instances:
  - name: npm
    enabled: true
    mode: npm
    path: /npm
    upstream: https://registry.npmjs.org
```

Start the container:

```bash
docker run --name cache-proxy \
  -p 18080:18080 \
  -v "$PWD/cache-proxy.yaml:/etc/cache-proxy.yaml:ro" \
  -v cache-proxy-data:/data \
  ghcr.io/d7z-project/cache-proxy:main \
  -config /etc/cache-proxy.yaml
```

The dashboard is available at `http://127.0.0.1:18080/`. Configure npm to use
the proxy with:

```bash
npm config set registry http://127.0.0.1:18080/npm
```

Validate a configuration without opening listeners:

```bash
./cache-proxy -config ./cache-proxy.yaml -validate
```

## Configuration

Configuration is decoded in strict mode. Unknown fields, malformed URLs,
duplicate instance names, and listener conflicts prevent startup. See
[`config.local.yaml`](config.local.yaml) for a complete multi-instance
configuration.

### Global Settings

| Field | Default | Description |
|---|---:|---|
| `server.bind` | `127.0.0.1:18080` | Main HTTP listener |
| `server.backend` | `/tmp/cache-proxy` | Persistent storage root |
| `server.public_url` | unset | Trusted external URL used in generated client commands |
| `server.status.disk_sample_interval` | `15m` | Disk history sampling interval |
| `server.status.disk_history_window` | `24h` | Retained disk history |
| `server.status.event_limit` | `500` | Maximum retained status events |
| `metrics.path` | `/metrics` | Prometheus endpoint |
| `metrics.token` | unset | Optional Bearer token for metrics |
| `storage.gc.blob` | `24h` | Unreferenced blob retention |
| `storage.cleanup.dry_run` | `false` | Report cleanup candidates without deleting them |
| `storage.cleanup.batch_size` | `500` | Objects inspected per cleanup pass |
| `storage.orphan_policy` | `report` | `report` or `auto` cleanup of inactive instance storage |
| `storage.download.max_active` | `256` | Process-wide active upstream requests |
| `storage.download.max_active_per_host` | `16` | Active upstream requests per host |
| `storage.download.min_interval` | `5ms` | Minimum request start interval per host |
| `storage.download.max_cache_object_size` | `2GiB` | Maximum size of one cached object |
| `storage.download.max_active_spool_size` | `8GiB` | Process-wide temporary spool budget |

Host-specific admission limits can be set under
`storage.download.hosts.<host>`. A host override accepts `max_active` and
`min_interval`; host names are normalized and may include a port.

### Instances

Each instance requires `name`, `enabled`, `mode`, and one HTTP(S) `upstream`.
It must define exactly one listener:

- `path` mounts the instance below `server.bind`.
- `bind` creates a dedicated listener and is required by `oci`.
- `display_url` controls the client-facing OCI address shown on the dashboard.
- `transport` accepts `proxy`, `ua`, `dial_timeout`, `header_timeout`,
  `idle_body_timeout`, `max_request_duration`, and `max_idle_conns`.

An instance represents one upstream repository or registry base URL. Requests
below a path mount are appended to that base without assigning a special file
meaning to the instance root. Put DNS or a load balancer in front of the URL
when upstream high availability is required. Protocol endpoints such as Go
SumDB and an OCI token realm remain separate auxiliary endpoints.

### Mode Options

Options are placed below an instance's `options` mapping.

| Mode | Options |
|---|---|
| `file` | `pass_headers`; `rules[]` with `match` and `policy` (`http_cache`, `immutable`, or `passthrough`) |
| `git` | `auth`, `sync_interval`, `operation_timeout` |
| `oci` | `auth` with type `none`, `basic`, or `bearer` |
| `go` | `sumdb`, `goprivate`, `disable_module_fetch_header` |

Other modes do not require mode-specific options. Git and OCI credential
values support shell-style environment expansion such as `$REGISTRY_TOKEN`.
npm and PyPI cross-origin download targets use per-instance signed routes
derived from upstream metadata. Cargo crate targets are resolved from the
cached sparse index and verified against its checksum.

## Client Configuration

Examples for path-mounted instances:

```bash
npm config set registry https://mirrors.example.com/npm
go env -w GOPROXY=https://mirrors.example.com/go
pip install --index-url https://mirrors.example.com/pypi/simple PACKAGE
```

OCI uses a dedicated listener:

```yaml
instances:
  - name: docker_hub
    enabled: true
    mode: oci
    bind: 0.0.0.0:5000
    display_url: registry.example.com:5000
    upstream: https://registry-1.docker.io
```

```bash
docker pull registry.example.com:5000/IMAGE:TAG
```

Linux package managers use the configured instance URL as their repository or
mirror base. Cargo sparse registry URLs must end in `/`. The dashboard renders
the complete client command for every configured instance.

## Runtime Behavior

### Cache Consistency

Mutable protocol pointers use conditional requests against their configured
upstream. A client request with `Cache-Control: no-cache` or `max-age=0`
requests revalidation. Updated content is published atomically; a transient
upstream error can continue serving the committed response.

Linux repository and Flatpak metadata is exposed only from a complete,
verified generation. Packages and sidecars use stable cache keys, so they
remain available while a metadata refresh is pending or has failed. Repository
metadata refresh runs every 15 minutes and can be scheduled immediately by
revalidating the current anchor.

Request paths are classified after one percent-decoding pass. Equivalent legal
encodings share a cache identity, while encoded separators and parent traversal
remain invalid. A request for a path-mounted instance without its trailing
slash receives a permanent redirect that preserves the query. Repository roots,
directory trailing slashes, and unclassified same-origin read resources pass
through transparently in file, npm, Maven, Cargo, PyPI, Debian, APK, RPM,
Pacman, Git, and Flatpak modes. File rules and Maven's open repository object
space may cache non-directory resources; protocol-specific modes cache only
objects they recognize. Go module proxy and OCI Distribution paths retain their
strict protocol endpoint boundaries.

### Storage

Each enabled instance uses an isolated directory:

```text
<backend>/instances/<name>/<mode>/
  blobs/
  state/
  work/
```

Backend contents are private application state. Use a dedicated persistent
volume and allow only one `cache-proxy` process to write to it. Downloads are
streamed while cache publication continues within the configured disk and
concurrency limits.

### Upstream Access

The proxy sends only read operations upstream. Git upload-pack, Cargo's
fetch-only Git operation, and npm audit use protocol-required `POST` requests
without modifying upstream state. Other mutation methods are rejected before
authentication or an upstream connection.

## Observability

| Endpoint | Description |
|---|---|
| `/` | Dashboard and generated client commands |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Bounded disk usage history |
| `/-/status/events` | Recent maintenance events; accepts `limit` |
| `/-/status/network` | Instance, upstream, and admission statistics |
| `metrics.path` | Prometheus metrics |

Send `SIGINT` or `SIGTERM` for graceful shutdown.

## Development

Run the standard checks with:

```bash
make fmt
make tidy
make vet
make test
make test-race
make test-fuzz
```

The end-to-end suite requires Docker or Podman on native Linux. All package
manager commands run inside host-network containers.

```bash
make test-e2e
E2E_RUNTIME=podman E2E_SUITE=deb make test-e2e
```

`E2E_SUITE` accepts `file`, `npm`, `go`, `maven`, `cargo`, `pypi`, `deb`,
`apk`, `rpm`, `pacman`, `git`, `oci`, or `flatpak`. An unset suite runs every
mode. Each case verifies cold population, warm reuse, upstream refresh, and
reuse of persisted data while the fixture is offline.

## License

Licensed under the [MIT License](LICENSE).
