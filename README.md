# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

`cache-proxy` is a self-hosted, read-only caching proxy for package registries,
Linux repositories, OCI registries, Flatpak/OSTree repositories, Git smart
HTTP, and ordinary HTTP files.

It understands each supported protocol instead of treating every request as a
generic HTTP cache entry. This preserves repository metadata consistency,
verifies content when the protocol provides a digest, and keeps package
downloads available independently of metadata refreshes.

## Highlights

- Thirteen package and repository modes in one process.
- Streaming downloads with bounded object size, spool usage, and upstream
  concurrency.
- Concurrent cache misses for the same object share one upstream transfer when
  supported by the mode.
- Digest verification before immutable objects are committed to storage.
- Atomic metadata generations for Linux repositories and Flatpak/OSTree.
- Conditional revalidation for mutable objects and finite retention for
  unverified content.
- Strict YAML configuration, isolated instance storage, backend locking, and
  bounded cleanup jobs.
- Prometheus metrics, JSON status endpoints, and a built-in dashboard.
- Read-only upstream behavior: publishing, pushing, deleting, and other
  mutation operations are rejected locally.

## Supported Modes

| Mode | Support |
|---|---|
| `file` | HTTP files with configurable cache policies |
| `git` | Fetch-only Git smart HTTP and local bare mirrors |
| `oci` | Pull-only OCI Distribution manifests and blobs |
| `npm` | Packuments, verified tarballs, and audit queries |
| `go` | Go module proxy and optional checksum database |
| `maven` | Maven metadata, artifacts, and checksums |
| `cargo` | Sparse indexes, verified crates, and fetch-only Git indexes |
| `pypi` | PyPI Simple HTML/JSON and verified distributions |
| `flatpak` | Flatpak/OSTree summaries, objects, and static deltas |
| `apk` | Alpine indexes and packages |
| `deb` | Debian standard, nested, and flat repositories |
| `rpm` | rpm-md metadata and RPM artifacts |
| `pacman` | Pacman databases, packages, deltas, and signatures |

Repository layout, metadata roots, compression variants, and object types are
detected by the selected mode. They do not require per-repository rules.

## Installation

### Container Image

The `main` image tracks the default branch. Releases are published with version
tags.

```bash
docker pull ghcr.io/d7z-project/cache-proxy:main
```

### Release Binary

Linux binaries for `amd64`, `arm64`, and `loong64` are available from the
[Releases](https://github.com/d7z-project/cache-proxy/releases) page.

### Build From Source

Go 1.25 or newer is required.

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
  public_url: https://mirrors.example.com
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

  - name: debian
    enabled: true
    mode: deb
    path: /debian
    upstream: https://deb.debian.org/debian

  - name: docker_hub
    enabled: true
    mode: oci
    bind: 0.0.0.0:5000
    display_url: registry.example.com:5000
    upstream: https://registry-1.docker.io
```

Run the published image:

```bash
docker run --name cache-proxy \
  -p 18080:18080 \
  -p 5000:5000 \
  -v "$PWD/cache-proxy.yaml:/etc/cache-proxy.yaml:ro" \
  -v cache-proxy-data:/data \
  ghcr.io/d7z-project/cache-proxy:main \
  -config /etc/cache-proxy.yaml
```

Open `http://127.0.0.1:18080/` to view the dashboard and client setup
commands.

With a locally installed binary, validate a configuration without starting
listeners:

```bash
./cache-proxy -config ./cache-proxy.yaml -validate
```

## Configuration

Configuration uses strict YAML decoding. Unknown fields, invalid URLs,
duplicate instance names, and conflicting listeners fail before startup. See
[`config.local.yaml`](config.local.yaml) for a larger multi-instance example.

### Global Settings

| Field | Default | Description |
|---|---:|---|
| `server.bind` | `127.0.0.1:18080` | Main HTTP listener |
| `server.backend` | `/tmp/cache-proxy` | Persistent storage root |
| `server.public_url` | unset | Trusted external URL used in generated client configuration |
| `server.status.disk_sample_interval` | `15m` | Disk history sampling interval |
| `server.status.disk_history_window` | `24h` | Retained disk history |
| `server.status.event_limit` | `500` | Maximum retained status events |
| `metrics.path` | `/metrics` | Prometheus endpoint |
| `metrics.token` | unset | Optional Bearer token for metrics |
| `storage.gc.blob` | `24h` | Unreferenced blob retention |
| `storage.cleanup.batch_size` | `500` | Objects inspected per cleanup pass |
| `storage.orphan_policy` | `report` | `report` or `auto` cleanup of inactive instance stores |
| `storage.download.max_active` | `256` | Process-wide active upstream requests |
| `storage.download.max_active_per_host` | `16` | Active requests per upstream host |
| `storage.download.min_interval` | `5ms` | Minimum request start interval per host |
| `storage.download.max_cache_object_size` | `2GiB` | Maximum size of one cached object |
| `storage.download.max_active_spool_size` | `8GiB` | Process-wide temporary spool budget |

Host-specific admission limits can be set below
`storage.download.hosts.<host>`. Host keys are normalized names with an
optional port.

### Instances

Every instance requires `name`, `enabled`, `mode`, and one HTTP(S) `upstream`.
It must define exactly one listener:

- `path` mounts the instance below the main listener.
- `bind` creates a dedicated listener and is required by `oci`.
- `display_url` sets the client-facing OCI address shown by the dashboard.
- `transport` configures `proxy`, `ua`, `dial_timeout`, `header_timeout`,
  `idle_body_timeout`, `max_request_duration`, and `max_idle_conns`.

An instance never combines repositories or fails over between configured
mirrors. Use DNS or a load balancer in front of the configured upstream when
upstream high availability is required. Protocol endpoints such as Go SumDB
and an OCI registry's token realm remain separate from the instance's content
upstream.

### Mode Options

| Mode | Options |
|---|---|
| `file` | `pass_headers`; `rules[]` with `match` and `policy` (`http_cache`, `immutable`, or `passthrough`) |
| `git` | `auth`, `sync_interval`, `operation_timeout` |
| `oci` | `auth` with type `none`, `basic`, or `bearer` |
| `go` | `sumdb`, `goprivate`, `disable_module_fetch_header` |

Git and OCI credentials may reference environment variables. Cargo and PyPI
authorize download targets from signed routes derived from upstream metadata;
clients cannot supply arbitrary download hosts.

## Client Setup

Examples for path-mounted instances:

```bash
npm config set registry https://mirrors.example.com/npm
go env -w GOPROXY=https://mirrors.example.com/go
pip install --index-url https://mirrors.example.com/pypi/simple PACKAGE
```

OCI instances use their dedicated listener:

```bash
docker pull registry.example.com:5000/IMAGE:TAG
```

Linux package managers use the instance URL as their repository or mirror
base. Cargo sparse registry URLs must end in `/`. The dashboard provides the
complete command for every configured instance.

## Cache Behavior

Each enabled instance stores its state below:

```text
<backend>/instances/<name>/<mode>-v4/
  blobs/
  state/
  work/
```

Only one process may write to a backend. Downloads are streamed to clients
while cache publication continues under bounded disk and concurrency limits.
Mutable objects use validators from the same upstream; range responses are not
stored as complete objects.

Linux repository and Flatpak metadata is published as an atomic generation
only after all required objects have been downloaded and verified. Clients
therefore do not observe metadata assembled from different upstream versions.
Packages and sidecars use stable object caches and remain available while a
metadata refresh is pending or has failed.

The proxy sends only read operations upstream. Git upload-pack, Cargo's
fetch-only Git operation, and npm audit use protocol-required `POST` requests
without mutating upstream state. Other mutation methods are rejected before
authentication or an upstream connection.

## Observability

| Endpoint | Description |
|---|---|
| `/` | Dashboard and client setup commands |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Bounded disk usage history |
| `/-/status/events` | Recent maintenance events; accepts `limit` |
| `/-/status/network` | Instance, upstream, and admission statistics |
| `metrics.path` | Prometheus metrics |

Send `SIGINT` or `SIGTERM` for graceful shutdown.

## Development

```bash
make fmt
make tidy
make vet
make test
make test-race
make test-fuzz
```

The fuzz smoke suite includes bounded parallel cache fills, joined flights,
early client disconnects, declared-size mismatches, and upstream admission
contention and lifecycle shutdown.

Run the containerized end-to-end suite with Docker or Podman on native Linux:

```bash
make test-e2e
E2E_RUNTIME=podman E2E_SUITE=deb-apk make test-e2e
```

`E2E_SUITE` accepts `registries`, `deb-apk`, `rpm-pacman`, or
`git-oci-flatpak`. An unset suite runs all modes. The suite builds deterministic
local repositories and runs every proxy, fixture, readiness probe, and native
package client in host-network containers. It does not run package managers on
the host.

## License

Licensed under the [MIT License](LICENSE).
