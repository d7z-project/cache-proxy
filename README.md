# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

`cache-proxy` is a self-hosted, read-only caching proxy for package managers,
Linux repositories, OCI registries, Flatpak/OSTree repositories, Git smart
HTTP, and regular HTTP files.

It understands each supported protocol instead of treating every response as a
generic HTTP object. Mutable metadata is refreshed from its configured
upstream, immutable content is verified when a digest is available, and
concurrent cache misses share the same upstream transfer.

## Highlights

- One process can serve multiple isolated proxy instances.
- Thirteen protocol modes with native request classification and cache keys.
- Streaming downloads with global and per-host concurrency limits.
- Atomic metadata snapshots for Linux repositories and Flatpak/OSTree.
- Conditional refresh and stale serving for transient upstream failures.
- Persistent cache storage with bounded background cleanup.
- Strict YAML configuration with one upstream per instance.
- Built-in dashboard, JSON status endpoints, and Prometheus metrics.

## Supported Modes

| Mode | Cached content |
|---|---|
| `file` | HTTP files selected by configurable cache rules |
| `npm` | Packuments and verified tarballs; audit queries pass through |
| `go` | Go module proxy content and an optional checksum database |
| `maven` | Maven metadata, artifacts, signatures, and checksums |
| `cargo` | Sparse indexes, verified crates, and fetch-only Git indexes |
| `pypi` | PyPI Simple HTML/JSON and verified distributions |
| `deb` | Debian standard, nested, and flat repositories |
| `apk` | Alpine indexes, packages, and package sidecars |
| `rpm` | rpm-md metadata and RPM packages |
| `pacman` | Pacman databases, packages, deltas, and sidecars |
| `flatpak` | Flatpak/OSTree summaries, objects, and static deltas |
| `oci` | OCI Distribution manifests and blobs |
| `git` | Git smart HTTP backed by a local bare mirror |

Repository roots, metadata paths, compression variants, and package objects
are detected by the selected mode. They do not require per-repository path
rules.

## Quick Start

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

Start the container:

```bash
docker run --name cache-proxy \
  -p 18080:18080 \
  -v "$PWD/cache-proxy.yaml:/etc/cache-proxy.yaml:ro" \
  -v cache-proxy-data:/data \
  ghcr.io/d7z-project/cache-proxy:main \
  -config /etc/cache-proxy.yaml
```

Open `http://127.0.0.1:18080/` to view the dashboard, or configure npm
directly:

```bash
npm config set registry http://127.0.0.1:18080/npm
```

## Installation

### Container Image

The `main` image follows the default branch. Versioned release images are
published to the same registry.

```bash
docker pull ghcr.io/d7z-project/cache-proxy:main
```

### Release Binary

Linux binaries and SHA256 checksums are attached to each
[GitHub release](https://github.com/d7z-project/cache-proxy/releases). Binary
builds are published for `amd64`, `arm64`, and `loong64`.

### Build From Source

Building requires Go 1.26 or newer.

```bash
git clone https://github.com/d7z-project/cache-proxy.git
cd cache-proxy
make cache-proxy
```

Run the binary with a configuration file:

```bash
./cache-proxy -config ./cache-proxy.yaml
```

Validate configuration without opening listeners:

```bash
./cache-proxy -config ./cache-proxy.yaml -validate
```

## Configuration

Configuration uses strict YAML decoding. Unknown fields, malformed URLs,
duplicate instance names, and listener conflicts prevent startup. See
[`config.example.yaml`](config.example.yaml) for a commented multi-instance example.

All duration values use Go duration syntax, such as `500ms`, `15m`, or `24h`.
Byte sizes accept binary suffixes such as `512MiB` and `2GiB`.

### Global Settings

| Field | Default | Description |
|---|---:|---|
| `server.bind` | `127.0.0.1:18080` | Main HTTP listener |
| `server.backend` | `/tmp/cache-proxy` | Persistent storage root |
| `server.public_url` | request URL | External base URL used by dashboard commands |
| `server.status.disk_sample_interval` | `15m` | Disk usage sampling interval |
| `server.status.disk_history_window` | `24h` | Retained disk history |
| `server.status.event_limit` | `500` | Retained maintenance events |
| `metrics.path` | `/metrics` | Prometheus endpoint |
| `metrics.token` | unset | Optional Bearer token for metrics |
| `storage.gc.blob` | `24h` | Unreferenced blob retention |
| `storage.cleanup.dry_run` | `false` | Report cleanup candidates without deleting them |
| `storage.cleanup.batch_size` | `500` | Objects inspected per cleanup pass |
| `storage.orphan_policy` | `report` | `report` or `auto` cleanup for inactive instance data |
| `storage.download.max_active` | `256` | Process-wide active upstream requests |
| `storage.download.max_active_per_host` | `16` | Active upstream requests per host |
| `storage.download.min_interval` | `5ms` | Minimum request start interval per host |
| `storage.download.max_cache_object_size` | `2GiB` | Maximum cached object size |
| `storage.download.max_active_spool_size` | `8GiB` | Process-wide temporary spool budget |

Host-specific admission settings use
`storage.download.hosts.<host>.max_active` and `min_interval`. Host names are
normalized and may include a port.

Admission retains at most 4096 additional dynamic hosts. Idle dynamic entries
expire after their pacing interval and any upstream cooldown; cleanup does not
depend on dashboard access. New hosts are rejected at capacity until space is
available, without interrupting admitted transfers.

### Instances

Every instance configures one upstream repository or registry.

| Field | Required | Description |
|---|---:|---|
| `name` | yes | Unique identifier matching `[a-zA-Z0-9_-]+` |
| `enabled` | yes | Enables the instance |
| `mode` | yes | One of the supported modes |
| `upstream` | yes | Absolute HTTP(S) upstream base URL |
| `path` | one listener | Mount below `server.bind` |
| `bind` | one listener | Dedicated listener; required by `oci` |
| `display_url` | no | Client-facing address for a dedicated OCI listener |
| `transport` | no | Per-instance HTTP transport settings |
| `refresh.interval` | no | Mutable freshness limit and repository polling interval, at least `1s` |
| `options` | no | Mode-specific settings |

Exactly one of `path` or `bind` is required. Each instance has one configured
upstream; DNS or an external load balancer can provide upstream high
availability.

The optional `transport` mapping accepts `proxy`, `ua`, `dial_timeout`,
`header_timeout`, `idle_body_timeout`, `max_request_duration`, and
`max_idle_conns`. Supported proxy schemes are HTTP, HTTPS, SOCKS5, and
SOCKS5H.

`idle_body_timeout` bounds each blocked upstream body read, including OCI
token responses. Downloads that keep making progress remain subject to
`max_request_duration`.

### Refresh Timing

Set `refresh: {interval: 30m}` on an instance to control its mutable metadata.
Linux repository and Flatpak anchors default to 15 minutes. npm, PyPI, Cargo,
Go mutable metadata, Maven metadata and Flatpak mutable references default to
one minute; OCI tags default to two minutes, Maven SNAPSHOT files to five minutes.
Git uses `options.sync_interval` instead. File `http_cache` rules use the configured
interval as a freshness ceiling and a fallback when the upstream gives no lifetime;
without it, file responses require an explicit upstream freshness lifetime.

Upstream cache policy can require earlier validation. The interval does not
extend signed URL validity, protocol expiry, immutable object lifetime or retention.
Discovered repository roots are checked serially with stable 80-100% interval
offsets; other mutable objects are revalidated on demand. Shared-host admission
limits and upstream `Retry-After` still apply. These settings reduce bursts but
do not guarantee an upstream quota will never be exceeded.

### Mode Options

Mode options are placed below the instance's `options` mapping.

| Mode | Options |
|---|---|
| `file` | `pass_headers`; `rules` entries with `match` and `policy` |
| `git` | `auth`, `sync_interval`, `operation_timeout` |
| `go` | `sumdb`, `goprivate`, `disable_module_fetch_header` |
| `oci` | `auth` |

File rule policies are `http_cache`, `immutable`, and `passthrough`. Git auth
supports `basic` and `token`; OCI auth supports `none`, `basic`, and `bearer`.
Credential values support shell-style environment expansion. Other modes do
not require mode-specific options.

Git upload-pack request bodies are limited to 16 MiB and must arrive within
`operation_timeout`. Oversized requests receive `413`; timed-out reads receive
`408`. Mirror synchronization also uses this timeout. When a local reader holds
the mirror, synchronization is deferred for 2 seconds while other maintenance
tasks continue.

## Client Setup

The dashboard renders a client command for every configured instance. Common
path-mounted examples are:

```bash
npm config set registry https://mirrors.example.com/npm
go env -w GOPROXY=https://mirrors.example.com/go
pip install --index-url https://mirrors.example.com/pypi/simple PACKAGE
git clone https://mirrors.example.com/git
```

Cargo sparse registry URLs must retain a trailing slash:

```toml
[source.cache-proxy]
registry = "sparse+https://mirrors.example.com/cargo/"

[source.crates-io]
replace-with = "cache-proxy"
```

Linux package managers use the instance URL as their repository or mirror
base. Debian standard and flat repository examples are:

```text
deb https://mirrors.example.com/debian bookworm main
deb [trusted=yes] https://mirrors.example.com/debian-flat ./
```

OCI instances use dedicated listeners:

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

## Cache Behavior

Mutable metadata is conditionally refreshed against its configured upstream.
Committed stale data remains available during transient upstream failures when
the request and upstream cache policy permit it. Explicit `no-cache` or
`max-age=0` requests require validation; `HEAD` follows the same policy as `GET`.
Response age includes time spent downloading and publishing, and successful
conditional validation updates freshness without changing the representation.
Native clients can request validation on every invocation. In particular, APT
normally sends `max-age=0` for indexes. Offline cache reuse therefore requires
client settings that permit reuse; a forced validation fails when upstream is
unavailable, even when the object exists locally.
Large objects stream to the requesting client while cache publication is in
progress, and concurrent requests for the same object share one transfer.

Linux repository and Flatpak/OSTree metadata is published as an atomically
verified snapshot. Package artifacts and sidecars use stable cache identities,
so an unsuccessful metadata refresh does not invalidate already cached package
content. Digest-addressed objects are verified before publication.

A strict request for an existing repository anchor waits up to 30 seconds for
validation and any required candidate publication; a shorter client deadline
takes precedence. Timeout returns 504 while scheduled work continues. A failed
candidate does not delete the committed generation or cached packages. The first
anchor request streams upstream directly. An upstream no-store policy retires
cached metadata routing for that root so subsequent reads pass through.
Debian `Valid-Until` bounds anchor reuse. Atomic publication keeps a generation
coherent but cannot provide cross-request snapshot isolation for unversioned URLs.

Flatpak static deltas use a finite cache lifetime and rely on client-side
content verification.

Repository-tree modes preserve repository roots and directory trailing
slashes. Safe, unclassified same-origin read resources pass through to the
configured upstream. Go module and OCI modes keep their protocol endpoint
boundaries. Upstream access is limited to read-side protocol operations.

## Operations

Each enabled instance uses an isolated directory:

```text
<backend>/instances/<name>/<mode>/
  blobs/
  state/
  work/
```

Backend contents are private application state. Use a dedicated persistent
volume and allow one `cache-proxy` process to write to it. Send `SIGINT` or
`SIGTERM` for graceful shutdown.

Backend formats are internal to the running build, with no compatibility
guarantee across builds. Response records are strictly validated; invalid records
are treated as cache misses and reclaimed by cleanup. Repository snapshots retain
their protocol validation and atomic publication rules.

Metadata refresh and metadata GC schedules are checkpointed after completion
and restored on restart. Maintenance events expose the task result and full
error message in the dashboard and status API.

### Status and Metrics

| Endpoint | Description |
|---|---|
| `/` | Dashboard, instance status, and client configuration |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Bounded disk usage history |
| `/-/status/events` | Recent maintenance events; accepts `limit` |
| `/-/status/network` | Instance, upstream, and admission statistics |
| `metrics.path` | Prometheus metrics |

Upstream statistics group requests by origin (scheme, host, and port), excluding
credentials and resource paths. Each instance retains up to 64 origins plus an
`other` aggregate. Nonstandard HTTP methods share the `OTHER` metric label.

## Development

Protocol implementations live in `pkg/proxy/<mode>`, with each mode's `Plan`
registered in `pkg/app/drivers.go`. Shared response storage lives in `storeio`,
repository snapshots in `filerepo`, and upstream HTTP handling in `transport`.
See [AGENTS.md](AGENTS.md) for engineering and test requirements.

Run the local checks with:

```bash
make fmt
make tidy
make vet
make test
make test-race
make test-fuzz
```

The end-to-end suite requires Docker or Podman on native Linux. Package manager
commands run inside host-network containers.

```bash
make test-e2e
E2E_RUNTIME=podman E2E_SUITE=deb make test-e2e
```

`E2E_SUITE` accepts any supported mode. Without it, the runner tests every
mode. Each case covers cold population, warm reuse, upstream updates, and
persisted offline reuse.

## License

Licensed under the [MIT License](LICENSE).
