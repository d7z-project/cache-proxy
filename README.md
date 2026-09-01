# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

`cache-proxy` is a self-hosted streaming cache for package registries, Linux repositories, OCI registries, Flatpak/OSTree repositories, Git smart HTTP, and generic HTTP files.

It stores large responses without buffering them in memory, coalesces concurrent cache fills, revalidates mutable metadata, and verifies content when the upstream protocol provides a digest.

## Features

- One process can expose multiple independently configured proxy instances.
- Metadata, immutable content, and passthrough requests use fixed cache semantics.
- Concurrent downloads share one upstream transfer while clients stream the result.
- Global and per-host admission limits control upstream concurrency and request rate.
- Debian, RPM, OCI, Flatpak, Cargo, and other digest-aware modes verify content before publication.
- Prometheus metrics, JSON status endpoints, and a built-in instance dashboard are included.
- Configuration is strict YAML; unknown fields fail validation.

## Supported modes

| Mode | Upstreams | Purpose |
|---|---:|---|
| `file` | one or more | Generic HTTP files with configurable path rules |
| `git` | one | Git smart HTTP backed by a local bare mirror |
| `oci` | one | OCI/Docker registry manifests and blobs |
| `npm` | one | npm registry metadata and tarballs |
| `go` | one | Go module proxy and optional checksum database proxy |
| `maven` | one | Maven metadata and artifacts |
| `cargo` | one | Cargo sparse index and crate downloads |
| `pypi` | one | PyPI Simple API and package files |
| `flatpak` | one or more | Flatpak descriptors and OSTree content |
| `apk` | one or more | Alpine repositories |
| `deb` | one | Debian repositories with Release-anchored metadata verification |
| `rpm` | one | RPM repositories with repomd-anchored metadata verification |
| `pacman` | one or more | Arch Linux repositories |

## Installation

### Release binary

Linux binaries for `amd64`, `arm64`, and `loong64` are published on the [Releases](https://github.com/d7z-project/cache-proxy/releases) page.

### Container image

The `main` image tracks the default branch. Version tags are published for releases.

```bash
docker pull ghcr.io/d7z-project/cache-proxy:main

docker run --name cache-proxy \
  -p 18080:18080 \
  -p 5000:5000 \
  -v "$PWD/cache-proxy.yaml:/etc/cache-proxy.yaml:ro" \
  -v cache-proxy-data:/data \
  ghcr.io/d7z-project/cache-proxy:main \
  -config /etc/cache-proxy.yaml
```

Use `/data` as `server.backend` when running this container. Port `5000` is only needed when the configuration includes the OCI example below.

### Build from source

Go 1.25 or newer is required.

```bash
git clone https://github.com/d7z-project/cache-proxy.git
cd cache-proxy
make cache-proxy
```

## Quick start

Create `cache-proxy.yaml`:

```yaml
server:
  bind: 0.0.0.0:18080
  backend: /tmp/cache-proxy
  status:
    disk_sample_interval: 15m
    disk_history_window: 24h
    event_limit: 500

metrics:
  path: /metrics
  token: replace-me

cache:
  metadata_ttl: 2m
  retention: 720h

storage:
  gc:
    blob: 24h
  cleanup:
    dry_run: false
    batch_size: 500
  download:
    max_active: 256
    max_active_per_host: 16
    min_interval: 5ms
    hosts:
      registry.npmjs.org:
        max_active: 24

instances:
  - name: npm
    enabled: true
    mode: npm
    path: /npm
    upstreams:
      - https://registry.npmjs.org

  - name: debian
    enabled: true
    mode: deb
    path: /debian
    upstreams:
      - https://deb.debian.org/debian

  - name: registry
    enabled: true
    mode: oci
    bind: 0.0.0.0:5000
    display_url: registry.example.com:5000
    upstreams:
      - https://registry-1.docker.io
```

Validate the configuration before starting the service:

```bash
./cache-proxy -config ./cache-proxy.yaml -validate
./cache-proxy -config ./cache-proxy.yaml
```

The home page at `http://localhost:18080/` lists configured instances and client setup commands.

## Configuration

Configuration uses Go duration syntax such as `5ms`, `30s`, or `24h`. Environment references used by Git and OCI credentials are expanded when the configuration is loaded.

### Defaults

| Field | Default |
|---|---:|
| `server.bind` | `127.0.0.1:18080` |
| `server.backend` | `/tmp/cache-proxy` |
| `server.status.disk_sample_interval` | `15m` |
| `server.status.disk_history_window` | `24h` |
| `server.status.event_limit` | `500` |
| `metrics.path` | `/metrics` |
| `cache.metadata_ttl` | `2m` |
| `cache.retention` | `720h` |
| `storage.gc.blob` | `24h` |
| `storage.cleanup.batch_size` | `500` |
| `storage.download.max_active` | `256` |
| `storage.download.max_active_per_host` | `16` |
| `storage.download.min_interval` | `5ms` |

`metrics.token` enables Bearer authentication on the metrics endpoint. Orphan storage tenants are reported by default; `storage.orphan_policy: auto` deletes tenants that are absent from `instances`.

Set `server.public_url` to the external HTTP(S) base URL when the service runs behind a reverse proxy. Entries in `storage.download.hosts` override `max_active` and `min_interval` for a normalized upstream host.

### Instances

Every instance requires `name`, `enabled`, `mode`, and at least one HTTP(S) entry in `upstreams`. Each instance uses exactly one listener field:

- `path` mounts the instance below the main HTTP server.
- `bind` opens a dedicated listener and is required by `oci` mode.
- `display_url` sets the client-facing OCI registry address shown on the dashboard.
- `transport` customizes upstream HTTP behavior.
- `options` contains fields owned by the selected mode.

Supported `transport` fields are `proxy`, `ua`, `dial_timeout`, `header_timeout`, `idle_body_timeout`, `max_request_duration`, and `max_idle_conns`. Proxy URLs may use `http`, `https`, `socks5`, or `socks5h`.

### Mode options

| Mode | Options |
|---|---|
| `file` | `pass_headers`; `rules[]` with `match` and `class` (`metadata`, `content`, or `passthrough`) |
| `git` | `auth`, `sync_interval`, `operation_timeout` |
| `oci` | `auth` (`none`, `basic`, or `bearer`) |
| `go` | `sumdb`, `goprivate`, `disable_module_fetch_header` |
| `cargo` | `auth_required`, `allowed_crate_hosts` |
| `pypi` | `allowed_file_hosts` |

File rules are evaluated in declaration order and the last matching rule wins. Cargo and PyPI allowlists authorize external download hosts referenced by upstream metadata.

Git auth accepts `type: basic` with `username` and `password`, or `type: token` with the token in `password`. OCI auth accepts `none`, `basic`, or `bearer`; bearer credentials use `token`. Go checksum database options contain `enabled`, `name`, and `url`. Host allowlists contain host names with optional ports, without schemes or paths.

## Cache behavior

- **Metadata** is served directly for `cache.metadata_ttl`. Expired entries use an origin-bound conditional request; `304 Not Modified` refreshes the stored timestamp, and transient upstream failures can serve stale content.
- **Content** uses stable cache keys and expires according to `cache.retention`.
- **Passthrough** requests are forwarded without cache publication.
- Range requests use a valid local object when available and otherwise pass through upstream.
- A real upstream `429 Too Many Requests` creates a host cooldown using `Retry-After`.

Debian metadata is verified against the SHA-256 entries in the request's `InRelease` or `Release` file. RPM metadata is verified against the request's `repomd.xml`. OCI manifests and blobs, plus Flatpak OSTree objects, are verified before immutable cache publication.

## Client examples

For instances mounted at `/npm`, `/go`, and `/pypi`, with a dedicated OCI listener:

```bash
npm config set registry https://mirrors.example.com/npm
go env -w GOPROXY=https://mirrors.example.com/go
pip install --index-url https://mirrors.example.com/pypi/simple PACKAGE
docker pull registry.example.com:5000/IMAGE:TAG
```

Repository modes use their normal client configuration with the instance URL as the repository or mirror base URL.

## Operations

| Endpoint | Description |
|---|---|
| `/` | Instance dashboard and client setup commands |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Bounded disk-usage history |
| `/-/status/events` | Recent maintenance events; accepts `limit` |
| `/-/status/network` | Instance, upstream, and admission statistics |
| `metrics.path` | Prometheus metrics |

Object expiration, blob garbage collection, and Git synchronization run in one in-memory serial scheduler. Send `SIGINT` or `SIGTERM` for graceful shutdown.

## Development

```bash
make fmt
make tidy
make vet
make test
make test-race
CGO_ENABLED=0 go build -trimpath -o /tmp/cache-proxy-build .
```

Parser and path-classification fuzz targets can be run with bounded `go test -fuzz` commands. CI runs smoke fuzzing for every active target.

## License

This project is available under the [MIT License](LICENSE).
