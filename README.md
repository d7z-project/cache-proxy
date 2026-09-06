# cache-proxy

[![CI](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/d7z-project/cache-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/d7z-project/cache-proxy/releases)
[![License](https://img.shields.io/github/license/d7z-project/cache-proxy)](LICENSE)

A self-hosted, read-only caching proxy for package registries, Linux repositories,
container images, Git repositories, and HTTP files.

Reduce repeated downloads across machines and CI jobs while continuing to use
native package clients. Run multiple instances in one process, each with its own
upstream, persistent cache, and refresh settings.

[Quick Start](#quick-start) | [Installation](#installation) |
[Configuration](#configuration) | [Client Setup](#client-setup) |
[Operations](docs/operations.md)

## Features

- Protocol-aware caching and content verification.
- Persistent downloads shared across clients and service restarts.
- Shared upstream transfers for concurrent requests to the same object.
- Configurable refresh intervals, download concurrency, and request pacing.
- Built-in dashboard with client configuration, cache usage, and maintenance status.
- Prometheus metrics for monitoring.

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

Each instance uses one HTTP(S) upstream. Repository layouts are detected
automatically; each `git` instance mirrors one repository. All upstream access
is read-only. Offline availability depends on which content is cached and whether
its freshness and protocol policies permit reuse.

## Quick Start

The following example runs an npm cache on the local machine using Docker.
Podman can be used in place of Docker.

1. Create `cache-proxy.yaml`:

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

2. Start the service with persistent storage:

```bash
docker run -d --name cache-proxy \
  -p 127.0.0.1:18080:18080 \
  -v "$PWD/cache-proxy.yaml:/etc/cache-proxy.yaml:ro" \
  -v cache-proxy-data:/data \
  ghcr.io/d7z-project/cache-proxy:main \
  -config /etc/cache-proxy.yaml
```

3. Open the dashboard at `http://127.0.0.1:18080/` and configure npm:

```bash
npm config set registry http://127.0.0.1:18080/npm
```

The service is accessible only from the host. For shared access, adjust the
published address and `server.public_url`, and configure TLS and access controls
at your ingress. The dashboard and proxy routes do not provide client
authentication.

## Installation

### Container Images

Images are published to `ghcr.io/d7z-project/cache-proxy`. The `main` tag follows
the default branch; use a specific release image tag for repeatable deployments.
See [GitHub Releases](https://github.com/d7z-project/cache-proxy/releases) for
available releases and [Quick Start](#quick-start) for container configuration.

### Linux Binaries

Download the binary for `amd64`, `arm64`, or `loong64` and `SHA256SUMS` from
[GitHub Releases](https://github.com/d7z-project/cache-proxy/releases).
Verify its checksum, then run it with a configuration file:

```bash
chmod +x cache-proxy-amd64
./cache-proxy-amd64 -config config.example.yaml -validate
./cache-proxy-amd64 -config config.example.yaml
```

Replace `amd64` with your architecture. The
[example configuration](config.example.yaml) enables Debian and npm on
`127.0.0.1:18080` and stores data in `./data`.

### From Source

Requires Go 1.26 or newer and Make:

```bash
git clone https://github.com/d7z-project/cache-proxy.git
cd cache-proxy
make cache-proxy
./cache-proxy -config config.example.yaml
```

## Configuration

[config.example.yaml](config.example.yaml) documents settings and defaults, with
an instance example for every mode. Enable and customize only the instances you
need. Validate your configuration before starting the service:

```bash
./cache-proxy -config cache-proxy.yaml -validate
```

- `server.bind` sets the main listener; `server.public_url` sets its public address
  for dashboard client instructions.
- `server.backend` sets the persistent storage directory. Use a dedicated volume
  in containers and allow only one writer process per backend.
- Each instance requires a unique `name`, a `mode`, an `upstream`, and exactly one
  of `path` or `bind`. Set `enabled: true` to start it.
- OCI requires a dedicated `bind`; set `display_url` to its public client address.
- `refresh.interval` controls repository background polling or mutable-object
  freshness. Git uses `options.sync_interval`.
- `transport` configures outbound connections, proxies, and timeouts;
  `storage.download` controls shared concurrency and request pacing.

Unknown YAML fields and invalid settings are rejected. Durations use Go syntax
such as `500ms`, `15m`, and `24h`; sizes accept binary units such as `MiB` and `GiB`.

For refresh behavior, upstream credentials, rate limits, storage, and deployment
security, see the [operations guide](docs/operations.md).

## Client Setup

The dashboard provides configuration for every enabled instance, including Maven,
Alpine, RPM, Pacman, and Flatpak. Use the instance URL in place of the upstream
address. The examples below assume matching instances are enabled and exposed
through TLS at your public hostname.

```bash
npm config set registry https://mirrors.example.com/npm
go env -w GOPROXY=https://mirrors.example.com/go
pip install --index-url https://mirrors.example.com/pypi/simple PACKAGE
git clone https://mirrors.example.com/git/cache-proxy.git
```

For Cargo, add this to `.cargo/config.toml`. The sparse registry URL must end in
a slash:

```toml
[source.cache-proxy]
registry = "sparse+https://mirrors.example.com/cargo/"

[source.crates-io]
replace-with = "cache-proxy"
```

Linux clients retain their upstream distribution, components, and signing keys.
For example, standard and flat Debian sources in
`/etc/apt/sources.list.d/cache-proxy.list`:

```text
deb https://mirrors.example.com/debian bookworm main
deb [signed-by=/usr/share/keyrings/vendor-archive-keyring.gpg] https://mirrors.example.com/debian-flat ./
```

These require instances mounted at `/debian` and `/debian-flat`. The flat example
assumes the vendor's key is installed at the specified path.

For OCI, expose its dedicated listener through a TLS endpoint, for example
`https://registry.example.com`, and set `display_url` accordingly. With the
example configuration's Docker Hub upstream:

```bash
docker pull registry.example.com/library/alpine:3.20
```

## Monitoring and Support

Use the dashboard at `/` to inspect instances, storage usage, network activity,
and maintenance errors. Prometheus metrics are served at `/metrics` by default.
Container logs are available with:

```bash
docker logs --tail 100 -f cache-proxy
```

See the [operations guide](docs/operations.md) for offline behavior, upgrades,
status endpoints, and troubleshooting. Report bugs through
[GitHub Issues](https://github.com/d7z-project/cache-proxy/issues), including the
build or image tag, mode, configuration without credentials, and relevant logs.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build and test requirements.
Pull requests should include tests for the affected behavior.

## License

[MIT](LICENSE)
