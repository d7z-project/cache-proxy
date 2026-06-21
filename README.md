# cache-proxy

`cache-proxy` is a Go caching reverse proxy with static YAML configuration and BlobFS-backed cache storage.

## Features

- File, OCI registry, npm registry, Go module, Maven, Cargo, PyPI, APK, DEB, RPM, and Pacman proxy modes.
- One YAML config file defines listeners, storage, metrics, cleanup, and proxy instances.
- Path-mounted instances and dedicated bind listeners.
- Prometheus metrics for proxy traffic and BlobFS state.
- Background BlobFS GC and cache cleanup.

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

## Example Config

```yaml
server:
  bind: 127.0.0.1:18080
  backend: /tmp/cache-proxy

metrics:
  path: /metrics
  token: ""

storage:
  gc:
    blob: 24h
  cleanup:
    enabled: false
    interval: 6h
    dry_run: false
    batch_size: 500
    workers: 0

instances:
  - name: files
    enabled: true
    file:
      expire_after: 720h
      route:
        path: /files
      upstreams:
        - https://example.com
      default_policy: revalidate
      busy_policy: stale
      rules: []

  - name: registry
    enabled: true
    oci:
      bind: 127.0.0.1:5000
      upstream: https://registry-1.docker.io
      default_policy: bypass
      busy_policy: stale
      rules: []
```

## Development

```bash
make fmt
make test
```
