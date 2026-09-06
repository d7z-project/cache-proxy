# Operations

[Back to README](../README.md) | [Configuration reference](../config.example.yaml)

## Deployment and Security

For shared access, terminate TLS and configure client authentication or network
access controls at your ingress. `metrics.token` protects only the Prometheus
endpoint, not the dashboard or proxy routes. Upstream credentials authenticate
cache-proxy to the upstream; they do not restrict client access.

Set `server.public_url` to the public address behind a reverse proxy. OCI uses a
dedicated listener and its own `display_url`.

Use `transport.proxy` for an outbound HTTP, HTTPS, SOCKS5, or SOCKS5H proxy.
Without an explicit setting, HTTP transport follows the proxy environment
variables. Git and OCI credentials are configured in `options.auth`; credential
fields support environment variable expansion.

## Refresh and Rate Limits

Set `refresh: {interval: 30m}` on an instance to adjust its repository background
polling interval or mutable-object freshness setting. The minimum is `1s`.

| Content | Default interval |
|---|---|
| Linux repository metadata and Flatpak summaries (background polling) | 15m |
| npm, PyPI, Cargo, Go and Maven mutable metadata; Flatpak mutable refs | 1m |
| OCI tags | 2m |
| Maven SNAPSHOT files | 5m |
| Git mirror synchronization, using `options.sync_interval` | 5m |

Repository polling is staggered: a `1h` interval schedules the next check 60 to
61 minutes after the latest successful validation or publication. A busy service
may delay the check. The interval applies to each discovered repository root, not
the instance's combined event stream.

Background polling is independent of HTTP freshness. Shorter upstream freshness
does not accelerate an idle poll, but a client requiring validation, an initial
fill, or a missing object can trigger earlier work. Consequently, an instance
with a long polling interval may still contact its upstream between polls.

Other mutable objects refresh on demand. `refresh.interval` acts as a local
freshness ceiling and fallback; stricter upstream policy still applies. For
`file` rules using `http_cache`, omitting the interval requires explicit upstream
freshness for caching. Refresh settings do not extend protocol expiry, signed URL
validity, or immutable-object retention.

Use `storage.download` for global concurrency, per-host concurrency, and request
pacing. Its `hosts` mapping overrides limits for a hostname with an optional
port. Limits apply across instances, including redirect and authentication
requests. Upstream `429` responses establish cooldowns honoring `Retry-After`.
Tune both refresh intervals and download limits to upstream quotas.

## Freshness and Offline Use

Cached content can remain available during transient failures when request,
upstream, and protocol policies allow reuse. Offline access is limited to content
already cached and eligible for reuse; cache-proxy is not a complete upstream
mirror. Responses marked `no-store` are not retained.

Explicit `no-cache` or `max-age=0` requests require validation even when content
is cached. APT normally requests index validation; offline use requires client
settings that permit cache reuse. During retry backoff, requests reuse eligible
cached metadata or immediately receive the known refresh failure. Client
cancellation stops waiting, not background publication.

Repository metadata is verified before an updated snapshot replaces the cached
one. A failed refresh leaves the committed cache intact, subject to expiry and
validation requirements. Debian `Valid-Until` remains effective. Unversioned
repository URLs can still change between separate client requests.

Debian repositories advertising `Acquire-By-Hash: yes` download and verify indexes
on demand. Other Debian repositories cache complete metadata snapshots. This
selection is automatic for standard, nested, and flat layouts.

## Storage and Upgrades

Use a dedicated persistent backend volume with one writer process. Send `SIGINT`
or `SIGTERM` for graceful shutdown.

Backend formats are internal and have no compatibility guarantee across builds.
Plan for cache repopulation when upgrading.

`storage.gc.blob` controls unreferenced blob retention. `storage.cleanup` controls
cleanup batching and dry runs, and `storage.orphan_policy` selects reporting or
automatic cleanup of inactive instance data. Object size and temporary download
limits are not a total backend disk quota; monitor available storage.

## Monitoring

| Endpoint | Purpose |
|---|---|
| `/` | Dashboard, instance status, and client configuration |
| `/-/status/summary` | Service and storage summary |
| `/-/status/disk` | Disk usage history |
| `/-/status/events` | Maintenance results and errors; accepts `limit` |
| `/-/status/network` | Instance and upstream request statistics |
| `/metrics` | Prometheus metrics; configurable through `metrics.path` |

Metadata events identify the repository root, trigger, phase, and outcome.
Queue and execution time are separate. Validation details show client or upstream
revalidation requirements, freshness or protocol expiry, cache age, and the last
validation time. Multiple events for an instance can concern different roots;
inspect the target before comparing refresh times.

## Troubleshooting

Start with the dashboard's maintenance errors and service logs:

```bash
docker logs --tail 100 -f cache-proxy
```

| Symptom | What to check |
|---|---|
| Repeated `429` responses | Upstream quotas, per-host request pacing and concurrency, and refresh intervals |
| Frequent metadata requests | Event target and validation cause; client validation can run between background polls |
| `504` during metadata validation | Upstream reachability, response latency, and the `X-Cache-Error` header below |
| Offline request fails despite cached content | Client validation requirements, upstream policy, protocol expiry, and whether the requested object was cached |
| Cache write or cleanup errors | Backend permissions, free space, and download limits |

Existing repository metadata requests needing validation wait up to 30 seconds,
or the client's shorter deadline. `transport.header_timeout` (default `30s`)
independently limits the upstream response header wait. Body reads and total
requests have separate limits documented in the configuration reference.

Failed metadata validation responses can include `X-Cache-Error`:

| Value | Meaning |
|---|---|
| `validation_wait` | The client's validation wait timed out or was canceled |
| `upstream_timeout` | The refresh encountered a timeout |
