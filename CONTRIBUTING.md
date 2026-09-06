# Contributing

Report bugs and discuss changes through
[GitHub Issues](https://github.com/d7z-project/cache-proxy/issues). Include the
build or image tag, affected mode, relevant configuration without credentials,
and logs needed to reproduce the issue.

## Local Checks

Requires Go 1.26 or newer and Make. Race tests also require a working C compiler.
Run from the repository root:

```bash
make fmt
git diff --check
make test
make test-race
make test-fuzz
make vet
make cache-proxy
./cache-proxy -config config.example.yaml -validate
```

`make test-fuzz` runs a bounded smoke test for every fuzz target.

## End-to-End Tests

E2E tests require Docker or Podman on native Linux. The service, fixture, probes,
and native package clients run in host-network containers:

```bash
make test-e2e
E2E_RUNTIME=podman E2E_SUITE=deb make test-e2e
```

`E2E_SUITE` accepts a mode from the README's
[supported modes](README.md#supported-modes); omitting it runs every mode.
Each run cleans up its own labeled resources.

Keep changes focused, include tests for affected behavior, and update user
documentation when public behavior or configuration changes. Run the related
E2E suite for protocol changes and the full suite for shared protocol changes.
Include verification results and any checks that could not run in your pull
request.
