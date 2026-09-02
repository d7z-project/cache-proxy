#!/usr/bin/env bash

registry_client() {
  local case_name=$1 phase=$2 image=$3 script=$4
  shift 4
  e2e_run_client_shell "${E2E_RUN_ID}-${case_name}-${phase}" "$image" "$script" "$@"
}

run_file_case() {
  printf '\n[file] cold, warm, Range/HEAD and persisted offline reuse\n'
  e2e_reset_fixture_counts
  registry_client file cold "$E2E_TOOLS_IMAGE" '
    test "$(curl -fsS "$1/file/payload.txt")" = cache-proxy-e2e-payload
    test "$(curl -fsS -H "Range: bytes=0-4" "$1/file/payload.txt")" = cache
    curl -fsSI "$1/file/payload.txt" >/dev/null
  ' "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count GET /file/payload.txt)
  ((before >= 1)) || e2e_fail 'file cold request did not reach the fixture'
  registry_client file warm "$E2E_TOOLS_IMAGE" \
    'test "$(curl -fsS "$1/file/payload.txt")" = cache-proxy-e2e-payload' "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET /file/payload.txt "$before" 'file immutable payload was fetched during warm use'
  e2e_offline_restart
  registry_client file offline "$E2E_TOOLS_IMAGE" \
    'test "$(curl -fsS "$1/file/payload.txt")" = cache-proxy-e2e-payload' "$E2E_PROXY_URL"
  e2e_restore_online
}

run_npm_case() {
  printf '\n[npm] install, audit, warm tarball, publish rejection and offline reuse\n'
  e2e_reset_fixture_counts
  local install_script='
    mkdir /tmp/project && cd /tmp/project
    printf "{\"name\":\"consumer\",\"version\":\"1.0.0\",\"dependencies\":{\"e2e-pkg\":\"1.0.0\"}}\n" >package.json
    npm install --registry="$1/npm/" --ignore-scripts --no-fund --no-audit
    test "$(node -e "process.stdout.write(require(\"e2e-pkg\"))")" = cache-proxy-e2e
  '
  registry_client npm cold "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL"
  registry_client npm audit "$E2E_NODE_IMAGE" '
    mkdir /tmp/project && cd /tmp/project
    printf "{\"name\":\"consumer\",\"version\":\"1.0.0\",\"dependencies\":{\"e2e-pkg\":\"1.0.0\"}}\n" >package.json
    npm install --registry="$1/npm/" --ignore-scripts --no-fund --no-audit >/dev/null
    npm audit --registry="$1/npm/" --audit-level=critical >/dev/null
  ' "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count GET /npm/e2e-pkg/-/e2e-pkg-1.0.0.tgz)
  ((before >= 1)) || e2e_fail 'npm cold tarball request did not reach the fixture'
  registry_client npm warm "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET /npm/e2e-pkg/-/e2e-pkg-1.0.0.tgz "$before" 'npm tarball was fetched during warm install'
  local writes
  writes=$(e2e_fixture_count PUT /npm/e2e-publish)
  registry_client npm publish "$E2E_NODE_IMAGE" '
    mkdir /tmp/publish && cd /tmp/publish
    printf "{\"name\":\"e2e-publish\",\"version\":\"1.0.0\"}\n" >package.json
    printf "module.exports = true;\n" >index.js
    if npm publish --registry="$1/npm/" --ignore-scripts >/tmp/publish.log 2>&1; then
      cat /tmp/publish.log >&2
      exit 1
    fi
  ' "$E2E_PROXY_URL"
  e2e_assert_count_unchanged PUT /npm/e2e-publish "$writes" 'npm publish reached the upstream fixture'
  e2e_offline_restart
  registry_client npm offline "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_go_case() {
  printf '\n[go] module download, warm zip and persisted offline reuse\n'
  e2e_reset_fixture_counts
  local script='
    mkdir /tmp/module && cd /tmp/module
    go mod init client.example/e2e >/dev/null
    GOPROXY="$1/go" GOSUMDB=off GOPRIVATE= go mod download example.com/e2e@v1.0.0
  '
  registry_client go cold "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count GET /go/example.com/e2e/@v/v1.0.0.zip)
  ((before >= 1)) || e2e_fail 'Go module zip did not reach the fixture'
  registry_client go warm "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET /go/example.com/e2e/@v/v1.0.0.zip "$before" 'Go module zip was fetched during warm download'
  e2e_offline_restart
  registry_client go offline "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_maven_case() {
  printf '\n[maven] dependency resolution, warm artifact and persisted offline reuse\n'
  e2e_reset_fixture_counts
  local script='
    mvn -B -ntp \
      -DremoteRepositories=e2e::default::"$1/maven" \
      org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get \
      -Dartifact=com.example:e2e-maven:1.0.0 -Dtransitive=false >/dev/null
    test -f /root/.m2/repository/com/example/e2e-maven/1.0.0/e2e-maven-1.0.0.jar
  '
  registry_client maven cold "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count TRANSFER /maven/com/example/e2e-maven/1.0.0/e2e-maven-1.0.0.jar)
  ((before >= 1)) || e2e_fail 'Maven artifact did not reach the fixture'
  registry_client maven warm "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged TRANSFER /maven/com/example/e2e-maven/1.0.0/e2e-maven-1.0.0.jar "$before" 'Maven artifact body was transferred during warm resolution'
  e2e_offline_restart
  registry_client maven offline "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_cargo_case() {
  printf '\n[cargo] sparse registry fetch, warm crate and persisted offline reuse\n'
  e2e_reset_fixture_counts
  local script='
    mkdir -p /tmp/project/.cargo /tmp/project/src && cd /tmp/project
    cat >Cargo.toml <<EOF
[package]
name = "consumer"
version = "1.0.0"
edition = "2021"
[dependencies]
e2e-crate = "=1.0.0"
EOF
    printf "fn main() {}\n" >src/main.rs
    cat >.cargo/config.toml <<EOF
[source.crates-io]
replace-with = "e2e"
[source.e2e]
registry = "sparse+$1/cargo/"
EOF
    cargo fetch --locked 2>/dev/null || cargo fetch
  '
  registry_client cargo cold "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count GET /cargo/api/v1/crates/e2e-crate/1.0.0/download)
  ((before >= 1)) || e2e_fail 'Cargo crate did not reach the fixture'
  registry_client cargo warm "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET /cargo/api/v1/crates/e2e-crate/1.0.0/download "$before" 'Cargo crate was fetched during warm resolution'
  e2e_offline_restart
  registry_client cargo offline "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_pypi_case() {
  printf '\n[pypi] pip download, warm wheel and persisted offline reuse\n'
  e2e_reset_fixture_counts
  local script='
    mkdir /tmp/download
    pip download --disable-pip-version-check --no-cache-dir --no-deps \
      --trusted-host 127.0.0.1 --index-url "$1/pypi/simple/" \
      --dest /tmp/download e2e-pkg==1.0.0 >/dev/null
    test -f /tmp/download/e2e_pkg-1.0.0-py3-none-any.whl
  '
  registry_client pypi cold "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count GET /pypi/files/e2e_pkg-1.0.0-py3-none-any.whl)
  ((before >= 1)) || e2e_fail 'PyPI wheel did not reach the fixture'
  registry_client pypi warm "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET /pypi/files/e2e_pkg-1.0.0-py3-none-any.whl "$before" 'PyPI wheel was fetched during warm download'
  e2e_offline_restart
  registry_client pypi offline "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_registries_suite() {
  run_file_case
  run_npm_case
  run_go_case
  run_maven_case
  run_cargo_case
  run_pypi_case
}
