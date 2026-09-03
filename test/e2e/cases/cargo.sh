#!/usr/bin/env bash

e2e_prepare_cargo() {
  e2e_pull_image "$E2E_RUST_IMAGE"
}

e2e_run_cargo() {
	printf '\n[cargo] sparse registry fetch, warm crate, index update and persisted offline reuse\n'
	e2e_reset_fixture
	e2e_assert_transparent_paths cargo /cargo /cargo bypass
  local script='
    mkdir -p /tmp/project/.cargo /tmp/project/src && cd /tmp/project
    cat >Cargo.toml <<EOF
[package]
name = "consumer"
version = "1.0.0"
edition = "2021"
[dependencies]
e2e-crate = "=$2"
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
  e2e_client cargo cold "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL" 1.0.0
  local before
  before=$(e2e_fixture_count GET /cargo/api/v1/crates/e2e-crate/1.0.0/download)
  ((before >= 1)) || e2e_fail 'Cargo crate did not reach the fixture'
  e2e_client cargo warm "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL" 1.0.0
  e2e_assert_count_unchanged GET /cargo/api/v1/crates/e2e-crate/1.0.0/download "$before" 'Cargo crate was fetched during warm resolution'
  e2e_set_fixture_state updated
  e2e_wait_contains "$E2E_PROXY_URL/cargo/e2/e-/e2e-crate" '"vers":"2.0.0"'
  e2e_client cargo update "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL" 2.0.0
  e2e_offline_restart
  e2e_client cargo offline "$E2E_RUST_IMAGE" "$script" "$E2E_PROXY_URL" 2.0.0
  e2e_restore_online
}
