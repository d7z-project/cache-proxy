#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  release_count=1
  [[ $state == updated ]] && release_count=2
  for release in $(seq 1 "$release_count"); do
    version="$release.0.0"
    crate_dir="$work/$state/e2e-crate-$version"
    fixture_dir="$out/$state/cargo/api/v1/crates/e2e-crate/$version"
    mkdir -p "$crate_dir/src" "$fixture_dir"
    cat >"$crate_dir/Cargo.toml" <<EOF
[package]
name = "e2e-crate"
version = "$version"
edition = "2021"
license = "MIT"
EOF
    fixture_state=initial
    [[ $release == 2 ]] && fixture_state=updated
    printf 'pub const VALUE: &str = "cache-proxy-e2e-%s";\n' "$fixture_state" >"$crate_dir/src/lib.rs"
    tar -C "$(dirname "$crate_dir")" -czf "$fixture_dir/download" "e2e-crate-$version"
  done
done
