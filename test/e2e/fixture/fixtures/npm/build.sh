#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  version="$major.0.0"
  package_dir="$work/$state/package"
  fixture_dir="$out/$state/npm/e2e-pkg/-"
  mkdir -p "$package_dir" "$fixture_dir"
  printf '{"name":"e2e-pkg","version":"%s","main":"index.js"}\n' "$version" >"$package_dir/package.json"
  printf 'module.exports = "cache-proxy-e2e-%s";\n' "$state" >"$package_dir/index.js"
  tar -C "$work/$state" -czf "$fixture_dir/e2e-pkg-$version.tgz" package
done
