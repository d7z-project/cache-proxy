#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  release_count=1
  [[ $state == updated ]] && release_count=2
  fixture_dir="$out/$state/go/example.com/e2e/@v"
  mkdir -p "$fixture_dir"
  : >"$fixture_dir/list"
  for release in $(seq 1 "$release_count"); do
    if [[ $release == 1 ]]; then
      version=v1.0.0
    else
      version=v1.1.0
    fi
    printf '%s\n' "$version" >>"$fixture_dir/list"
    printf 'module example.com/e2e\n\ngo 1.22\n' >"$fixture_dir/$version.mod"
    printf '{"Version":"%s","Time":"2024-01-0%sT00:00:00Z"}\n' "$version" "$release" >"$fixture_dir/$version.info"
    module_dir="$work/$state/release-$release/example.com/e2e@$version"
    mkdir -p "$module_dir"
    cp "$fixture_dir/$version.mod" "$module_dir/go.mod"
    fixture_state=initial
    [[ $release == 2 ]] && fixture_state=updated
    printf 'package e2e\n\nconst Value = "cache-proxy-e2e-%s"\n' "$fixture_state" >"$module_dir/e2e.go"
    (cd "$(dirname "$(dirname "$module_dir")")" && zip -X -q -r "$fixture_dir/$version.zip" "example.com/e2e@$version")
  done
done
