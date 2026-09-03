#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  version="$major.0.0"
  wheel_dir="$work/$state"
  fixture_dir="$out/$state/pypi/files"
  mkdir -p "$wheel_dir/e2e_pkg" "$wheel_dir/e2e_pkg-$version.dist-info" "$fixture_dir"
  printf '__version__ = "%s"\nVALUE = "cache-proxy-e2e-%s"\n' "$version" "$state" >"$wheel_dir/e2e_pkg/__init__.py"
  cat >"$wheel_dir/e2e_pkg-$version.dist-info/METADATA" <<EOF
Metadata-Version: 2.1
Name: e2e-pkg
Version: $version
Summary: cache-proxy E2E fixture
EOF
  cat >"$wheel_dir/e2e_pkg-$version.dist-info/WHEEL" <<'EOF'
Wheel-Version: 1.0
Generator: cache-proxy-e2e
Root-Is-Purelib: true
Tag: py3-none-any
EOF
  cat >"$wheel_dir/e2e_pkg-$version.dist-info/RECORD" <<EOF
e2e_pkg/__init__.py,,
e2e_pkg-$version.dist-info/METADATA,,
e2e_pkg-$version.dist-info/WHEEL,,
e2e_pkg-$version.dist-info/RECORD,,
EOF
  (cd "$wheel_dir" && zip -X -q -r "$fixture_dir/e2e_pkg-$version-py3-none-any.whl" .)
done
