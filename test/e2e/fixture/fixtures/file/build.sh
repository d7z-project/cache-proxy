#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
for state in initial updated; do
  mkdir -p "$out/$state/file"
  printf 'cache-proxy-e2e-%s\n' "$state" >"$out/$state/file/payload.txt"
done
