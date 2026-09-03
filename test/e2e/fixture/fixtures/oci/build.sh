#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  build_dir="$work/$state"
  fixture_dir="$out/$state/oci/v2/e2e/image"
  mkdir -p "$build_dir/layer" "$fixture_dir/manifests" "$fixture_dir/blobs"
  printf 'cache-proxy-e2e-%s\n' "$state" >"$build_dir/layer/payload.txt"
  tar -C "$build_dir/layer" -cf "$build_dir/layer.tar" .
  gzip -n -c "$build_dir/layer.tar" >"$build_dir/layer.tar.gz"
  layer_digest=$(sha256sum "$build_dir/layer.tar.gz" | awk '{print $1}')
  layer_size=$(wc -c <"$build_dir/layer.tar.gz" | tr -d ' ')
  diff_id=$(sha256sum "$build_dir/layer.tar" | awk '{print $1}')
  cat >"$build_dir/config.json" <<EOF
{"architecture":"amd64","os":"linux","created":"2024-01-0${major}T00:00:00Z","config":{"Labels":{"fixture.state":"$state"}},"rootfs":{"type":"layers","diff_ids":["sha256:$diff_id"]},"history":[{"created":"2024-01-0${major}T00:00:00Z","created_by":"cache-proxy e2e $state"}]}
EOF
  config_digest=$(sha256sum "$build_dir/config.json" | awk '{print $1}')
  config_size=$(wc -c <"$build_dir/config.json" | tr -d ' ')
  cat >"$fixture_dir/manifests/latest" <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:$config_digest","size":$config_size},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"sha256:$layer_digest","size":$layer_size}],"annotations":{"fixture.state":"$state"}}
EOF
  cp "$build_dir/config.json" "$fixture_dir/blobs/sha256:$config_digest"
  cp "$build_dir/layer.tar.gz" "$fixture_dir/blobs/sha256:$layer_digest"
done
