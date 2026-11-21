#!/usr/bin/env bash

e2e_prepare_openvsx() {
  E2E_OPENVSX_SELECTED=1
  export E2E_OPENVSX_SELECTED
  e2e_build_image "$E2E_OPENVSX_CLIENT_IMAGE" \
    "$E2E_ROOT/test/e2e/clients/openvsx/Containerfile" "$E2E_ROOT/test/e2e/clients/openvsx"
}

e2e_run_openvsx() {
  printf '\n[openvsx] native download, gallery, item UI, warm object and update\n'
  e2e_reset_fixture

  e2e_client openvsx metadata "$E2E_OPENVSX_CLIENT_IMAGE" '
    ovsx get e2e.extension --metadata --registryUrl "$1" > /tmp/metadata.json
    grep -Fq "$1/api/e2e/extension/1.0.0/file/download" /tmp/metadata.json
    ! grep -Fq "$2" /tmp/metadata.json
  ' "$E2E_OPENVSX_URL" "$E2E_FIXTURE_URL"

  e2e_client openvsx gallery "$E2E_TOOLS_IMAGE" '
    response=$(curl --fail --silent --show-error -H "Content-Type: application/json" \
      --data "{\"filters\":[]}" "$1/vscode/gallery/extensionquery")
    printf "%s" "$response" | grep -Fq "$1/api/e2e/extension/1.0.0/file/download"
    ! printf "%s" "$response" | grep -Fq "$2"
    location=$(curl --silent --show-error --output /dev/null --write-out "%{redirect_url}" \
      "$1/vscode/item?itemName=e2e.extension")
    test "$location" = "$1/extension/e2e/extension"
    page=$(curl --fail --silent --show-error "$1/extension/e2e/extension")
    printf "%s" "$page" | grep -Fq "$1/bundle-e2e.js"
    ! printf "%s" "$page" | grep -Fq tracker.invalid
    curl --fail --silent --show-error "$1/bundle-e2e.js" | grep -Fq "$1/api"
  ' "$E2E_OPENVSX_URL" "$E2E_FIXTURE_URL"

  e2e_client openvsx cold "$E2E_OPENVSX_CLIENT_IMAGE" '
    ovsx get e2e.extension --registryUrl "$1" --output /tmp/e2e.vsix
    grep -Fq e2e-openvsx-v1.0.0 /tmp/e2e.vsix
  ' "$E2E_OPENVSX_URL"
  local before
  before=$(e2e_fixture_count GET /openvsx-cdn/e2e-extension-1.0.0.vsix)
  ((before == 1)) || e2e_fail 'Open VSX cold VSIX download did not reach the CDN fixture exactly once'

  e2e_client openvsx warm "$E2E_OPENVSX_CLIENT_IMAGE" '
    ovsx get e2e.extension --registryUrl "$1" --output /tmp/e2e.vsix
    grep -Fq e2e-openvsx-v1.0.0 /tmp/e2e.vsix
  ' "$E2E_OPENVSX_URL"
  if [[ ${E2E_CACHE:-true} == true ]]; then
    e2e_assert_count_unchanged GET /openvsx-cdn/e2e-extension-1.0.0.vsix "$before" 'Open VSX warm VSIX was fetched again'
  fi

  e2e_client openvsx target-cold "$E2E_OPENVSX_CLIENT_IMAGE" '
    ovsx get e2e.extension --target alpine-arm64 --registryUrl "$1" --output /tmp/e2e-target.vsix
    grep -Fq e2e-openvsx-alpine-arm64-v1.0.0 /tmp/e2e-target.vsix
  ' "$E2E_OPENVSX_URL"
  local target_before
  target_before=$(e2e_fixture_count GET '/openvsx-cdn/e2e-extension-1.0.0@alpine-arm64.vsix')
  ((target_before == 1)) || e2e_fail 'Open VSX target-platform cold download did not reach the CDN fixture exactly once'

  e2e_client openvsx target-warm "$E2E_OPENVSX_CLIENT_IMAGE" '
    ovsx get e2e.extension --target alpine-arm64 --registryUrl "$1" --output /tmp/e2e-target.vsix
    grep -Fq e2e-openvsx-alpine-arm64-v1.0.0 /tmp/e2e-target.vsix
  ' "$E2E_OPENVSX_URL"
  if [[ ${E2E_CACHE:-true} == true ]]; then
    e2e_assert_count_unchanged GET '/openvsx-cdn/e2e-extension-1.0.0@alpine-arm64.vsix' "$target_before" 'Open VSX warm target-platform VSIX was fetched again'
  fi

  e2e_client openvsx unpkg "$E2E_TOOLS_IMAGE" '
    directory=$(curl --fail --silent --show-error "$1/vscode/unpkg/e2e/extension/1.0.0/extension")
    printf "%s" "$directory" | grep -Fq "$1/vscode/unpkg/e2e/extension/1.0.0/extension/module.wasm"
    ! printf "%s" "$directory" | grep -Fq "$2"
    curl --fail --silent --show-error "$1/vscode/unpkg/e2e/extension/1.0.0/extension/module.wasm" |
      grep -Fq e2e-openvsx-unpkg
  ' "$E2E_OPENVSX_URL" "$E2E_FIXTURE_URL"
  local unpkg_before
  unpkg_before=$(e2e_fixture_count GET /openvsx-cdn/unpkg-module.wasm)
  ((unpkg_before == 1)) || e2e_fail 'Open VSX unpkg redirect did not reach the CDN fixture exactly once'
  e2e_client openvsx unpkg-warm "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error "$1/vscode/unpkg/e2e/extension/1.0.0/extension/module.wasm" |
      grep -Fq e2e-openvsx-unpkg
  ' "$E2E_OPENVSX_URL"
  if [[ ${E2E_CACHE:-true} == true ]]; then
    e2e_assert_count_unchanged GET /openvsx-cdn/unpkg-module.wasm "$unpkg_before" 'Open VSX warm unpkg file was fetched again'
  fi

  local upstream_ua
  upstream_ua=$(e2e_fixture_header GET /api/e2e/extension User-Agent)
  [[ $upstream_ua == cache-proxy* ]] || e2e_fail "Open VSX client bypassed the proxy: upstream UA was $upstream_ua"

  e2e_set_fixture_state updated
  e2e_client openvsx refresh "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error -H "Cache-Control: no-cache" "$1/api/e2e/extension" |
      grep -Fq '"'"'"version"'"'"':"'"'"2.0.0"'"'"''
  ' "$E2E_OPENVSX_URL"
  e2e_client openvsx update "$E2E_OPENVSX_CLIENT_IMAGE" '
    ovsx get e2e.extension --registryUrl "$1" --output /tmp/e2e.vsix
    grep -Fq e2e-openvsx-v2.0.0 /tmp/e2e.vsix
  ' "$E2E_OPENVSX_URL"

  if [[ ${E2E_CACHE:-true} == true ]]; then
    e2e_wait_cache_hit "$E2E_OPENVSX_URL/api/e2e/extension"
    e2e_offline_restart
    e2e_client openvsx offline "$E2E_OPENVSX_CLIENT_IMAGE" '
      ovsx get e2e.extension --registryUrl "$1" --output /tmp/e2e.vsix
      grep -Fq e2e-openvsx-v2.0.0 /tmp/e2e.vsix
    ' "$E2E_OPENVSX_URL"
    e2e_restore_online
  fi
}
