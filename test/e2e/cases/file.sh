#!/usr/bin/env bash

e2e_prepare_file() {
  :
}

e2e_run_file() {
  printf '\n[file] cold, warm, update, Range/HEAD and persisted offline reuse\n'
  e2e_reset_fixture
  e2e_client file cold "$E2E_TOOLS_IMAGE" '
    test "$(curl -fsS "$1/file/payload.txt")" = cache-proxy-e2e-initial
    test "$(curl -fsS -H "Range: bytes=0-4" "$1/file/payload.txt")" = cache
    curl -fsSI "$1/file/payload.txt" >/dev/null
  ' "$E2E_PROXY_URL"
  local before
  before=$(e2e_fixture_count GET /file/payload.txt)
  ((before >= 1)) || e2e_fail 'file cold request did not reach the fixture'
  e2e_client file warm "$E2E_TOOLS_IMAGE" \
    'test "$(curl -fsS "$1/file/payload.txt")" = cache-proxy-e2e-initial' "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET /file/payload.txt "$before" 'file payload was fetched during warm use'
  e2e_set_fixture_state updated
  e2e_wait_body "$E2E_PROXY_URL/file/payload.txt" cache-proxy-e2e-updated
  e2e_offline_restart
  e2e_client file offline "$E2E_TOOLS_IMAGE" \
    'test "$(curl -fsS "$1/file/payload.txt")" = cache-proxy-e2e-updated' "$E2E_PROXY_URL"
  e2e_restore_online
}
