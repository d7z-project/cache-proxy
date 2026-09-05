#!/usr/bin/env bash

e2e_prepare_apk() {
  e2e_pull_image "$E2E_ALPINE_IMAGE"
}

e2e_run_apk() {
	printf '\n[apk] native index/install, generation update, warm package and offline restart\n'
	e2e_reset_fixture
	e2e_assert_transparent_paths apk /apk /apk bypass
  local script='
    printf "%s/apk/v3.20/main\n" "$1" >/tmp/repositories
    apk add --no-cache --allow-untrusted --repositories-file /tmp/repositories e2e-apk >/dev/null
    grep -qx "$2" /usr/share/e2e-apk/payload.txt
  '
  e2e_client apk cold "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-initial
  e2e_wait_cache_hit "$E2E_PROXY_URL/apk/v3.20/main/x86_64/APKINDEX.tar.gz"
  local package_path=/apk/v3.20/main/x86_64/e2e-apk-1.0.0-r0.apk before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'APK package did not reach the fixture'
  e2e_client apk warm "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-initial
  e2e_assert_count_unchanged GET "$package_path" "$before" 'APK package was fetched during warm install'
  local previous_etag
  previous_etag=$(e2e_response_header "$E2E_PROXY_URL/apk/v3.20/main/x86_64/APKINDEX.tar.gz" ETag)
  e2e_set_fixture_state updated
  e2e_wait_header_changed "$E2E_PROXY_URL/apk/v3.20/main/x86_64/APKINDEX.tar.gz" ETag "$previous_etag"
  e2e_client apk update "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-updated
  e2e_wait_cache_hit "$E2E_PROXY_URL/apk/v3.20/main/x86_64/APKINDEX.tar.gz"
  e2e_offline_restart
  e2e_client apk offline "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-updated
  e2e_assert_offline_validation apk "$E2E_PROXY_URL/apk/v3.20/main/x86_64/APKINDEX.tar.gz"
  e2e_restore_online
}
