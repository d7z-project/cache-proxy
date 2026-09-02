#!/usr/bin/env bash

linux_client() {
  local case_name=$1 phase=$2 image=$3 script=$4
  shift 4
  e2e_run_client_shell "${E2E_RUN_ID}-${case_name}-${phase}" "$image" "$script" "$@"
}

run_deb_standard_case() {
  printf '\n[deb-standard] apt update/install, published generation, warm package and offline restart\n'
  e2e_reset_fixture_counts
  local script='
    rm -f /etc/apt/sources.list.d/*
    printf "deb [trusted=yes] %s/deb stable main\n" "$1" >/etc/apt/sources.list
    apt-get -o Acquire::Check-Date=false -o Acquire::Languages=none update >/dev/null
    apt-get -o Acquire::Check-Date=false install -y --no-install-recommends e2e-deb >/dev/null
    grep -q cache-proxy-e2e /usr/share/e2e-deb/payload.txt
  '
  linux_client deb-standard cold "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_wait_cache_hit "$E2E_PROXY_URL/deb/dists/stable/Release"
  local package_path=/deb/pool/main/e/e2e-deb/e2e-deb_1.0.0_all.deb before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'Debian standard package did not reach the fixture'
  linux_client deb-standard warm "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET "$package_path" "$before" 'Debian standard package was fetched during warm install'
  e2e_offline_restart
  linux_client deb-standard offline "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_deb_flat_case() {
  printf '\n[deb-flat] apt flat repository update/install, warm package and offline restart\n'
  e2e_reset_fixture_counts
  local script='
    rm -f /etc/apt/sources.list.d/*
    printf "deb [trusted=yes] %s/deb/flat ./\n" "$1" >/etc/apt/sources.list
    apt-get -o Acquire::Check-Date=false -o Acquire::Languages=none update >/dev/null
    apt-get -o Acquire::Check-Date=false install -y --no-install-recommends e2e-deb >/dev/null
    grep -q cache-proxy-e2e /usr/share/e2e-deb/payload.txt
  '
  linux_client deb-flat cold "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_wait_cache_hit "$E2E_PROXY_URL/deb/flat/Release"
  local package_path=/deb/flat/e2e-deb_1.0.0_all.deb before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'Debian flat package did not reach the fixture'
  linux_client deb-flat warm "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET "$package_path" "$before" 'Debian flat package was fetched during warm install'
  e2e_offline_restart
  linux_client deb-flat offline "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_apk_case() {
  printf '\n[apk] native index/install, published generation, warm package and offline restart\n'
  e2e_reset_fixture_counts
  local script='
    printf "%s/apk/v3.20/main\n" "$1" >/tmp/repositories
    apk add --no-cache --allow-untrusted --repositories-file /tmp/repositories e2e-apk >/dev/null
    grep -q cache-proxy-e2e /usr/share/e2e-apk/payload.txt
  '
  linux_client apk cold "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_wait_cache_hit "$E2E_PROXY_URL/apk/v3.20/main/x86_64/APKINDEX.tar.gz"
  local package_path=/apk/v3.20/main/x86_64/e2e-apk-1.0.0-r0.apk before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'APK package did not reach the fixture'
  linux_client apk warm "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET "$package_path" "$before" 'APK package was fetched during warm install'
  e2e_offline_restart
  linux_client apk offline "$E2E_ALPINE_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_deb_apk_suite() {
  run_deb_standard_case
  run_deb_flat_case
  run_apk_case
}
