#!/usr/bin/env bash

e2e_prepare_deb() {
  e2e_pull_image "$E2E_DEBIAN_IMAGE"
}

e2e_run_deb_repository() {
  local layout=$1 source=$2 anchor=$3 package_path=$4
  printf '\n[deb-%s] apt update/install, generation update, warm package and offline restart\n' "$layout"
  e2e_reset_fixture
  local script='
    rm -f /etc/apt/sources.list.d/*
    printf "%s\n" "$2" >/etc/apt/sources.list
    apt-get -o Acquire::Check-Date=false -o Acquire::Languages=none update >/dev/null
    apt-get -o Acquire::Check-Date=false install -y --no-install-recommends e2e-deb >/dev/null
    grep -qx "$3" /usr/share/e2e-deb/payload.txt
  '
  e2e_client "deb-$layout" cold "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-initial
  e2e_wait_cache_hit "$E2E_PROXY_URL$anchor"
  local before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail "Debian $layout package did not reach the fixture"
  e2e_client "deb-$layout" warm "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-initial
  e2e_assert_count_unchanged GET "$package_path" "$before" "Debian $layout package was fetched during warm install"
  e2e_set_fixture_state updated
  e2e_wait_contains "$E2E_PROXY_URL$anchor" 'repository updated'
  e2e_client "deb-$layout" update "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-updated
  e2e_offline_restart
  e2e_client "deb-$layout" offline "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-updated
  e2e_restore_online
}

e2e_run_deb() {
	e2e_reset_fixture
	e2e_assert_transparent_paths deb /deb /deb bypass
	e2e_run_deb_repository standard "deb [trusted=yes] $E2E_PROXY_URL/deb stable main" \
    /deb/dists/stable/Release /deb/pool/main/e/e2e-deb/e2e-deb_1.0.0+e2e1_all.deb
  e2e_run_deb_repository flat "deb [trusted=yes] $E2E_PROXY_URL/deb/flat ./" \
    /deb/flat/Release /deb/flat/e2e-deb_1.0.0+e2e1_all.deb
}
