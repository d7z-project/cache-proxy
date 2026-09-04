#!/usr/bin/env bash

e2e_prepare_deb() {
  e2e_pull_image "$E2E_DEBIAN_IMAGE"
}

e2e_run_deb_repository() {
  local layout=$1 source=$2 anchor=$3 package_path=$4 metadata_path=$5 unavailable_path=$6
  printf '\n[deb-%s] apt update/install, generation update, warm package and offline restart\n' "$layout"
  e2e_reset_fixture
  e2e_set_fixture_fault "$unavailable_path" 404
  e2e_client "deb-$layout" fixture-fault "$E2E_TOOLS_IMAGE" '
    status=$(curl --silent --show-error --output /dev/null --write-out "%{http_code}" "$1$2")
    if [ "$status" != 404 ]; then
      printf "fixture fault was not active: status=%s\n" "$status" >&2
      exit 1
    fi
  ' "$E2E_FIXTURE_URL" "$unavailable_path"
  local script='
    rm -f /etc/apt/sources.list.d/*
    printf "%s\n" "$2" >/etc/apt/sources.list
    apt-get -o Acquire::Check-Date=false -o Acquire::Languages=none update >/dev/null
    apt-get -o Acquire::Check-Date=false install -y --no-install-recommends e2e-deb >/dev/null
    grep -qx "$3" /usr/share/e2e-deb/payload.txt
  '
  e2e_client "deb-$layout" cold "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-initial
  e2e_wait_cache_hit "$E2E_PROXY_URL$anchor"
  e2e_wait_cache_hit "$E2E_PROXY_URL$metadata_path"
  e2e_assert_bypass_status "deb-$layout-unavailable" "$E2E_PROXY_URL$unavailable_path" 404
  e2e_clear_fixture_fault "$unavailable_path"
  local unavailable_before unavailable_after
  unavailable_before=$(e2e_fixture_count GET "$unavailable_path")
  e2e_client "deb-$layout" recovered "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error "$1$2" >/dev/null
    curl --fail --silent --show-error "$1$2" >/dev/null
  ' "$E2E_PROXY_URL" "$unavailable_path"
  e2e_wait_cache_hit "$E2E_PROXY_URL$unavailable_path"
  unavailable_after=$(e2e_fixture_count GET "$unavailable_path")
  if ((unavailable_after <= unavailable_before)); then
    e2e_fail "Debian $layout recovered metadata did not reach the upstream"
  fi
  e2e_set_fixture_fault "$unavailable_path" 404
  local before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail "Debian $layout package did not reach the fixture"
  e2e_client "deb-$layout" warm "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-initial
  e2e_assert_count_unchanged GET "$package_path" "$before" "Debian $layout package was fetched during warm install"
  local previous_etag
  previous_etag=$(e2e_response_header "$E2E_PROXY_URL$anchor" ETag)
  e2e_set_fixture_state updated
  e2e_wait_header_changed "$E2E_PROXY_URL$anchor" ETag "$previous_etag"
  e2e_client "deb-$layout" update "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-updated
  e2e_assert_bypass_status "deb-$layout-updated-unavailable" "$E2E_PROXY_URL$unavailable_path" 404
  e2e_offline_restart
  e2e_client "deb-$layout" offline "$E2E_DEBIAN_IMAGE" "$script" "$E2E_PROXY_URL" "$source" cache-proxy-e2e-updated
  e2e_restore_online
}

e2e_run_deb() {
  e2e_reset_fixture
  e2e_assert_transparent_paths deb /deb /deb bypass
  e2e_run_deb_repository standard "deb [arch=amd64 trusted=yes] $E2E_PROXY_URL/deb stable main" \
    /deb/dists/stable/InRelease /deb/pool/main/e/e2e-deb/e2e-deb_1.0.0+e2e1_all.deb \
    /deb/dists/stable/main/binary-amd64/Packages.gz /deb/dists/stable/main/binary-arm64/Packages.gz
  e2e_run_deb_repository flat "deb [arch=amd64 trusted=yes] $E2E_PROXY_URL/deb/flat ./" \
    /deb/flat/InRelease /deb/flat/e2e-deb_1.0.0+e2e1_all.deb \
    /deb/flat/Packages.gz /deb/flat/Contents-all.gz
}
