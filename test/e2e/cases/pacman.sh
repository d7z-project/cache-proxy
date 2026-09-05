#!/usr/bin/env bash

e2e_prepare_pacman() {
  e2e_pull_image "$E2E_ARCH_IMAGE"
}

e2e_run_pacman() {
	printf '\n[pacman] database/install, generation update, warm package and offline restart\n'
	e2e_reset_fixture
	e2e_assert_transparent_paths pacman /pacman /pacman bypass
  local script='
    cat >/tmp/pacman.conf <<EOF
[options]
Architecture = auto
SigLevel = Never
LocalFileSigLevel = Never
[e2e]
Server = $1/pacman
SigLevel = Never
EOF
    pacman -Sy --noconfirm --config /tmp/pacman.conf e2e-pacman >/dev/null
    grep -qx "$2" /usr/share/e2e-pacman/payload.txt
  '
  e2e_client pacman cold "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-initial
  e2e_wait_cache_hit "$E2E_PROXY_URL/pacman/e2e.db"
  local package_path=/pacman/e2e-pacman-1.0.0-1-any.pkg.tar.zst before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'Pacman package did not reach the fixture'
  e2e_client pacman warm "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-initial
  e2e_assert_count_unchanged GET "$package_path" "$before" 'Pacman package was fetched during warm install'
  local previous_etag
  previous_etag=$(e2e_response_header "$E2E_PROXY_URL/pacman/e2e.db" ETag)
  e2e_set_fixture_state updated
  e2e_wait_header_changed "$E2E_PROXY_URL/pacman/e2e.db" ETag "$previous_etag"
  e2e_client pacman update "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-updated
  e2e_wait_cache_hit "$E2E_PROXY_URL/pacman/e2e.db"
  e2e_offline_restart
  e2e_client pacman offline "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-updated
  e2e_assert_offline_validation pacman "$E2E_PROXY_URL/pacman/e2e.db"
  e2e_restore_online
}
