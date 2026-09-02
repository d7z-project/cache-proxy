#!/usr/bin/env bash

repository_client() {
  local case_name=$1 phase=$2 image=$3 script=$4
  shift 4
  e2e_run_client_shell "${E2E_RUN_ID}-${case_name}-${phase}" "$image" "$script" "$@"
}

run_rpm_case() {
  printf '\n[rpm] dnf metadata/install, published closure, warm RPM and offline restart\n'
  e2e_reset_fixture_counts
  local script='
    mkdir -p /tmp/repos
    cat >/tmp/repos/e2e.repo <<EOF
[e2e]
name=cache-proxy-e2e
baseurl=$1/rpm
enabled=1
gpgcheck=0
repo_gpgcheck=0
metadata_expire=0
EOF
    dnf -y --setopt=reposdir=/tmp/repos --disablerepo="*" --enablerepo=e2e --nogpgcheck install e2e-rpm >/dev/null
    grep -q cache-proxy-e2e /usr/share/e2e-rpm/payload.txt
  '
  repository_client rpm cold "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_wait_cache_hit "$E2E_PROXY_URL/rpm/repodata/repomd.xml"
  local package_path=/rpm/e2e-rpm-1.0.0-1.noarch.rpm before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'RPM package did not reach the fixture'
  repository_client rpm warm "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET "$package_path" "$before" 'RPM package was fetched during warm install'
  e2e_offline_restart
  repository_client rpm offline "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_pacman_case() {
  printf '\n[pacman] database/install, published generation, warm package and offline restart\n'
  e2e_reset_fixture_counts
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
    grep -q cache-proxy-e2e /usr/share/e2e-pacman/payload.txt
  '
  repository_client pacman cold "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_wait_cache_hit "$E2E_PROXY_URL/pacman/e2e.db"
  local package_path=/pacman/e2e-pacman-1.0.0-1-any.pkg.tar.zst before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'Pacman package did not reach the fixture'
  repository_client pacman warm "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_assert_count_unchanged GET "$package_path" "$before" 'Pacman package was fetched during warm install'
  e2e_offline_restart
  repository_client pacman offline "$E2E_ARCH_IMAGE" "$script" "$E2E_PROXY_URL"
  e2e_restore_online
}

run_rpm_pacman_suite() {
  run_rpm_case
  run_pacman_case
}
