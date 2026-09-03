#!/usr/bin/env bash

e2e_prepare_rpm() {
  e2e_pull_image "$E2E_FEDORA_IMAGE"
}

e2e_run_rpm() {
  printf '\n[rpm] dnf metadata/install, closure update, warm RPM and offline restart\n'
  e2e_reset_fixture
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
    grep -qx "$2" /usr/share/e2e-rpm/payload.txt
  '
  e2e_client rpm cold "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-initial
  e2e_wait_cache_hit "$E2E_PROXY_URL/rpm/repodata/repomd.xml"
  local package_path=/rpm/e2e-rpm-1.0.0-1.noarch.rpm before
  before=$(e2e_fixture_count GET "$package_path")
  ((before >= 1)) || e2e_fail 'RPM package did not reach the fixture'
  e2e_client rpm warm "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-initial
  e2e_assert_count_unchanged GET "$package_path" "$before" 'RPM package was fetched during warm install'
  local previous_etag
  previous_etag=$(e2e_response_header "$E2E_PROXY_URL/rpm/repodata/repomd.xml" ETag)
  e2e_set_fixture_state updated
  e2e_wait_header_changed "$E2E_PROXY_URL/rpm/repodata/repomd.xml" ETag "$previous_etag"
  e2e_client rpm update "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-updated
  e2e_offline_restart
  e2e_client rpm offline "$E2E_FEDORA_IMAGE" "$script" "$E2E_PROXY_URL" cache-proxy-e2e-updated
  e2e_restore_online
}
