#!/usr/bin/env bash

e2e_prepare_git() {
  :
}

e2e_git_clone() {
  local phase=$1 expected=$2
  e2e_client git "$phase" "$E2E_TOOLS_IMAGE" '
    git -c protocol.version=0 clone -q "$1/git" /tmp/repo
    git -C /tmp/repo -c protocol.version=0 fetch -q origin
    grep -qx "$2" /tmp/repo/README.md
  ' "$E2E_PROXY_URL" "$expected"
}

e2e_run_git() {
  printf '\n[git] smart HTTP clone, local mirror update and offline restart\n'
  e2e_reset_fixture
  e2e_git_clone cold cache-proxy-e2e-initial

  local before_get before_post after_get after_post ready=0 attempt
  for ((attempt = 0; attempt < 30; attempt++)); do
    before_get=$(e2e_fixture_count GET /git/repo.git/info/refs)
    before_post=$(e2e_fixture_count POST /git/repo.git/git-upload-pack)
    e2e_git_clone warm-probe cache-proxy-e2e-initial
    after_get=$(e2e_fixture_count GET /git/repo.git/info/refs)
    after_post=$(e2e_fixture_count POST /git/repo.git/git-upload-pack)
    if [[ $before_get == "$after_get" && $before_post == "$after_post" ]]; then
      ready=1
      break
    fi
    sleep 1
  done
  e2e_assert_eq 1 "$ready" 'Git mirror did not become locally readable'

  e2e_set_fixture_state updated
  ready=0
  for ((attempt = 0; attempt < 60; attempt++)); do
    if e2e_git_clone update-probe cache-proxy-e2e-updated; then
      ready=1
      break
    fi
    sleep 1
  done
  e2e_assert_eq 1 "$ready" 'Git mirror did not publish the updated upstream ref'

  e2e_offline_restart
  e2e_git_clone offline cache-proxy-e2e-updated
  e2e_restore_online
}
