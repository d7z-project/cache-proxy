#!/usr/bin/env bash

distribution_client() {
  local case_name=$1 phase=$2 image=$3 script=$4
  shift 4
  e2e_run_client_shell "${E2E_RUN_ID}-${case_name}-${phase}" "$image" "$script" "$@"
}

git_clone_client() {
  local phase=$1
  distribution_client git "$phase" "$E2E_TOOLS_IMAGE" '
    git -c protocol.version=0 clone -q "$1/git" /tmp/repo
    git -C /tmp/repo -c protocol.version=0 fetch -q origin
    grep -q cache-proxy-e2e /tmp/repo/README.md
  ' "$E2E_PROXY_URL"
}

run_git_case() {
  printf '\n[git] smart HTTP clone, local mirror, receive rejection and offline restart\n'
  e2e_reset_fixture_counts
  git_clone_client cold

  local before_get before_post after_get after_post ready=0
  local attempt
  for ((attempt = 0; attempt < 30; attempt++)); do
    before_get=$(e2e_fixture_count GET /git/repo.git/info/refs)
    before_post=$(e2e_fixture_count POST /git/repo.git/git-upload-pack)
    git_clone_client warm-probe
    after_get=$(e2e_fixture_count GET /git/repo.git/info/refs)
    after_post=$(e2e_fixture_count POST /git/repo.git/git-upload-pack)
    if [[ $before_get == "$after_get" && $before_post == "$after_post" ]]; then
      ready=1
      break
    fi
    sleep 1
  done
  e2e_assert_eq 1 "$ready" 'Git mirror did not become locally readable'

  local receive_before
  receive_before=$(e2e_fixture_count POST /git/repo.git/git-receive-pack)
  distribution_client git push-rejected "$E2E_TOOLS_IMAGE" '
    git -c protocol.version=0 clone -q "$1/git" /tmp/repo
    cd /tmp/repo
    git config user.name e2e
    git config user.email e2e@example.invalid
    printf change >>README.md
    git add README.md && git commit -q -m change
    if git -c protocol.version=0 push origin HEAD:refs/heads/rejected >/tmp/push.log 2>&1; then
      cat /tmp/push.log >&2
      exit 1
    fi
  ' "$E2E_PROXY_URL"
  e2e_assert_count_unchanged POST /git/repo.git/git-receive-pack "$receive_before" 'Git receive-pack reached the upstream fixture'

  e2e_offline_restart
  git_clone_client offline
  e2e_restore_online
}

oci_pull_client() {
  local phase=$1
  e2e_run_client "${E2E_RUN_ID}-oci-${phase}" "$E2E_CRANE_IMAGE" \
    pull --insecure "$E2E_OCI_URL/e2e/image:1.0.0" /tmp/e2e-image.tar
}

run_oci_case() {
  printf '\n[oci] Distribution pull, warm blobs, mutation rejection and offline restart\n'
  e2e_reset_fixture_counts
  oci_pull_client cold
  local blobs_before
  blobs_before=$(e2e_fixture_prefix_count GET /oci/v2/e2e/image/blobs/)
  ((blobs_before >= 2)) || e2e_fail 'OCI config and layer did not reach the fixture'
  oci_pull_client warm
  local blobs_after
  blobs_after=$(e2e_fixture_prefix_count GET /oci/v2/e2e/image/blobs/)
  e2e_assert_eq "$blobs_before" "$blobs_after" 'OCI blobs were fetched during warm pull'

  local writes
  writes=$(e2e_fixture_count POST /oci/v2/e2e/image/blobs/uploads/)
  distribution_client oci write-rejected "$E2E_TOOLS_IMAGE" '
    status=$(curl --silent --output /dev/null --write-out "%{http_code}" -X POST "$1/v2/e2e/image/blobs/uploads/")
    test "$status" = 405
  ' "http://$E2E_OCI_URL"
  e2e_assert_count_unchanged POST /oci/v2/e2e/image/blobs/uploads/ "$writes" 'OCI upload initiation reached the upstream fixture'

  e2e_offline_restart
  oci_pull_client offline
  e2e_restore_online
}

flatpak_install_client() {
  local phase=$1
  distribution_client flatpak "$phase" "$E2E_FLATPAK_CLIENT_IMAGE" '
    export HOME=/tmp/home XDG_DATA_HOME=/tmp/data XDG_CACHE_HOME=/tmp/cache XDG_RUNTIME_DIR=/tmp/runtime
    mkdir -p "$HOME" "$XDG_DATA_HOME" "$XDG_CACHE_HOME" "$XDG_RUNTIME_DIR"
    chmod 700 "$XDG_RUNTIME_DIR"
    flatpak --user remote-add --if-not-exists --no-gpg-verify e2e "$1/flatpak"
    flatpak --user install -y --noninteractive e2e org.example.E2E >/dev/null
    flatpak --user list --app | grep -q org.example.E2E
  ' "$E2E_PROXY_URL"
}

run_flatpak_case() {
  printf '\n[flatpak] remote/install, generation metadata, warm objects and offline restart\n'
  e2e_reset_fixture_counts
  flatpak_install_client cold
  e2e_wait_cache_hit "$E2E_PROXY_URL/flatpak/summary.idx"
  local objects_before
  objects_before=$(e2e_fixture_prefix_count TRANSFER /flatpak/repo/objects/)
  ((objects_before >= 1)) || e2e_fail 'Flatpak objects did not reach the fixture'
  local paths_before
  paths_before=$(e2e_fixture_counts TRANSFER /flatpak/repo/objects/)
  local delta_indexes_before
  delta_indexes_before=$(e2e_fixture_prefix_count TRANSFER /flatpak/repo/delta-indexes/)
  ((delta_indexes_before >= 1)) || e2e_fail 'Flatpak delta index did not reach the fixture'
  local delta_index_paths_before
  delta_index_paths_before=$(e2e_fixture_counts TRANSFER /flatpak/repo/delta-indexes/)
  flatpak_install_client warm
  local objects_after
  objects_after=$(e2e_fixture_prefix_count TRANSFER /flatpak/repo/objects/)
  if [[ $objects_after != "$objects_before" ]]; then
    printf '%s\n' '--- Flatpak object transfers after cold install ---' "$paths_before" >&2
    printf '%s\n' '--- Flatpak object transfers after warm install ---' >&2
    e2e_fixture_counts TRANSFER /flatpak/repo/objects/ >&2
  fi
  e2e_assert_eq "$objects_before" "$objects_after" 'Flatpak object bodies were transferred during warm install'
  local delta_indexes_after
  delta_indexes_after=$(e2e_fixture_prefix_count TRANSFER /flatpak/repo/delta-indexes/)
  if [[ $delta_indexes_after != "$delta_indexes_before" ]]; then
    printf '%s\n' '--- Flatpak delta-index transfers after cold install ---' "$delta_index_paths_before" >&2
    printf '%s\n' '--- Flatpak delta-index transfers after warm install ---' >&2
    e2e_fixture_counts TRANSFER /flatpak/repo/delta-indexes/ >&2
  fi
  e2e_assert_eq "$delta_indexes_before" "$delta_indexes_after" 'Flatpak delta index was transferred during warm install'
  e2e_offline_restart
  flatpak_install_client offline
  e2e_restore_online
}

run_git_oci_flatpak_suite() {
  run_git_case
  run_oci_case
  run_flatpak_case
}
