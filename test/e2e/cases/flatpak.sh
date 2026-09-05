#!/usr/bin/env bash

e2e_prepare_flatpak() {
  e2e_build_image "$E2E_FLATPAK_CLIENT_IMAGE" "$E2E_ROOT/test/e2e/clients/flatpak/Containerfile" "$E2E_ROOT/test/e2e/clients/flatpak"
}

e2e_flatpak_install() {
  local phase=$1 expected=$2
  e2e_client flatpak "$phase" "$E2E_FLATPAK_CLIENT_IMAGE" '
    export HOME=/tmp/home XDG_DATA_HOME=/tmp/data XDG_CACHE_HOME=/tmp/cache XDG_RUNTIME_DIR=/tmp/runtime
    mkdir -p "$HOME" "$XDG_DATA_HOME" "$XDG_CACHE_HOME" "$XDG_RUNTIME_DIR"
	    chmod 700 "$XDG_RUNTIME_DIR"
	    flatpak --user remote-add --if-not-exists --no-gpg-verify e2e "$1/flatpak"
	    flatpak --user install -y --noninteractive e2e org.example.E2E >/dev/null
	    flatpak --user list --app | grep -q org.example.E2E
	    location=$(flatpak --user info --show-location org.example.E2E)
	    grep -qx "echo $2" "$location/files/bin/e2e"
	  ' "$E2E_PROXY_URL" "$expected"
}

e2e_run_flatpak() {
  printf '\n[flatpak] remote/install, generation update, warm objects and offline cache persistence\n'
  e2e_reset_fixture
  e2e_assert_transparent_paths flatpak /flatpak /flatpak/repo bypass
  local unrelated_summary upstream_unrelated proxy_unrelated
  unrelated_summary=$(e2e_run_client_shell "${E2E_RUN_ID}-flatpak-unrelated" "$E2E_TOOLS_IMAGE" \
    'curl --fail --silent --show-error "$1/flatpak/unrelated-summary-path.txt"' "$E2E_FIXTURE_URL")
  upstream_unrelated="/flatpak/repo/$unrelated_summary"
  proxy_unrelated="$E2E_PROXY_URL/flatpak/$unrelated_summary"
  e2e_set_fixture_fault "$upstream_unrelated" 404
  e2e_flatpak_install cold cache-proxy-e2e-initial
  e2e_wait_cache_hit "$E2E_PROXY_URL/flatpak/summary.idx"
  e2e_assert_eq 0 "$(e2e_fixture_count GET "$upstream_unrelated")" 'Flatpak fetched an unrelated architecture summary'
  e2e_assert_bypass_status flatpak-unrelated-summary "$proxy_unrelated" 404
  e2e_clear_fixture_fault "$upstream_unrelated"
  e2e_client flatpak recovered-summary "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error "$1" >/dev/null
  ' "$proxy_unrelated"
  e2e_wait_cache_hit "$proxy_unrelated"
  local objects_before paths_before commitmeta_before delta_indexes_before delta_index_paths_before
  paths_before=$(e2e_fixture_counts TRANSFER /flatpak/repo/objects/)
  objects_before=$(awk '$1 !~ /\.commitmeta$/ { count += $2 } END { print count + 0 }' <<<"$paths_before")
  ((objects_before >= 1)) || e2e_fail 'Flatpak objects did not reach the fixture'
  commitmeta_before=$(awk '$1 ~ /\.commitmeta$/ { count += $2 } END { print count + 0 }' <<<"$paths_before")
  delta_indexes_before=$(e2e_fixture_prefix_count TRANSFER /flatpak/repo/delta-indexes/)
  ((delta_indexes_before >= 1)) || e2e_fail 'Flatpak delta index did not reach the fixture'
  delta_index_paths_before=$(e2e_fixture_counts TRANSFER /flatpak/repo/delta-indexes/)
  e2e_flatpak_install warm cache-proxy-e2e-initial
  local objects_after paths_after commitmeta_after delta_indexes_after
  paths_after=$(e2e_fixture_counts TRANSFER /flatpak/repo/objects/)
  objects_after=$(awk '$1 !~ /\.commitmeta$/ { count += $2 } END { print count + 0 }' <<<"$paths_after")
  if [[ $objects_after != "$objects_before" ]]; then
    printf '%s\n' '--- Flatpak object transfers after cold install ---' "$paths_before" >&2
    printf '%s\n' '--- Flatpak object transfers after warm install ---' >&2
    e2e_fixture_counts TRANSFER /flatpak/repo/objects/ >&2
  fi
  e2e_assert_eq "$objects_before" "$objects_after" 'Flatpak object bodies were transferred during warm install'
  commitmeta_after=$(awk '$1 ~ /\.commitmeta$/ { count += $2 } END { print count + 0 }' <<<"$paths_after")
  ((commitmeta_after > commitmeta_before)) || e2e_fail 'Flatpak detached metadata availability was not rechecked'
  delta_indexes_after=$(e2e_fixture_prefix_count TRANSFER /flatpak/repo/delta-indexes/)
  if ((delta_indexes_after <= delta_indexes_before)); then
    printf '%s\n' '--- Flatpak delta-index transfers after cold install ---' "$delta_index_paths_before" >&2
    printf '%s\n' '--- Flatpak delta-index transfers after warm install ---' >&2
    e2e_fixture_counts TRANSFER /flatpak/repo/delta-indexes/ >&2
  fi
  ((delta_indexes_after > delta_indexes_before)) || e2e_fail 'Flatpak delta-index availability was not rechecked'
  local previous_etag
  previous_etag=$(e2e_response_header "$E2E_PROXY_URL/flatpak/summary.idx" ETag)
  e2e_set_fixture_state updated
  e2e_wait_header_changed "$E2E_PROXY_URL/flatpak/summary.idx" ETag "$previous_etag"
  e2e_flatpak_install update cache-proxy-e2e-updated
  local cached_object_paths
  cached_object_paths=$(e2e_fixture_counts TRANSFER /flatpak/repo/objects/ |
    awk '$1 !~ /\.commitmeta$/ { print $1 }')
  [[ -n $cached_object_paths ]] || e2e_fail 'Flatpak install did not cache any immutable objects'
  e2e_wait_cache_hit "$E2E_PROXY_URL/flatpak/summary.idx"
  e2e_offline_restart
  e2e_client flatpak offline "$E2E_TOOLS_IMAGE" '
	    curl --fail --silent --show-error "$1/flatpak/summary.idx" >/dev/null
	    printf "%s\n" "$2" | while IFS= read -r upstream_path; do
	      [ -n "$upstream_path" ] || continue
	      object_path=${upstream_path#/flatpak/repo}
	      curl --fail --silent --show-error "$1/flatpak$object_path" >/dev/null
	    done
	  ' "$E2E_PROXY_URL" "$cached_object_paths"
  e2e_assert_offline_validation flatpak "$E2E_PROXY_URL/flatpak/summary.idx"
  e2e_restore_online
}
