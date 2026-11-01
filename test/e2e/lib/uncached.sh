#!/usr/bin/env bash

e2e_uncached_native() {
  local mode=$1 prefix=$2 probe=$3 callback=$4 initial=$5 updated=$6 before after
  printf '\n[%s uncached] repeat, update, restart and upstream outage\n' "$mode"
  "$callback" uncached-cold "$initial"
  before=$(e2e_fixture_prefix_count GET "$prefix")
  ((before > 0)) || e2e_fail "$mode did not reach upstream"
  "$callback" uncached-repeat "$initial"
  after=$(e2e_fixture_prefix_count GET "$prefix")
  ((after > before)) || e2e_fail "$mode reused cached content"
  e2e_set_fixture_state updated
  "$callback" uncached-update "$updated"
  e2e_stop_proxy
  e2e_start_proxy
  "$callback" uncached-restart "$updated"
  e2e_offline_restart
  e2e_client "$mode" uncached-offline "$E2E_TOOLS_IMAGE" '
    code=$(curl --silent --show-error --max-time 10 -o /dev/null -w "%{http_code}" "$1")
    test "$code" = 502 -o "$code" = 504
  ' "$probe"
  e2e_restore_online
}

e2e_uncached_install() {
  local mode=$1 image=$2 script=$3 prefix=$4 initial=$5 updated=$6
  e2e_uncached_install_client() {
    local payload=cache-proxy-e2e-initial
    case $1 in *update*|*restart*) payload=cache-proxy-e2e-updated ;; esac
    e2e_client "$mode" "$1" "$image" "$script" "$E2E_PROXY_URL" "$2" "$payload"
  }
  e2e_uncached_native "$mode" "$prefix" "$E2E_PROXY_URL$prefix" e2e_uncached_install_client "$initial" "$updated"
  unset -f e2e_uncached_install_client
}
