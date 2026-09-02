#!/usr/bin/env bash

e2e_find_port() {
  local attempt port
  for ((attempt = 0; attempt < 100; attempt++)); do
    port=$((20000 + RANDOM % 30000))
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      printf '%s\n' "$port"
      return 0
    fi
  done
  e2e_fail 'unable to allocate a loopback port'
}

e2e_init_lifecycle() {
  E2E_FIXTURE_PORT=$(e2e_find_port)
  E2E_PROXY_PORT=$(e2e_find_port)
  while [[ $E2E_PROXY_PORT == "$E2E_FIXTURE_PORT" ]]; do
    E2E_PROXY_PORT=$(e2e_find_port)
  done
  E2E_OCI_PORT=$(e2e_find_port)
  while [[ $E2E_OCI_PORT == "$E2E_FIXTURE_PORT" || $E2E_OCI_PORT == "$E2E_PROXY_PORT" ]]; do
    E2E_OCI_PORT=$(e2e_find_port)
  done

  E2E_FIXTURE_URL="http://127.0.0.1:$E2E_FIXTURE_PORT"
  E2E_PROXY_URL="http://127.0.0.1:$E2E_PROXY_PORT"
  E2E_OCI_URL="127.0.0.1:$E2E_OCI_PORT"
  E2E_FIXTURE_CONTAINER="${E2E_RUN_ID}-fixture"
  E2E_PROXY_CONTAINER="${E2E_RUN_ID}-proxy"
  E2E_BACKEND_VOLUME="${E2E_RUN_ID}-backend"
  E2E_CONFIG_FILE="$E2E_WORK_DIR/cache-proxy.yaml"
  export E2E_FIXTURE_PORT E2E_PROXY_PORT E2E_OCI_PORT
  export E2E_FIXTURE_URL E2E_PROXY_URL E2E_OCI_URL
  export E2E_FIXTURE_CONTAINER E2E_PROXY_CONTAINER E2E_BACKEND_VOLUME E2E_CONFIG_FILE

  e2e_runtime volume create --label "$E2E_OWNER_LABEL" "$E2E_BACKEND_VOLUME" >/dev/null
  e2e_write_config
}

e2e_write_config() {
  sed \
    -e "s|@PROXY_PORT@|$E2E_PROXY_PORT|g" \
    -e "s|@PROXY_URL@|$E2E_PROXY_URL|g" \
    -e "s|@OCI_PORT@|$E2E_OCI_PORT|g" \
    -e "s|@OCI_URL@|$E2E_OCI_URL|g" \
    -e "s|@FIXTURE_URL@|$E2E_FIXTURE_URL|g" \
    "$E2E_ROOT/test/e2e/config/cache-proxy.yaml.in" >"$E2E_CONFIG_FILE"
}

e2e_start_fixture() {
  e2e_runtime rm -f "$E2E_FIXTURE_CONTAINER" >/dev/null 2>&1 || true
  e2e_runtime run -d --network host \
    --name "$E2E_FIXTURE_CONTAINER" \
    --label "$E2E_OWNER_LABEL" \
    "$E2E_FIXTURE_IMAGE" \
    -addr "127.0.0.1:$E2E_FIXTURE_PORT" \
    -root /srv/fixture \
    -public-url "$E2E_FIXTURE_URL" >/dev/null
  e2e_wait_http "${E2E_RUN_ID}-wait-fixture" "$E2E_FIXTURE_URL/__e2e/ready"
}

e2e_stop_fixture() {
  e2e_runtime rm -f "$E2E_FIXTURE_CONTAINER" >/dev/null 2>&1 || true
}

e2e_start_proxy() {
  e2e_runtime rm -f "$E2E_PROXY_CONTAINER" >/dev/null 2>&1 || true
  e2e_runtime run -d --network host \
    --name "$E2E_PROXY_CONTAINER" \
    --label "$E2E_OWNER_LABEL" \
    -v "$E2E_BACKEND_VOLUME:/data" \
    -v "$E2E_CONFIG_FILE:/etc/cache-proxy.yaml:ro" \
    "$E2E_PROXY_IMAGE" -config /etc/cache-proxy.yaml >/dev/null
  e2e_wait_http "${E2E_RUN_ID}-wait-proxy" "$E2E_PROXY_URL/-/status/summary"
  if [[ ${E2E_OFFLINE:-0} != 1 ]]; then
    e2e_wait_http "${E2E_RUN_ID}-wait-oci" "http://$E2E_OCI_URL/v2/"
  fi
}

e2e_stop_proxy() {
  e2e_runtime stop --time 15 "$E2E_PROXY_CONTAINER" >/dev/null 2>&1 || true
  e2e_runtime rm -f "$E2E_PROXY_CONTAINER" >/dev/null 2>&1 || true
}

e2e_offline_restart() {
  e2e_stop_fixture
  e2e_stop_proxy
  E2E_OFFLINE=1
  e2e_start_proxy
}

e2e_restore_online() {
  e2e_stop_proxy
  unset E2E_OFFLINE
  e2e_start_fixture
  e2e_start_proxy
}

e2e_dump_diagnostics() {
  printf '\n===== E2E diagnostics (%s) =====\n' "$E2E_RUN_ID" >&2
  printf '%s\n' '--- proxy logs ---' >&2
  if [[ -n ${E2E_PROXY_CONTAINER:-} ]]; then
    e2e_runtime logs "$E2E_PROXY_CONTAINER" >&2 2>/dev/null || true
  fi
  printf '%s\n' '--- fixture logs ---' >&2
  if [[ -n ${E2E_FIXTURE_CONTAINER:-} ]]; then
    e2e_runtime logs "$E2E_FIXTURE_CONTAINER" >&2 2>/dev/null || true
  fi
  printf '%s\n' '--- owned resources ---' >&2
  e2e_runtime ps -a --filter "label=$E2E_OWNER_LABEL" >&2 2>/dev/null || true
}

e2e_cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if ((status != 0)); then
    e2e_dump_diagnostics
  fi
  e2e_runtime ps -aq --filter "label=$E2E_OWNER_LABEL" 2>/dev/null | while IFS= read -r id; do
    [[ -z $id ]] || e2e_runtime rm -f "$id" >/dev/null 2>&1 || true
  done
  if [[ -n ${E2E_BACKEND_VOLUME:-} ]]; then
    e2e_runtime volume rm -f "$E2E_BACKEND_VOLUME" >/dev/null 2>&1 || true
  fi
  for image in "$E2E_PROXY_IMAGE" "$E2E_FIXTURE_IMAGE" "$E2E_TOOLS_IMAGE" "$E2E_MAVEN_CLIENT_IMAGE" "$E2E_FLATPAK_CLIENT_IMAGE"; do
    e2e_runtime image rm -f "$image" >/dev/null 2>&1 || true
  done
  rm -rf "$E2E_WORK_DIR"
  exit "$status"
}
