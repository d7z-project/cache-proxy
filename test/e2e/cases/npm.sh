#!/usr/bin/env bash

e2e_prepare_npm() {
  e2e_pull_image "$E2E_NODE_IMAGE"
}

e2e_run_npm() {
  printf '\n[npm] install, audit, warm tarball, update and offline reuse\n'
  e2e_reset_fixture
  local install_script='
    mkdir /tmp/project && cd /tmp/project
    printf "{\"name\":\"consumer\",\"version\":\"1.0.0\",\"dependencies\":{\"e2e-pkg\":\"%s\"}}\n" "$2" >package.json
    npm install --registry="$1/npm/" --prefer-online --ignore-scripts --no-fund --no-audit
    test "$(node -e "process.stdout.write(require(\"e2e-pkg\"))")" = "$3"
  '
  e2e_client npm cold "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL" 1.0.0 cache-proxy-e2e-initial
  e2e_client npm audit "$E2E_NODE_IMAGE" '
    mkdir /tmp/project && cd /tmp/project
    printf "{\"name\":\"consumer\",\"version\":\"1.0.0\",\"dependencies\":{\"e2e-pkg\":\"1.0.0\"}}\n" >package.json
    npm install --registry="$1/npm/" --ignore-scripts --no-fund --no-audit >/dev/null
    npm audit --registry="$1/npm/" --audit-level=critical >/dev/null
  ' "$E2E_PROXY_URL"
  local before npm_accept
  before=$(e2e_fixture_count GET /npm/e2e-pkg/-/e2e-pkg-1.0.0.tgz)
  ((before >= 1)) || e2e_fail 'npm cold tarball request did not reach the fixture'
  e2e_client npm warm "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL" 1.0.0 cache-proxy-e2e-initial
  e2e_assert_count_unchanged GET /npm/e2e-pkg/-/e2e-pkg-1.0.0.tgz "$before" 'npm tarball was fetched during warm install'
  npm_accept=$(e2e_fixture_header GET /npm/e2e-pkg Accept)
  [[ -n $npm_accept ]] || e2e_fail 'npm packument request did not carry an Accept header'
  e2e_set_fixture_state updated
  e2e_client npm refresh "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error \
      -H "Accept: $2" -H "Cache-Control: no-cache" "$1/npm/e2e-pkg" | grep -Fq '"'"'"latest":"2.0.0"'"'"'
  ' "$E2E_PROXY_URL" "$npm_accept"
  e2e_client npm update "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL" 2.0.0 cache-proxy-e2e-updated
  e2e_offline_restart
  e2e_client npm offline "$E2E_NODE_IMAGE" "$install_script" "$E2E_PROXY_URL" 2.0.0 cache-proxy-e2e-updated
  e2e_restore_online
}
