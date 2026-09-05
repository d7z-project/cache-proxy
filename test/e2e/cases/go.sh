#!/usr/bin/env bash

e2e_prepare_go() {
  e2e_pull_image "$E2E_GO_IMAGE"
}

e2e_run_go() {
	printf '\n[go] module download, warm zip, list update and persisted offline reuse\n'
	e2e_reset_fixture
	e2e_assert_strict_path go "$E2E_PROXY_URL" /go /go
  local script='
    mkdir /tmp/module && cd /tmp/module
    go mod init client.example/e2e >/dev/null
    GOPROXY="$1/go" GOSUMDB=off GOPRIVATE= go mod download "example.com/e2e@$2"
  '
  e2e_client go cold "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL" v1.0.0
  local before
  before=$(e2e_fixture_count GET /go/example.com/e2e/@v/v1.0.0.zip)
  ((before >= 1)) || e2e_fail 'Go module zip did not reach the fixture'
  e2e_client go warm "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL" v1.0.0
  e2e_assert_count_unchanged GET /go/example.com/e2e/@v/v1.0.0.zip "$before" 'Go module zip was fetched during warm download'
  e2e_set_fixture_state updated
  e2e_wait_contains "$E2E_PROXY_URL/go/example.com/e2e/@v/list" v1.1.0
  e2e_client go update "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL" v1.1.0
  e2e_wait_cache_hit "$E2E_PROXY_URL/go/example.com/e2e/@v/list"
  e2e_offline_restart
  e2e_client go offline "$E2E_GO_IMAGE" "$script" "$E2E_PROXY_URL" v1.1.0
  e2e_assert_offline_validation go "$E2E_PROXY_URL/go/example.com/e2e/@v/list"
  e2e_restore_online
}
