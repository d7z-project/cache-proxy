#!/usr/bin/env bash

e2e_prepare_pypi() {
  e2e_pull_image "$E2E_PYTHON_IMAGE"
}

e2e_run_pypi() {
  printf '\n[pypi] pip download, warm wheel, project update and persisted offline reuse\n'
  e2e_reset_fixture
  local script='
    mkdir /tmp/download
    pip download --disable-pip-version-check --no-cache-dir --no-deps \
      --refresh-package e2e-pkg \
      --trusted-host 127.0.0.1 --index-url "$1/pypi/simple/" \
      --dest /tmp/download "e2e-pkg==$2" >/dev/null
    test -f "/tmp/download/e2e_pkg-$2-py3-none-any.whl"
  '
  e2e_client pypi cold "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL" 1.0.0
  local before
  before=$(e2e_fixture_count GET /pypi/files/e2e_pkg-1.0.0-py3-none-any.whl)
  ((before >= 1)) || e2e_fail 'PyPI wheel did not reach the fixture'
  e2e_client pypi warm "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL" 1.0.0
  e2e_assert_count_unchanged GET /pypi/files/e2e_pkg-1.0.0-py3-none-any.whl "$before" 'PyPI wheel was fetched during warm download'
  e2e_set_fixture_state updated
  e2e_wait_contains "$E2E_PROXY_URL/pypi/simple/e2e-pkg/" e2e_pkg-2.0.0-py3-none-any.whl
  e2e_client pypi update "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL" 2.0.0
  e2e_offline_restart
  e2e_client pypi offline "$E2E_PYTHON_IMAGE" "$script" "$E2E_PROXY_URL" 2.0.0
  e2e_restore_online
}
