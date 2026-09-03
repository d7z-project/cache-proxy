#!/usr/bin/env bash

e2e_prepare_oci() {
  e2e_pull_image "$E2E_CRANE_IMAGE"
}

e2e_oci_pull() {
  local phase=$1 expected=$2 config
  e2e_run_client "${E2E_RUN_ID}-oci-${phase}" "$E2E_CRANE_IMAGE" \
    pull --insecure "$E2E_OCI_URL/e2e/image:latest" /tmp/e2e-image.tar
  config=$(e2e_run_client "${E2E_RUN_ID}-oci-${phase}-config" "$E2E_CRANE_IMAGE" \
    config --insecure "$E2E_OCI_URL/e2e/image:latest")
  [[ $config == *\"fixture.state\":\"$expected\"* ]] || e2e_fail "OCI $phase pull did not resolve fixture state $expected"
}

e2e_run_oci() {
	printf '\n[oci] Distribution pull, tag update, warm blobs and offline restart\n'
	e2e_reset_fixture
	e2e_assert_strict_path oci "http://$E2E_OCI_URL" "" /oci
  e2e_oci_pull cold initial
  local blobs_before blobs_after
  blobs_before=$(e2e_fixture_prefix_count GET /oci/v2/e2e/image/blobs/)
  ((blobs_before >= 2)) || e2e_fail 'OCI config and layer did not reach the fixture'
  e2e_oci_pull warm initial
  blobs_after=$(e2e_fixture_prefix_count GET /oci/v2/e2e/image/blobs/)
  e2e_assert_eq "$blobs_before" "$blobs_after" 'OCI blobs were fetched during warm pull'

  e2e_set_fixture_state updated
  e2e_client oci refresh "$E2E_TOOLS_IMAGE" '
    accept="application/vnd.docker.distribution.manifest.v1+json,application/vnd.docker.distribution.manifest.v1+prettyjws,application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.index.v1+json"
    curl --fail --silent --show-error -H "Accept: $accept" -H "Cache-Control: no-cache" \
		"$1/v2/e2e/image/manifests/latest" | grep -Fq '"'"'"fixture.state":"updated"'"'"'
  ' "http://$E2E_OCI_URL"
  e2e_oci_pull update updated

  e2e_offline_restart
  e2e_oci_pull offline updated
  e2e_restore_online
}
