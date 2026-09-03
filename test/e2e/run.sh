#!/usr/bin/env bash
set -Eeuo pipefail

E2E_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$E2E_ROOT/test/e2e/images.env"
source "$E2E_ROOT/test/e2e/lib/runtime.sh"
source "$E2E_ROOT/test/e2e/lib/assert.sh"
source "$E2E_ROOT/test/e2e/lib/client.sh"
source "$E2E_ROOT/test/e2e/lib/lifecycle.sh"

suite=${E2E_SUITE:-all}
all_modes=(file npm go maven cargo pypi deb apk rpm pacman git oci flatpak)
if [[ $suite == all ]]; then
  selected_modes=("${all_modes[@]}")
else
  selected_modes=()
  for mode in "${all_modes[@]}"; do
    if [[ $suite == "$mode" ]]; then
      selected_modes=("$mode")
      break
    fi
  done
  if ((${#selected_modes[@]} == 0)); then
    printf 'unknown E2E_SUITE %q; expected all or one of: %s\n' "$suite" "${all_modes[*]}" >&2
    exit 2
  fi
fi

e2e_select_runtime
E2E_RUN_ID="cache-proxy-e2e-$(date +%s)-$$-$RANDOM"
E2E_OWNER_LABEL="cache-proxy.e2e.run=$E2E_RUN_ID"
E2E_WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cache-proxy-e2e.XXXXXX")
E2E_BACKEND_VOLUME=
E2E_PROXY_CONTAINER=
E2E_FIXTURE_CONTAINER=
E2E_PROXY_IMAGE="localhost/cache-proxy-e2e-proxy:$E2E_RUN_ID"
E2E_FIXTURE_IMAGE="localhost/cache-proxy-e2e-fixture:$E2E_RUN_ID"
E2E_TOOLS_IMAGE="localhost/cache-proxy-e2e-tools:$E2E_RUN_ID"
E2E_MAVEN_CLIENT_IMAGE="localhost/cache-proxy-e2e-maven:$E2E_RUN_ID"
E2E_FLATPAK_CLIENT_IMAGE="localhost/cache-proxy-e2e-flatpak:$E2E_RUN_ID"
export E2E_ROOT E2E_RUN_ID E2E_OWNER_LABEL E2E_WORK_DIR
export E2E_PROXY_IMAGE E2E_FIXTURE_IMAGE E2E_TOOLS_IMAGE E2E_MAVEN_CLIENT_IMAGE E2E_FLATPAK_CLIENT_IMAGE

for mode in "${selected_modes[@]}"; do
  source "$E2E_ROOT/test/e2e/cases/$mode.sh"
done

trap e2e_cleanup EXIT INT TERM

printf 'Runtime: %s\nSuite: %s\nRun ID: %s\n' "$E2E_RUNTIME" "$suite" "$E2E_RUN_ID"

e2e_build_image "$E2E_PROXY_IMAGE" "$E2E_ROOT/Dockerfile" "$E2E_ROOT"
e2e_build_image "$E2E_FIXTURE_IMAGE" "$E2E_ROOT/test/e2e/fixture/Containerfile" "$E2E_ROOT/test/e2e/fixture" --target "fixture-$suite"
e2e_build_image "$E2E_TOOLS_IMAGE" "$E2E_ROOT/test/e2e/clients/tools/Containerfile" "$E2E_ROOT/test/e2e/clients/tools"

for mode in "${selected_modes[@]}"; do
  "e2e_prepare_$mode"
done

e2e_init_lifecycle
e2e_start_fixture
e2e_start_proxy

for mode in "${selected_modes[@]}"; do
  "e2e_run_$mode"
done

printf '\nAll %s end-to-end cases passed.\n' "$suite"
