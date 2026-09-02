#!/usr/bin/env bash
set -Eeuo pipefail

E2E_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$E2E_ROOT/test/e2e/images.env"
source "$E2E_ROOT/test/e2e/lib/runtime.sh"
source "$E2E_ROOT/test/e2e/lib/assert.sh"
source "$E2E_ROOT/test/e2e/lib/lifecycle.sh"

suite=${E2E_SUITE:-all}
case "$suite" in
  all | registries | deb-apk | rpm-pacman | git-oci-flatpak) ;;
  *)
    printf 'unknown E2E_SUITE %q; expected all, registries, deb-apk, rpm-pacman, or git-oci-flatpak\n' "$suite" >&2
    exit 2
    ;;
esac

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

trap e2e_cleanup EXIT INT TERM

printf 'Runtime: %s\nSuite: %s\nRun ID: %s\n' "$E2E_RUNTIME" "$suite" "$E2E_RUN_ID"

e2e_build_image "$E2E_PROXY_IMAGE" "$E2E_ROOT/Dockerfile" "$E2E_ROOT"
e2e_build_image "$E2E_FIXTURE_IMAGE" "$E2E_ROOT/test/e2e/fixture/Containerfile" "$E2E_ROOT/test/e2e/fixture"
e2e_build_image "$E2E_TOOLS_IMAGE" "$E2E_ROOT/test/e2e/client/Containerfile" "$E2E_ROOT/test/e2e/client" --target tools

case "$suite" in
  all | registries)
    e2e_pull_image "$E2E_NODE_IMAGE"
    e2e_pull_image "$E2E_GO_IMAGE"
    e2e_pull_image "$E2E_RUST_IMAGE"
    e2e_pull_image "$E2E_PYTHON_IMAGE"
    e2e_build_image "$E2E_MAVEN_CLIENT_IMAGE" "$E2E_ROOT/test/e2e/client/Containerfile" "$E2E_ROOT/test/e2e/client" --target maven
    ;;
esac
case "$suite" in
  all | deb-apk)
    e2e_pull_image "$E2E_DEBIAN_IMAGE"
    e2e_pull_image "$E2E_ALPINE_IMAGE"
    ;;
esac
case "$suite" in
  all | rpm-pacman)
    e2e_pull_image "$E2E_FEDORA_IMAGE"
    e2e_pull_image "$E2E_ARCH_IMAGE"
    ;;
esac
case "$suite" in
  all | git-oci-flatpak)
    e2e_pull_image "$E2E_CRANE_IMAGE"
    e2e_build_image "$E2E_FLATPAK_CLIENT_IMAGE" "$E2E_ROOT/test/e2e/client/Containerfile" "$E2E_ROOT/test/e2e/client" --target flatpak
    ;;
esac

e2e_init_lifecycle
e2e_start_fixture
e2e_start_proxy

if [[ $suite == all || $suite == registries ]]; then
  source "$E2E_ROOT/test/e2e/suites/registries.sh"
  run_registries_suite
fi
if [[ $suite == all || $suite == deb-apk ]]; then
  source "$E2E_ROOT/test/e2e/suites/deb-apk.sh"
  run_deb_apk_suite
fi
if [[ $suite == all || $suite == rpm-pacman ]]; then
  source "$E2E_ROOT/test/e2e/suites/rpm-pacman.sh"
  run_rpm_pacman_suite
fi
if [[ $suite == all || $suite == git-oci-flatpak ]]; then
  source "$E2E_ROOT/test/e2e/suites/git-oci-flatpak.sh"
  run_git_oci_flatpak_suite
fi

printf '\nAll %s end-to-end cases passed.\n' "$suite"
