#!/usr/bin/env bash

e2e_fail() {
  printf 'E2E failure: %s\n' "$*" >&2
  return 1
}

e2e_assert_eq() {
  local expected=$1 actual=$2 message=$3
  [[ $actual == "$expected" ]] || e2e_fail "$message: expected $expected, got $actual"
}

e2e_assert_ne() {
  local unexpected=$1 actual=$2 message=$3
  [[ $actual != "$unexpected" ]] || e2e_fail "$message: unexpectedly got $actual"
}

e2e_wait_http() {
  local name=$1 url=$2
  e2e_run_client_shell "$name" "$E2E_TOOLS_IMAGE" '
    url=$1
    label=$2
    i=0
    while [ "$i" -lt 60 ]; do
      if curl --fail --silent --show-error --max-time 2 "$url" >/dev/null 2>&1; then
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    printf "timed out waiting for %s at %s\n" "$label" "$url" >&2
    exit 1
  ' "$url" "$name"
}

e2e_fixture_count() {
  local method=$1 path=$2
  e2e_run_client_shell "${E2E_RUN_ID}-count-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error --get \
      --data-urlencode "method=$1" --data-urlencode "path=$2" \
      "$3/__e2e/count"
  ' "$method" "$path" "$E2E_FIXTURE_URL"
}

e2e_fixture_mutations() {
  e2e_run_client_shell "${E2E_RUN_ID}-mutations-$RANDOM" "$E2E_TOOLS_IMAGE" \
    'curl --fail --silent --show-error "$1/__e2e/mutations"' "$E2E_FIXTURE_URL"
}

e2e_fixture_prefix_count() {
  local method=$1 prefix=$2
  e2e_run_client_shell "${E2E_RUN_ID}-prefix-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error --get \
      --data-urlencode "method=$1" --data-urlencode "prefix=$2" \
      "$3/__e2e/prefix"
  ' "$method" "$prefix" "$E2E_FIXTURE_URL"
}

e2e_fixture_counts() {
  local method=$1 prefix=$2
  e2e_run_client_shell "${E2E_RUN_ID}-counts-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error --get \
      --data-urlencode "method=$1" --data-urlencode "prefix=$2" \
      "$3/__e2e/counts"
  ' "$method" "$prefix" "$E2E_FIXTURE_URL"
}

e2e_reset_fixture_counts() {
  e2e_run_client_shell "${E2E_RUN_ID}-reset-$RANDOM" "$E2E_TOOLS_IMAGE" \
    'curl --fail --silent --show-error -X POST "$1/__e2e/reset" >/dev/null' "$E2E_FIXTURE_URL"
}

e2e_assert_count_unchanged() {
  local method=$1 path=$2 before=$3 message=$4 after
  after=$(e2e_fixture_count "$method" "$path")
  e2e_assert_eq "$before" "$after" "$message"
}

e2e_wait_for_fixture_count() {
  local method=$1 path=$2 minimum=$3 i count
  for ((i = 0; i < 60; i++)); do
    count=$(e2e_fixture_count "$method" "$path")
    if ((count >= minimum)); then
      printf '%s\n' "$count"
      return 0
    fi
    sleep 1
  done
  e2e_fail "fixture did not observe $method $path at least $minimum time(s)"
}

e2e_wait_cache_hit() {
  local url=$1
  e2e_run_client_shell "${E2E_RUN_ID}-wait-cache-$RANDOM" "$E2E_TOOLS_IMAGE" '
    url=$1
    i=0
    while [ "$i" -lt 60 ]; do
      if curl --silent --show-error --dump-header /tmp/headers --output /dev/null "$url" &&
         grep -Eiq "^X-Cache: (HIT|COALESCED)" /tmp/headers; then
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    printf "timed out waiting for a published cache response at %s\n" "$url" >&2
    exit 1
  ' "$url"
}
