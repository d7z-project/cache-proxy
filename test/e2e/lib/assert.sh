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

e2e_fixture_header() {
  local method=$1 path=$2 header=$3
  e2e_run_client_shell "${E2E_RUN_ID}-fixture-header-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error --get \
      --data-urlencode "method=$1" --data-urlencode "path=$2" --data-urlencode "name=$3" \
      "$4/__e2e/header"
  ' "$method" "$path" "$header" "$E2E_FIXTURE_URL"
}

e2e_reset_fixture_counts() {
  e2e_run_client_shell "${E2E_RUN_ID}-reset-$RANDOM" "$E2E_TOOLS_IMAGE" \
    'curl --fail --silent --show-error -X POST "$1/__e2e/reset" >/dev/null' "$E2E_FIXTURE_URL"
}

e2e_set_fixture_fault() {
  local path=$1 status=$2
  e2e_run_client_shell "${E2E_RUN_ID}-fault-$status-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error -X POST --get \
      --data-urlencode "path=$1" --data-urlencode "status=$2" \
      "$3/__e2e/fault" >/dev/null
  ' "$path" "$status" "$E2E_FIXTURE_URL"
}

e2e_clear_fixture_fault() {
  local path=$1
  e2e_run_client_shell "${E2E_RUN_ID}-fault-clear-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error -X DELETE --get \
      --data-urlencode "path=$1" "$2/__e2e/fault" >/dev/null
  ' "$path" "$E2E_FIXTURE_URL"
}

e2e_assert_bypass_status() {
  local name=$1 url=$2 expected_status=$3
  e2e_run_client_shell "${E2E_RUN_ID}-${name}-bypass-$RANDOM" "$E2E_TOOLS_IMAGE" '
    headers=/tmp/headers
    status=$(curl --silent --show-error --dump-header "$headers" --output /dev/null --write-out "%{http_code}" "$1")
    if [ "$status" != "$2" ] || ! grep -Eiq "^X-Cache:[[:space:]]*BYPASS" "$headers"; then
      printf "unexpected bypass response: status=%s, expected=%s\n" "$status" "$2" >&2
      cat "$headers" >&2
      exit 1
    fi
    if grep -Eiq "^Retry-After:" "$headers"; then
      printf "bypass response included Retry-After\n" >&2
      cat "$headers" >&2
      exit 1
    fi
  ' "$url" "$expected_status"
}

e2e_set_fixture_state() {
  local state=$1
  e2e_run_client_shell "${E2E_RUN_ID}-state-$state-$RANDOM" "$E2E_TOOLS_IMAGE" \
    'curl --fail --silent --show-error -X POST "$1/__e2e/state?value=$2" >/dev/null' "$E2E_FIXTURE_URL" "$state"
}

e2e_reset_fixture() {
  e2e_set_fixture_state initial
  e2e_reset_fixture_counts
}

e2e_wait_body() {
  local url=$1 expected=$2
  e2e_run_client_shell "${E2E_RUN_ID}-wait-body-$RANDOM" "$E2E_TOOLS_IMAGE" '
    url=$1
    expected=$2
    i=0
    while [ "$i" -lt 60 ]; do
      if actual=$(curl --fail --silent --show-error --max-time 5 -H "Cache-Control: no-cache" "$url") &&
         [ "$actual" = "$expected" ]; then
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    printf "timed out waiting for expected content at %s\n" "$url" >&2
    exit 1
  ' "$url" "$expected"
}

e2e_wait_contains() {
  local url=$1 expected=$2
  e2e_run_client_shell "${E2E_RUN_ID}-wait-contains-$RANDOM" "$E2E_TOOLS_IMAGE" '
    url=$1
    expected=$2
    i=0
    while [ "$i" -lt 60 ]; do
      if curl --fail --silent --show-error --max-time 5 -H "Cache-Control: no-cache" "$url" | grep -Fq "$expected"; then
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    printf "timed out waiting for %s at %s\n" "$expected" "$url" >&2
    exit 1
  ' "$url" "$expected"
}

e2e_response_header() {
  local url=$1 header=$2 value
  value=$(e2e_run_client_shell "${E2E_RUN_ID}-header-$RANDOM" "$E2E_TOOLS_IMAGE" '
    curl --fail --silent --show-error --dump-header - --output /dev/null "$1" |
      awk -v name="$2" "BEGIN { IGNORECASE=1 } \$1 == name \
        { sub(/^[^:]*:[[:space:]]*/, \"\"); sub(/\\r$/, \"\"); print; exit }"
  ' "$url" "$header:")
  [[ -n $value ]] || e2e_fail "$header was absent at $url"
  printf '%s\n' "$value"
}

e2e_wait_header_changed() {
  local url=$1 header=$2 previous=$3
  e2e_run_client_shell "${E2E_RUN_ID}-wait-header-$RANDOM" "$E2E_TOOLS_IMAGE" '
    url=$1
    header=$2
    previous=$3
    headers=/tmp/response-headers
    i=0
    while [ "$i" -lt 60 ]; do
      if curl --fail --silent --show-error --max-time 5 -H "Cache-Control: no-cache" \
          --dump-header "$headers" --output /dev/null "$url"; then
        current=$(awk -v name="$header:" "BEGIN { IGNORECASE=1 } \$1 == name \
          { sub(/^[^:]*:[[:space:]]*/, \"\"); sub(/\\r$/, \"\"); print; exit }" "$headers")
        cache=$(awk "BEGIN { IGNORECASE=1 } \$1 == \"X-Cache:\" \
          { sub(/^[^:]*:[[:space:]]*/, \"\"); sub(/\\r$/, \"\"); print; exit }" "$headers")
      else
        current=
        cache=
      fi
      if [ -n "$current" ] && [ "$current" != "$previous" ] && [ "$cache" = HIT ]; then
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    printf "timed out waiting for %s to change at %s\n" "$header" "$url" >&2
    exit 1
  ' "$url" "$header" "$previous"
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

e2e_assert_transparent_paths() {
  local mode=$1 proxy_path=$2 upstream_path=$3 cache_unknown=$4 before after expected
  before=$(e2e_fixture_count GET "$upstream_path/__e2e_path__/asset.css")
  e2e_client "$mode" transparent-paths "$E2E_TOOLS_IMAGE" '
    proxy_url=$1
    proxy_path=$2
    upstream_path=$3
    headers=/tmp/root-headers
    status=$(curl --silent --show-error --dump-header "$headers" --output /dev/null \
      --write-out "%{http_code}" "$proxy_url$proxy_path?view=root")
    test "$status" = 308
    tr -d "\r" <"$headers" | grep -Fqi "Location: $proxy_path/?view=root"
    test "$(curl --fail --silent --show-error "$proxy_url$proxy_path/?view=root")" = "$upstream_path/?view=root"
    test "$(curl --fail --silent --show-error "$proxy_url$proxy_path/__e2e_path__/?view=directory")" = \
      "$upstream_path/__e2e_path__/?view=directory"
    test "$(curl --fail --silent --show-error "$proxy_url$proxy_path/__e2e_path__/asset.css?theme=dark")" = \
      "$upstream_path/__e2e_path__/asset.css?theme=dark"
    test "$(curl --fail --silent --show-error "$proxy_url$proxy_path/__e2e_path__/asset.css?theme=dark")" = \
      "$upstream_path/__e2e_path__/asset.css?theme=dark"
  ' "$E2E_PROXY_URL" "$proxy_path" "$upstream_path"
  after=$(e2e_fixture_count GET "$upstream_path/__e2e_path__/asset.css")
  expected=2
  if [[ $cache_unknown == cache ]]; then
    expected=1
  fi
  e2e_assert_eq "$((before + expected))" "$after" "$mode unknown resource transfer count"
}

e2e_assert_strict_path() {
  local mode=$1 base_url=$2 proxy_path=$3 upstream_path=$4 before after
  before=$(e2e_fixture_count GET "$upstream_path/__e2e_path__/asset.css")
  e2e_client "$mode" strict-path "$E2E_TOOLS_IMAGE" '
    status=$(curl --silent --show-error --output /dev/null --write-out "%{http_code}" \
      "$1$2/__e2e_path__/asset.css?theme=dark")
    test "$status" = 404
  ' "$base_url" "$proxy_path"
  after=$(e2e_fixture_count GET "$upstream_path/__e2e_path__/asset.css")
  e2e_assert_eq "$before" "$after" "$mode strict path reached the fixture"
}
