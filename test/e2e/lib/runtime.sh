#!/usr/bin/env bash

e2e_select_runtime() {
  local requested=${E2E_RUNTIME:-}
  if [[ -n $requested && $requested != docker && $requested != podman ]]; then
    printf 'E2E_RUNTIME must be docker or podman, got %q\n' "$requested" >&2
    return 2
  fi

  if [[ -n $requested ]]; then
    command -v "$requested" >/dev/null 2>&1 || {
      printf '%s is not installed\n' "$requested" >&2
      return 2
    }
    "$requested" info >/dev/null 2>&1 || {
      printf '%s is installed but its engine is unavailable\n' "$requested" >&2
      return 2
    }
    E2E_RUNTIME=$requested
  elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    E2E_RUNTIME=docker
  elif command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    E2E_RUNTIME=podman
  else
    printf 'no usable Docker or Podman engine was found\n' >&2
    return 2
  fi
  export E2E_RUNTIME

  local os
  os=$($E2E_RUNTIME info --format '{{.Host.OS}}' 2>/dev/null || true)
  if [[ -z $os ]]; then
    os=$($E2E_RUNTIME info --format '{{.OSType}}' 2>/dev/null || true)
  fi
  if [[ $os != linux ]]; then
    printf 'host-network E2E tests require a native Linux container engine (reported %q)\n' "$os" >&2
    return 2
  fi
}

e2e_runtime() {
  "$E2E_RUNTIME" "$@"
}

e2e_image_exists() {
  e2e_runtime image inspect "$1" >/dev/null 2>&1
}

e2e_pull_image() {
  local image=$1
  if ! e2e_image_exists "$image"; then
    printf 'Pulling %s\n' "$image"
    e2e_runtime pull "$image"
  fi
}

e2e_build_image() {
  local tag=$1 containerfile=$2 context=$3
  shift 3
  e2e_runtime build --network host --label cache-proxy.e2e=true -t "$tag" -f "$containerfile" "$@" "$context"
}

e2e_run_client() {
  local name=$1 image=$2
  shift 2
  e2e_runtime run --rm --network host \
    --name "$name" \
    --label "$E2E_OWNER_LABEL" \
    "$image" "$@"
}

e2e_run_client_shell() {
  local name=$1 image=$2 script=$3
  shift 3
  e2e_runtime run --rm --network host \
    --name "$name" \
    --label "$E2E_OWNER_LABEL" \
    --entrypoint /bin/sh \
    "$image" -ec "$script" e2e "$@"
}
