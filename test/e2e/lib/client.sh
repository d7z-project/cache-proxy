#!/usr/bin/env bash

e2e_client() {
  local case_name=$1 phase=$2 image=$3 script=$4
  shift 4
  e2e_run_client_shell "${E2E_RUN_ID}-${case_name}-${phase}" "$image" "$script" "$@"
}
