#!/usr/bin/env bash
set -Eeuo pipefail

# Builds initial and updated OSTree repositories for the Flatpak mode.
out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  day=1
  [[ $state == updated ]] && day=2
  root="$out/$state/flatpak/repo"
  state_work="$work/$state"
  mkdir -p "$root" "$state_work/runtime/files" "$state_work/runtime/usr" "$state_work/unrelated/files" "$state_work/unrelated/usr" "$state_work/app/export" "$state_work/app/files/bin"

  cat >"$state_work/runtime/metadata" <<'EOF'
[Runtime]
name=org.example.Platform
runtime=org.example.Platform/x86_64/stable
sdk=org.example.Sdk/x86_64/stable
EOF
  cat >"$state_work/app/metadata" <<'EOF'
[Application]
name=org.example.E2E
runtime=org.example.Platform/x86_64/stable
sdk=org.example.Sdk/x86_64/stable
command=e2e
EOF
  cat >"$state_work/unrelated/metadata" <<'EOF'
[Runtime]
name=org.example.Unrelated
runtime=org.example.Unrelated/aarch64/stable
sdk=org.example.Unrelated/aarch64/stable
EOF
  cat >"$state_work/app/files/bin/e2e" <<EOF
#!/bin/sh
echo cache-proxy-e2e-$state
EOF
  chmod 0755 "$state_work/app/files/bin/e2e"

  flatpak build-export --runtime --no-update-summary --timestamp="2024-01-0${day}T00:00:00Z" "$root" "$state_work/runtime" stable
  flatpak build-export --runtime --arch=aarch64 --no-update-summary --timestamp="2024-01-0${day}T00:00:00Z" "$root" "$state_work/unrelated" stable
  flatpak build-export --no-update-summary --timestamp="2024-01-0${day}T00:00:00Z" "$root" "$state_work/app" stable
  flatpak build-update-repo "$root"

  unrelated_summary=
  for summary in "$root"/summaries/*.gz; do
    if gzip -cd "$summary" | grep -a -q 'aarch64'; then
      unrelated_summary="summaries/${summary##*/}"
      break
    fi
  done
  [[ -n $unrelated_summary ]]
  printf '%s\n' "$unrelated_summary" >"$out/$state/flatpak/unrelated-summary-path.txt"
done
