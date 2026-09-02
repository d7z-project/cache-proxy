#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$out" "$work/runtime/files" "$work/runtime/usr" "$work/app/export" "$work/app/files/bin"

cat >"$work/runtime/metadata" <<'EOF'
[Runtime]
name=org.example.Platform
runtime=org.example.Platform/x86_64/stable
sdk=org.example.Sdk/x86_64/stable
EOF
cat >"$work/app/metadata" <<'EOF'
[Application]
name=org.example.E2E
runtime=org.example.Platform/x86_64/stable
sdk=org.example.Sdk/x86_64/stable
command=e2e
EOF
cat >"$work/app/files/bin/e2e" <<'EOF'
#!/bin/sh
echo cache-proxy-e2e
EOF
chmod 0755 "$work/app/files/bin/e2e"

flatpak build-export --runtime --no-update-summary --timestamp=2024-01-01T00:00:00Z "$out" "$work/runtime" stable
flatpak build-export --no-update-summary --timestamp=2024-01-01T00:00:00Z "$out" "$work/app" stable
flatpak build-update-repo "$out"
