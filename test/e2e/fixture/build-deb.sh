#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$out/pool/main/e/e2e-deb" "$out/dists/stable/main/binary-amd64" "$out/flat"
mkdir -p "$work/pkg/DEBIAN" "$work/pkg/usr/share/e2e-deb"
cat >"$work/pkg/DEBIAN/control" <<'EOF'
Package: e2e-deb
Version: 1.0.0
Section: misc
Priority: optional
Architecture: all
Maintainer: cache-proxy e2e <e2e@example.invalid>
Description: cache-proxy end-to-end fixture
EOF
printf 'cache-proxy-e2e\n' >"$work/pkg/usr/share/e2e-deb/payload.txt"
package="$out/pool/main/e/e2e-deb/e2e-deb_1.0.0_all.deb"
dpkg-deb --root-owner-group --build "$work/pkg" "$package" >/dev/null
size=$(wc -c <"$package" | tr -d ' ')
sha256=$(sha256sum "$package" | awk '{print $1}')
cat >"$out/dists/stable/main/binary-amd64/Packages" <<EOF
Package: e2e-deb
Version: 1.0.0
Architecture: all
Maintainer: cache-proxy e2e <e2e@example.invalid>
Filename: pool/main/e/e2e-deb/e2e-deb_1.0.0_all.deb
Size: $size
SHA256: $sha256
Description: cache-proxy end-to-end fixture

EOF
gzip -n -c "$out/dists/stable/main/binary-amd64/Packages" >"$out/dists/stable/main/binary-amd64/Packages.gz"

release="$out/dists/stable/Release"
cat >"$release" <<'EOF'
Origin: cache-proxy-e2e
Label: cache-proxy-e2e
Suite: stable
Codename: stable
Date: Mon, 01 Jan 2024 00:00:00 UTC
Architectures: amd64 all
Components: main
Description: cache-proxy end-to-end repository
SHA256:
EOF
for path in main/binary-amd64/Packages main/binary-amd64/Packages.gz; do
  file="$out/dists/stable/$path"
  printf ' %s %16s %s\n' "$(sha256sum "$file" | awk '{print $1}')" "$(wc -c <"$file" | tr -d ' ')" "$path" >>"$release"
done

cp "$package" "$out/flat/e2e-deb_1.0.0_all.deb"
cat >"$out/flat/Packages" <<EOF
Package: e2e-deb
Version: 1.0.0
Architecture: all
Maintainer: cache-proxy e2e <e2e@example.invalid>
Filename: e2e-deb_1.0.0_all.deb
Size: $size
SHA256: $sha256
Description: cache-proxy end-to-end flat fixture

EOF
gzip -n -c "$out/flat/Packages" >"$out/flat/Packages.gz"
cat >"$out/flat/Release" <<'EOF'
Origin: cache-proxy-e2e-flat
Label: cache-proxy-e2e-flat
Suite: ./
Codename: flat
Date: Mon, 01 Jan 2024 00:00:00 UTC
Architectures: amd64 all
Description: cache-proxy end-to-end flat repository
SHA256:
EOF
for path in Packages Packages.gz; do
  file="$out/flat/$path"
  printf ' %s %16s %s\n' "$(sha256sum "$file" | awk '{print $1}')" "$(wc -c <"$file" | tr -d ' ')" "$path" >>"$out/flat/Release"
done
