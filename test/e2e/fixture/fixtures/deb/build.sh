#!/usr/bin/env bash
set -Eeuo pipefail

# Builds both standard and flat layouts for the Debian mode.
out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  version="$major.0.0+e2e1"
  root="$out/$state/deb"
  package_work="$work/pkg-$state"
  mkdir -p "$root/pool/main/e/e2e-deb" "$root/dists/stable/main/binary-amd64" "$root/flat"
  mkdir -p "$package_work/DEBIAN" "$package_work/usr/share/e2e-deb"
  cat >"$package_work/DEBIAN/control" <<EOF
Package: e2e-deb
Version: $version
Section: misc
Priority: optional
Architecture: all
Maintainer: cache-proxy e2e <e2e@example.invalid>
Description: cache-proxy end-to-end fixture
EOF
  printf 'cache-proxy-e2e-%s\n' "$state" >"$package_work/usr/share/e2e-deb/payload.txt"
  package="$root/pool/main/e/e2e-deb/e2e-deb_${version}_all.deb"
  dpkg-deb --root-owner-group --build "$package_work" "$package" >/dev/null
  size=$(wc -c <"$package" | tr -d ' ')
  sha256=$(sha256sum "$package" | awk '{print $1}')
  cat >"$root/dists/stable/main/binary-amd64/Packages" <<EOF
Package: e2e-deb
Version: $version
Architecture: all
Maintainer: cache-proxy e2e <e2e@example.invalid>
Filename: pool/main/e/e2e-deb/e2e-deb_${version}_all.deb
Size: $size
SHA256: $sha256
Description: cache-proxy end-to-end fixture

EOF
  gzip -n -c "$root/dists/stable/main/binary-amd64/Packages" >"$root/dists/stable/main/binary-amd64/Packages.gz"

  release="$root/dists/stable/Release"
  cat >"$release" <<EOF
Origin: cache-proxy-e2e
Label: cache-proxy-e2e
Suite: stable
Codename: stable
Date: Tue, 0${major} Jan 2024 00:00:00 UTC
Architectures: amd64 all
Components: main
Description: cache-proxy end-to-end repository $state
Acquire-By-Hash: yes
SHA256:
EOF
  for relative in main/binary-amd64/Packages main/binary-amd64/Packages.gz; do
    file="$root/dists/stable/$relative"
    printf ' %s %16s %s\n' "$(sha256sum "$file" | awk '{print $1}')" "$(wc -c <"$file" | tr -d ' ')" "$relative" >>"$release"
  done
	rm "$root/dists/stable/main/binary-amd64/Packages"

  cp "$package" "$root/flat/e2e-deb_${version}_all.deb"
  cat >"$root/flat/Packages" <<EOF
Package: e2e-deb
Version: $version
Architecture: all
Maintainer: cache-proxy e2e <e2e@example.invalid>
Filename: e2e-deb_${version}_all.deb
Size: $size
SHA256: $sha256
Description: cache-proxy end-to-end flat fixture

EOF
  gzip -n -c "$root/flat/Packages" >"$root/flat/Packages.gz"
  cat >"$root/flat/Release" <<EOF
Origin: cache-proxy-e2e-flat
Label: cache-proxy-e2e-flat
Suite: ./
Codename: flat
Date: Tue, 0${major} Jan 2024 00:00:00 UTC
Architectures: amd64 all
Description: cache-proxy end-to-end flat repository $state
Acquire-By-Hash: yes
SHA256:
EOF
  for relative in Packages Packages.gz; do
    file="$root/flat/$relative"
    printf ' %s %16s %s\n' "$(sha256sum "$file" | awk '{print $1}')" "$(wc -c <"$file" | tr -d ' ')" "$relative" >>"$root/flat/Release"
  done
	rm "$root/flat/Packages"
done
