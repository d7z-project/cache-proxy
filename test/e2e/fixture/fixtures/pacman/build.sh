#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  fixture_dir="$out/$state/pacman"
  version="$major.0.0-1"
  package_dir="/work/$state/pkg"
  mkdir -p "$package_dir/usr/share/e2e-pacman" "$fixture_dir"
  printf 'pkgname = e2e-pacman\npkgbase = e2e-pacman\npkgver = %s\npkgdesc = cache-proxy E2E fixture\nurl = https://example.invalid\nbuilddate = 1704067200\npackager = cache-proxy e2e\nsize = 20\narch = any\nlicense = MIT\n' "$version" >"$package_dir/.PKGINFO"
  printf 'cache-proxy-e2e-%s\n' "$state" >"$package_dir/usr/share/e2e-pacman/payload.txt"
  (cd "$package_dir" && bsdtar --uid 0 --gid 0 -cf - .PKGINFO usr | zstd -q -o "$fixture_dir/e2e-pacman-$version-any.pkg.tar.zst")
  (cd "$fixture_dir" && repo-add e2e.db.tar.gz "e2e-pacman-$version-any.pkg.tar.zst")
done
