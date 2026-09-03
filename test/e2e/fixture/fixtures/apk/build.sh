#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  version="$major.0.0"
  if [[ $major == 2 ]]; then
    sed -i 's/pkgver=1.0.0/pkgver=2.0.0/' APKBUILD
  fi
  abuild -r
  fixture_dir="$out/$state/apk/v3.20/main/x86_64"
  mkdir -p "$fixture_dir"
  find /home/builder/packages -name "e2e-apk-$version-r0.apk" -exec cp {} "$fixture_dir/" \;
  find /home/builder/packages -name APKINDEX.tar.gz -exec cp {} "$fixture_dir/APKINDEX.tar.gz" \;
done
