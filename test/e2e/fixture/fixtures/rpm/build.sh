#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
mkdir -p /root/rpmbuild/{BUILD,BUILDROOT,RPMS,SOURCES,SRPMS}
for state in initial updated; do
  major=1
  [[ $state == updated ]] && major=2
  version="$major.0.0"
  fixture_dir="$out/$state/rpm"
  mkdir -p "$fixture_dir"
  rpmbuild -bb --define "fixture_version $version" --define "fixture_state $state" /root/rpmbuild/SPECS/e2e.spec
  find /root/rpmbuild/RPMS -name "e2e-rpm-$version-1.noarch.rpm" -exec cp {} "$fixture_dir/" \;
  createrepo_c --no-database "$fixture_dir"
  printf 'future metadata for %s\n' "$state" >"$fixture_dir/repodata/future.bin"
  future_checksum=$(sha256sum "$fixture_dir/repodata/future.bin" | awk '{print $1}')
  future_size=$(wc -c <"$fixture_dir/repodata/future.bin")
  sed -i "/<\/repomd>/i\  <data type=\"future-extension\"><checksum type=\"sha256\">$future_checksum</checksum><location href=\"repodata/future.bin\"/><size>$future_size</size></data>" "$fixture_dir/repodata/repomd.xml"
done
