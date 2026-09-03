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
done
