#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for state in initial updated; do
  release_count=1
  [[ $state == updated ]] && release_count=2
  fixture_dir="$out/$state/maven/com/example/e2e-maven"
  versions=
  for release in $(seq 1 "$release_count"); do
    version="$release.0.0"
    versions="$versions<version>$version</version>"
    release_dir="$fixture_dir/$version"
    archive_dir="$work/$state/release-$release"
    mkdir -p "$release_dir" "$archive_dir/META-INF"
    cat >"$release_dir/e2e-maven-$version.pom" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>com.example</groupId><artifactId>e2e-maven</artifactId><version>$version</version></project>
EOF
    printf 'Manifest-Version: 1.0\n' >"$archive_dir/META-INF/MANIFEST.MF"
    fixture_state=initial
    [[ $release == 2 ]] && fixture_state=updated
    printf 'cache-proxy-e2e-%s\n' "$fixture_state" >"$archive_dir/payload.txt"
    (cd "$archive_dir" && zip -X -q -r "$release_dir/e2e-maven-$version.jar" META-INF payload.txt)
    for file in "$release_dir"/*.{pom,jar}; do
      sha1sum "$file" | awk '{print $1}' >"$file.sha1"
      sha256sum "$file" | awk '{print $1}' >"$file.sha256"
    done
  done
  cat >"$fixture_dir/maven-metadata.xml" <<EOF
<metadata><groupId>com.example</groupId><artifactId>e2e-maven</artifactId><versioning><latest>$release_count.0.0</latest><release>$release_count.0.0</release><versions>$versions</versions><lastUpdated>2024010100000$release_count</lastUpdated></versioning></metadata>
EOF
done
