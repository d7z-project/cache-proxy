#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
export SOURCE_DATE_EPOCH=1704067200
mkdir -p "$out"/{file,npm,go/example.com/e2e/@v,maven/com/example/e2e-maven/1.0.0,cargo/api/v1/crates/e2e-crate/1.0.0,pypi/files,oci/v2/e2e/image/{manifests,blobs},git}

printf 'cache-proxy-e2e-payload\n' >"$out/file/payload.txt"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/npm/package"
cat >"$work/npm/package/package.json" <<'EOF'
{"name":"e2e-pkg","version":"1.0.0","main":"index.js"}
EOF
printf 'module.exports = "cache-proxy-e2e";\n' >"$work/npm/package/index.js"
tar -C "$work/npm" -czf "$out/npm/e2e-pkg-1.0.0.tgz" package
mkdir -p "$out/npm/e2e-pkg/-"
cp "$out/npm/e2e-pkg-1.0.0.tgz" "$out/npm/e2e-pkg/-/e2e-pkg-1.0.0.tgz"

printf 'module example.com/e2e\n\ngo 1.22\n' >"$out/go/example.com/e2e/@v/v1.0.0.mod"
printf '{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}\n' >"$out/go/example.com/e2e/@v/v1.0.0.info"
printf 'v1.0.0\n' >"$out/go/example.com/e2e/@v/list"
mkdir -p "$work/go/example.com/e2e@v1.0.0"
cp "$out/go/example.com/e2e/@v/v1.0.0.mod" "$work/go/example.com/e2e@v1.0.0/go.mod"
cat >"$work/go/example.com/e2e@v1.0.0/e2e.go" <<'EOF'
package e2e

const Value = "cache-proxy-e2e"
EOF
(cd "$work/go" && zip -X -q -r "$out/go/example.com/e2e/@v/v1.0.0.zip" example.com/e2e@v1.0.0)

maven="$out/maven/com/example/e2e-maven/1.0.0"
cat >"$maven/e2e-maven-1.0.0.pom" <<'EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>com.example</groupId><artifactId>e2e-maven</artifactId><version>1.0.0</version></project>
EOF
mkdir -p "$work/maven/META-INF"
printf 'Manifest-Version: 1.0\n' >"$work/maven/META-INF/MANIFEST.MF"
(cd "$work/maven" && zip -X -q -r "$maven/e2e-maven-1.0.0.jar" META-INF)
for file in "$maven"/*.{pom,jar}; do
  sha1sum "$file" | awk '{print $1}' >"$file.sha1"
  sha256sum "$file" | awk '{print $1}' >"$file.sha256"
done

mkdir -p "$work/cargo/e2e-crate-1.0.0/src"
cat >"$work/cargo/e2e-crate-1.0.0/Cargo.toml" <<'EOF'
[package]
name = "e2e-crate"
version = "1.0.0"
edition = "2021"
license = "MIT"
EOF
printf 'pub const VALUE: &str = "cache-proxy-e2e";\n' >"$work/cargo/e2e-crate-1.0.0/src/lib.rs"
tar -C "$work/cargo" -czf "$out/cargo/api/v1/crates/e2e-crate/1.0.0/download" e2e-crate-1.0.0

wheel="$work/wheel"
mkdir -p "$wheel/e2e_pkg" "$wheel/e2e_pkg-1.0.0.dist-info"
printf '__version__ = "1.0.0"\n' >"$wheel/e2e_pkg/__init__.py"
cat >"$wheel/e2e_pkg-1.0.0.dist-info/METADATA" <<'EOF'
Metadata-Version: 2.1
Name: e2e-pkg
Version: 1.0.0
Summary: cache-proxy end-to-end fixture
EOF
cat >"$wheel/e2e_pkg-1.0.0.dist-info/WHEEL" <<'EOF'
Wheel-Version: 1.0
Generator: cache-proxy-e2e
Root-Is-Purelib: true
Tag: py3-none-any
EOF
cat >"$wheel/e2e_pkg-1.0.0.dist-info/RECORD" <<'EOF'
e2e_pkg/__init__.py,,
e2e_pkg-1.0.0.dist-info/METADATA,,
e2e_pkg-1.0.0.dist-info/WHEEL,,
e2e_pkg-1.0.0.dist-info/RECORD,,
EOF
(cd "$wheel" && zip -X -q -r "$out/pypi/files/e2e_pkg-1.0.0-py3-none-any.whl" .)

mkdir -p "$work/oci/layer"
printf 'cache-proxy-e2e\n' >"$work/oci/layer/payload.txt"
tar -C "$work/oci/layer" -cf "$work/oci/layer.tar" .
gzip -n -c "$work/oci/layer.tar" >"$work/oci/layer.tar.gz"
layer_digest=$(sha256sum "$work/oci/layer.tar.gz" | awk '{print $1}')
layer_size=$(wc -c <"$work/oci/layer.tar.gz" | tr -d ' ')
diff_id=$(sha256sum "$work/oci/layer.tar" | awk '{print $1}')
cat >"$work/oci/config.json" <<EOF
{"architecture":"amd64","os":"linux","created":"2024-01-01T00:00:00Z","config":{},"rootfs":{"type":"layers","diff_ids":["sha256:$diff_id"]},"history":[{"created":"2024-01-01T00:00:00Z","created_by":"cache-proxy e2e"}]}
EOF
config_digest=$(sha256sum "$work/oci/config.json" | awk '{print $1}')
config_size=$(wc -c <"$work/oci/config.json" | tr -d ' ')
cat >"$out/oci/v2/e2e/image/manifests/1.0.0" <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:$config_digest","size":$config_size},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"sha256:$layer_digest","size":$layer_size}]}
EOF
cp "$work/oci/config.json" "$out/oci/v2/e2e/image/blobs/sha256:$config_digest"
cp "$work/oci/layer.tar.gz" "$out/oci/v2/e2e/image/blobs/sha256:$layer_digest"

git init -q "$work/git-work"
git -C "$work/git-work" config user.name 'cache-proxy e2e'
git -C "$work/git-work" config user.email 'e2e@example.invalid'
printf 'cache-proxy-e2e\n' >"$work/git-work/README.md"
git -C "$work/git-work" add README.md
GIT_AUTHOR_DATE=@1704067200 GIT_COMMITTER_DATE=@1704067200 git -C "$work/git-work" commit -q -m fixture
git clone -q --bare "$work/git-work" "$out/git/repo.git"
git -C "$out/git/repo.git" update-server-info
touch "$out/git/repo.git/git-daemon-export-ok"
