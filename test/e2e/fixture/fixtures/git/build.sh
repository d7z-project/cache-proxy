#!/usr/bin/env bash
set -Eeuo pipefail

out=${1:?output directory required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

git init -q "$work/repository"
git -C "$work/repository" config user.name 'cache-proxy e2e'
git -C "$work/repository" config user.email 'e2e@example.invalid'
day=0
for state in initial updated; do
  day=$((day + 1))
  printf 'cache-proxy-e2e-%s\n' "$state" >"$work/repository/README.md"
  git -C "$work/repository" add README.md
  timestamp=$((1703973600 + day * 86400))
  GIT_AUTHOR_DATE="@$timestamp" GIT_COMMITTER_DATE="@$timestamp" \
    git -C "$work/repository" commit -q -m "fixture $state"
  mkdir -p "$out/$state/git"
  git clone -q --bare "$work/repository" "$out/$state/git/repo.git"
  git -C "$out/$state/git/repo.git" update-server-info
  touch "$out/$state/git/repo.git/git-daemon-export-ok"
done
