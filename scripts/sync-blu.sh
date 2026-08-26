#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/Cargo.toml"
remote="https://github.com/borg-ml/blu.git"
mode="${1:---update}"

case "$mode" in
  --check | --update) ;;
  *) echo "usage: $0 [--check|--update]" >&2; exit 2 ;;
esac

pinned="$(sed -n 's/^blu-lang = .*rev = "\([0-9a-f]\{40\}\)".*/\1/p' "$manifest")"
test -n "$pinned" || { echo "could not read pinned blu-lang revision" >&2; exit 1; }
latest="$(git ls-remote "$remote" HEAD | awk 'NR == 1 { print $1 }')"
test -n "$latest" || { echo "could not resolve Blu HEAD" >&2; exit 1; }

if [ "$pinned" = "$latest" ]; then
  echo "Blu is current at ${latest:0:12}"
  exit 0
fi

if [ "$mode" = "--check" ]; then
  echo "Blu dependency is stale: pinned ${pinned:0:12}, latest ${latest:0:12}" >&2
  echo "run: just blu-update" >&2
  exit 1
fi

sed -i "s/rev = \"$pinned\"/rev = \"$latest\"/" "$manifest"
(cd "$repo_root" && cargo update -p blu-lang)
echo "Updated Blu from ${pinned:0:12} to ${latest:0:12}"
