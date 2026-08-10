#!/usr/bin/env bash
set -euo pipefail

readonly REPOSITORY_URL="https://github.com/borg-ml/cli"
readonly VERSION_PATTERN='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'

die() {
  echo "release: $*" >&2
  exit 1
}

workspace_version_from_stdin() {
  awk '
    /^\[workspace\.package\][[:space:]]*$/ {
      inside_workspace_package = 1
      next
    }
    /^\[/ {
      inside_workspace_package = 0
    }
    inside_workspace_package &&
      $0 ~ /^[[:space:]]*version[[:space:]]*=[[:space:]]*"[^"]+"/ {
        version = $0
        sub(/^[^"]*"/, "", version)
        sub(/".*$/, "", version)
        print version
        found += 1
    }
    END {
      if (found != 1) {
        exit 1
      }
    }
  '
}

workspace_version() {
  workspace_version_from_stdin <Cargo.toml
}

workspace_version_at() {
  git show "$1:Cargo.toml" | workspace_version_from_stdin
}

validate_version() {
  local version="$1"
  [[ "$version" =~ $VERSION_PATTERN ]]
}

next_patch_version() {
  local version="$1"
  validate_version "$version" || die "workspace version '$version' is not stable SemVer"
  IFS=. read -r major minor patch <<<"$version"
  printf '%d.%d.%d\n' "$((10#$major))" "$((10#$minor))" "$((10#$patch + 1))"
}

next_minor_version() {
  local version="$1"
  local major minor patch
  validate_version "$version" || die "workspace version '$version' is not stable SemVer"
  IFS=. read -r major minor patch <<<"$version"
  printf '%d.%d.0\n' "$((10#$major))" "$((10#$minor + 1))"
}

version_is_greater() {
  local candidate="$1"
  local current="$2"
  local candidate_major candidate_minor candidate_patch
  local current_major current_minor current_patch

  IFS=. read -r candidate_major candidate_minor candidate_patch <<<"$candidate"
  IFS=. read -r current_major current_minor current_patch <<<"$current"

  ((10#$candidate_major > 10#$current_major)) ||
    {
      ((10#$candidate_major == 10#$current_major)) &&
        {
          ((10#$candidate_minor > 10#$current_minor)) ||
            {
              ((10#$candidate_minor == 10#$current_minor)) &&
                ((10#$candidate_patch > 10#$current_patch))
            }
        }
    }
}

replace_workspace_version() {
  local current="$1"
  local target="$2"
  local manifest_tmp
  manifest_tmp="$(mktemp "${TMPDIR:-/tmp}/borg-release-manifest.XXXXXX")"

  if ! awk -v current="$current" -v target="$target" '
    /^\[workspace\.package\][[:space:]]*$/ {
      inside_workspace_package = 1
      print
      next
    }
    /^\[/ {
      inside_workspace_package = 0
    }
    inside_workspace_package &&
      $0 ~ "^[[:space:]]*version[[:space:]]*=[[:space:]]*\"" current "\"[[:space:]]*$" {
        print "version = \"" target "\""
        replaced += 1
        next
    }
    {
      print
    }
    END {
      if (replaced != 1) {
        exit 1
      }
    }
  ' Cargo.toml >"$manifest_tmp"; then
    rm -f "$manifest_tmp"
    die "could not replace workspace version $current in Cargo.toml"
  fi

  mv "$manifest_tmp" Cargo.toml
}

manifest_version_change() {
  local parent="$1"
  local commit="$2"
  local previous="$3"
  local target="$4"
  local changed_file
  local -a changed_files

  mapfile -t changed_files < <(git diff --name-only "$parent" "$commit" -- Cargo.toml Cargo.lock)
  [[ " ${changed_files[*]} " == *" Cargo.toml "* ]] || return 1
  for changed_file in "${changed_files[@]}"; do
    case "$changed_file" in
      Cargo.toml | Cargo.lock) ;;
      *) return 1 ;;
    esac
  done

  git diff --no-ext-diff --unified=0 "$parent" "$commit" -- Cargo.toml Cargo.lock |
    awk -v previous="$previous" -v target="$target" '
      /^diff --git / || /^index / || /^@@ / || /^--- / || /^\+\+\+ / { next }
      /^-[^-]/ {
        if ($0 != "-version = \"" previous "\"") exit 1
        changed = 1
        next
      }
      /^\+[^+]/ {
        if ($0 != "+version = \"" target "\"") exit 1
        changed = 1
        next
      }
      { exit 1 }
      END { if (!changed) exit 1 }
    '
}

prebumped_release_commit() {
  local target="$1"
  local commit
  local parent
  local previous
  local previous_tag

  while IFS= read -r commit; do
    parent="$(git rev-parse --verify "$commit^" 2>/dev/null)" || continue
    previous="$(workspace_version_at "$parent" 2>/dev/null)" || continue
    validate_version "$previous" || continue
    version_is_greater "$target" "$previous" || continue
    [[ "$(workspace_version_at "$commit" 2>/dev/null)" == "$target" ]] || continue

    previous_tag="v$previous"
    git rev-parse --quiet --verify "refs/tags/$previous_tag^{commit}" >/dev/null || continue
    git merge-base --is-ancestor "$previous_tag" "$parent" || continue

    # A release version can be bumped together with the code it releases. The
    # manifest and lockfile must still contain only the expected version
    # transition; non-Cargo changes are allowed and are the reason this is
    # separate from the old version-only release path.
    manifest_version_change "$parent" "$commit" "$previous" "$target" || continue
    [[ -z "$(git diff --name-only "$commit" HEAD -- Cargo.toml Cargo.lock)" ]] || continue
    printf '%s\n' "$commit"
    return 0
  done < <(git log --first-parent --format='%H' -- Cargo.toml Cargo.lock)

  return 1
}

run_release_checks() (
  local test_tmp
  # Keep the sandbox path short: borg-remote exposes Unix sockets below
  # TMPDIR, whose platform limit is shorter than a repository-relative path.
  test_tmp="$(mktemp -d /tmp/borg-release-tests.XXXXXX)"
  trap 'rm -rf -- "$test_tmp"' EXIT
  cargo fmt --all -- --check
  TMPDIR="$test_tmp" cargo test --workspace --locked
  git diff --check
)

mode="release"
release_kind="patch"
if [[ "${1:-}" == "--next-version" ]]; then
  [[ "$#" -eq 2 ]] || die "usage: $0 --next-version CURRENT"
  next_patch_version "$2"
  exit 0
elif [[ "${1:-}" == "--next-minor" ]]; then
  [[ "$#" -eq 2 ]] || die "usage: $0 --next-minor CURRENT"
  next_minor_version "$2"
  exit 0
elif [[ "${1:-}" == "--check" ]]; then
  mode="check"
  shift
elif [[ "${1:-}" == "--minor" ]]; then
  release_kind="minor"
  shift
elif [[ "${1:-}" == "--verify-tag" ]]; then
  mode="verify-tag"
  shift
fi

if [[ "$mode" == "verify-tag" ]]; then
  [[ "$#" -eq 1 ]] || die "usage: $0 --verify-tag TAG"
else
  [[ "$#" -le 1 ]] || die "usage: $0 [--check] [VERSION]"
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

for command in cargo git awk; do
  command -v "$command" >/dev/null 2>&1 || die "required command '$command' was not found"
done

current_version="$(workspace_version)" ||
  die "could not read [workspace.package].version from Cargo.toml"
validate_version "$current_version" ||
  die "workspace version '$current_version' is not stable SemVer"

if [[ "$mode" == "verify-tag" ]]; then
  [[ "$1" == "v$current_version" ]] ||
    die "tag '$1' does not match workspace version v$current_version"
  echo "Release tag $1 matches workspace version $current_version."
  exit 0
fi

requested_version="${1:-}"
requested_version="${requested_version#v}"
if [[ -n "$requested_version" ]]; then
  validate_version "$requested_version" ||
    die "requested version '$requested_version' must be stable SemVer (X.Y.Z)"
fi
remote="${BORG_RELEASE_REMOTE:-origin}"
branch="${BORG_RELEASE_BRANCH:-main}"

[[ -z "$(git status --porcelain --untracked-files=all)" ]] ||
  die "working tree must be clean"
[[ "$(git branch --show-current)" == "$branch" ]] ||
  die "release must run from branch '$branch'"
git remote get-url "$remote" >/dev/null 2>&1 ||
  die "git remote '$remote' does not exist"

echo "Fetching $remote/$branch and release tags..."
git fetch --quiet "$remote" "$branch" --tags

remote_head="$(git rev-parse "refs/remotes/$remote/$branch")"
local_head="$(git rev-parse HEAD)"
[[ "$local_head" == "$remote_head" ]] ||
  die "local $branch must exactly match $remote/$branch before release"

recovered=0
recovery_kind=""
tag="v$current_version"
current_tag_commit=""
if current_tag_commit="$(git rev-parse --verify "refs/tags/$tag^{commit}" 2>/dev/null)"; then
  if ! git ls-remote --quiet --exit-code "$remote" "refs/tags/$tag" >/dev/null 2>&1 &&
    [[ "$current_tag_commit" == "$local_head" ]]; then
    recovered=1
    recovery_kind="pending tag"
    target_version="$current_version"
  else
    if [[ -n "$requested_version" ]]; then
      target_version="$requested_version"
    else
      if [[ "$release_kind" == "minor" ]]; then
        target_version="$(next_minor_version "$current_version")"
      else
        target_version="$(next_patch_version "$current_version")"
      fi
    fi
    version_is_greater "$target_version" "$current_version" ||
      die "target $target_version must be newer than workspace version $current_version"
    tag="v$target_version"
  fi
else
  if prebumped_release_commit "$current_version" >/dev/null 2>&1 &&
    [[ "$release_kind" == "patch" ]] &&
    [[ -z "$requested_version" || "$requested_version" == "$current_version" ]]; then
    recovered=1
    recovery_kind="pre-bumped version"
    target_version="$current_version"
  else
    if [[ -n "$requested_version" ]]; then
      target_version="$requested_version"
    elif [[ "$release_kind" == "minor" ]]; then
      target_version="$(next_minor_version "$current_version")"
    else
      target_version="$(next_patch_version "$current_version")"
    fi
    version_is_greater "$target_version" "$current_version" ||
      die "target $target_version must be newer than workspace version $current_version"
    tag="v$target_version"
  fi
fi

if [[ "$release_kind" == "minor" && -n "$requested_version" ]]; then
  expected_minor="$(next_minor_version "$current_version")"
  [[ "$requested_version" == "$expected_minor" ]] ||
    die "minor release target must be $expected_minor (got $requested_version)"
fi

if [[ "$recovered" -eq 0 ]] && git rev-parse --quiet --verify "refs/tags/$tag^{commit}" >/dev/null; then
  die "tag $tag already exists"
fi

if [[ "$recovered" -eq 1 ]]; then
  echo "Recovered $recovery_kind release: $target_version (version bump already committed)"
else
  echo "Release plan: $current_version -> $target_version"
fi
echo "Targets: Linux, macOS, and Windows on x86-64 and ARM64"

if [[ "$mode" == "check" ]]; then
  run_release_checks
  if [[ "$recovered" -eq 1 ]]; then
    echo "Release checks passed. Run 'just release' to publish v$target_version."
  else
    echo "Release checks passed. Run 'just release${requested_version:+ $requested_version}'."
  fi
  exit 0
fi

if [[ "$recovered" -eq 1 ]]; then
  run_release_checks
  if ! git rev-parse --quiet --verify "refs/tags/$tag^{commit}" >/dev/null; then
    git tag -a "$tag" -m "Borg CLI $target_version"
  fi
  echo "Publishing $tag atomically to $remote..."
  git push --atomic "$remote" "HEAD:refs/heads/$branch" "refs/tags/$tag"
  echo "Release workflow started: $REPOSITORY_URL/actions/workflows/release.yml"
  exit 0
fi

manifest_backup="$(mktemp "${TMPDIR:-/tmp}/borg-release-cargo-toml.XXXXXX")"
lock_backup="$(mktemp "${TMPDIR:-/tmp}/borg-release-cargo-lock.XXXXXX")"
cp Cargo.toml "$manifest_backup"
cp Cargo.lock "$lock_backup"
committed=0

cleanup() {
  local status="$?"
  trap - EXIT
  if [[ "$status" -ne 0 && "$committed" -eq 0 ]]; then
    cp "$manifest_backup" Cargo.toml
    cp "$lock_backup" Cargo.lock
    echo "release: restored Cargo.toml and Cargo.lock after failure" >&2
  elif [[ "$status" -ne 0 ]]; then
    echo "release: the release commit was retained; inspect it before retrying the atomic push" >&2
  fi
  rm -f "$manifest_backup" "$lock_backup"
  exit "$status"
}
trap cleanup EXIT

replace_workspace_version "$current_version" "$target_version"

# Refresh workspace package versions in Cargo.lock before enforcing --locked.
cargo check --workspace --all-targets

mapfile -t changed_files < <(git diff --name-only)
for changed_file in "${changed_files[@]}"; do
  case "$changed_file" in
    Cargo.toml | Cargo.lock) ;;
    *) die "version update unexpectedly changed $changed_file" ;;
  esac
done
[[ " ${changed_files[*]} " == *" Cargo.toml "* ]] ||
  die "Cargo.toml was not updated"
[[ " ${changed_files[*]} " == *" Cargo.lock "* ]] ||
  die "Cargo.lock was not updated"

[[ "$(workspace_version)" == "$target_version" ]] ||
  die "workspace version did not update to $target_version"

run_release_checks

git add Cargo.toml Cargo.lock
git commit -m "Release Borg CLI $target_version"
committed=1
git tag -a "$tag" -m "Borg CLI $target_version"

echo "Publishing $tag atomically to $remote..."
git push --atomic "$remote" "HEAD:refs/heads/$branch" "refs/tags/$tag"

echo "Release workflow started: $REPOSITORY_URL/actions/workflows/release.yml"
