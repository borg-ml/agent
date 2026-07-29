#!/usr/bin/env bash
set -euo pipefail

test_root="$(mktemp -d "${TMPDIR:-/tmp}/borg-release-tests.XXXXXX")"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
release_script="$script_dir/release.sh"

cleanup() {
  local status="$?"
  trap - EXIT
  rm -rf -- "$test_root"
  exit "$status"
}
trap cleanup EXIT

fail() {
  echo "release test: $*" >&2
  exit 1
}

assert_equal() {
  local expected="$1"
  local actual="$2"
  local label="$3"
  [[ "$actual" == "$expected" ]] ||
    fail "$label: expected '$expected', got '$actual'"
}

fixture_version() {
  awk '
    /^\[workspace\.package\]$/ {
      inside_workspace_package = 1
      next
    }
    /^\[/ {
      inside_workspace_package = 0
    }
    inside_workspace_package && /^version = "/ {
      version = $0
      sub(/^[^"]*"/, "", version)
      sub(/".*$/, "", version)
      print version
    }
  ' "$1/Cargo.toml"
}

make_fixture() {
  local name="$1"
  local fixture="$test_root/$name"
  local origin="$test_root/$name-origin.git"

  mkdir -p "$fixture/scripts"
  git -C "$fixture" init --quiet --initial-branch=main
  git -C "$fixture" config user.name "Borg release test"
  git -C "$fixture" config user.email "release-test@borg.invalid"

  {
    echo '[workspace]'
    echo 'members = []'
    echo
    echo '[workspace.package]'
    echo 'version = "1.2.3"'
  } >"$fixture/Cargo.toml"

  {
    echo 'version = 4'
    for package in borg borg-provider borg-remote; do
      echo
      echo '[[package]]'
      echo "name = \"$package\""
      echo 'version = "1.2.3"'
    done
  } >"$fixture/Cargo.lock"

  git -C "$fixture" add Cargo.toml Cargo.lock
  git -C "$fixture" commit --quiet -m "Release Borg CLI 1.2.3"
  git -C "$fixture" tag -a v1.2.3 -m "Borg CLI 1.2.3"

  cp "$release_script" "$fixture/scripts/release.sh"
  chmod +x "$fixture/scripts/release.sh"
  git -C "$fixture" add scripts/release.sh
  git -C "$fixture" commit --quiet -m "Add release tooling"

  git init --quiet --bare "$origin"
  git -C "$fixture" remote add origin "$origin"
  git -C "$fixture" push --quiet origin main --tags

  printf '%s\n' "$fixture"
}

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
{
  echo '#!/usr/bin/env bash'
  echo 'set -euo pipefail'
  echo 'printf "%s\n" "$*" >>"${FAKE_CARGO_LOG:?}"'
  echo 'case "${1:-}" in'
  echo '  check)'
  cat <<'FAKE_CARGO'
    version="$(
      awk '
        /^\[workspace\.package\]$/ {
          inside_workspace_package = 1
          next
        }
        /^\[/ {
          inside_workspace_package = 0
        }
        inside_workspace_package && /^version = "/ {
          version = $0
          sub(/^[^"]*"/, "", version)
          sub(/".*$/, "", version)
          print version
        }
      ' Cargo.toml
    )"
    lock_tmp="${TMPDIR:-/tmp}/borg-release-test-lock.$$"
    awk -v target="$version" '
      /^name = "borg(-provider|-remote)?"$/ {
        update_version = 1
        print
        next
      }
      update_version && /^version = "/ {
        print "version = \"" target "\""
        update_version = 0
        next
      }
      {
        print
      }
    ' Cargo.lock >"$lock_tmp"
    mv "$lock_tmp" Cargo.lock
FAKE_CARGO
  echo '    ;;'
  echo '  fmt)'
  echo '    ;;'
  echo '  test)'
  echo '    [[ "${FAKE_CARGO_FAIL:-}" != "test" ]] || exit 42'
  echo '    ;;'
  echo '  *)'
  echo '    echo "unexpected fake cargo invocation: $*" >&2'
  echo '    exit 64'
  echo '    ;;'
  echo 'esac'
} >"$fake_bin/cargo"
chmod +x "$fake_bin/cargo"

run_release() {
  local fixture="$1"
  shift
  local cargo_log="$test_root/$(basename "$fixture")-fake-cargo.log"
  : >"$cargo_log"
  (
    cd "$fixture"
    PATH="$fake_bin:$PATH" FAKE_CARGO_LOG="$cargo_log" \
      ./scripts/release.sh "$@"
  )
}

assert_equal "1.2.4" "$("$release_script" --next-version 1.2.3)" \
  "default patch calculation"
if "$release_script" --next-version 01.2.3 >/dev/null 2>&1; then
  fail "invalid SemVer was accepted"
fi

success_fixture="$(make_fixture success)"
run_release "$success_fixture" --verify-tag v1.2.3
if run_release "$success_fixture" --verify-tag v1.2.4 >/dev/null 2>&1; then
  fail "mismatched release tag was accepted"
fi

before_check="$(git -C "$success_fixture" rev-parse HEAD)"
run_release "$success_fixture" --check
assert_equal "$before_check" "$(git -C "$success_fixture" rev-parse HEAD)" \
  "release check mutated HEAD"
[[ -z "$(git -C "$success_fixture" status --porcelain)" ]] ||
  fail "release check mutated the fixture"

run_release "$success_fixture"
assert_equal "1.2.4" "$(fixture_version "$success_fixture")" \
  "default release version"
assert_equal "Release Borg CLI 1.2.4" \
  "$(git -C "$success_fixture" log -1 --format=%s)" \
  "default release commit"
assert_equal "Borg CLI 1.2.4" \
  "$(git -C "$success_fixture" for-each-ref \
    --format='%(contents:subject)' refs/tags/v1.2.4)" \
  "default release tag"
assert_equal "$(git -C "$success_fixture" rev-parse HEAD)" \
  "$(git --git-dir="$test_root/success-origin.git" rev-parse refs/heads/main)" \
  "remote main"
assert_equal "$(git -C "$success_fixture" rev-parse HEAD)" \
  "$(git --git-dir="$test_root/success-origin.git" rev-parse \
    'refs/tags/v1.2.4^{commit}')" \
  "remote release tag"
grep -Fxq 'check --workspace --all-targets' "$test_root/success-fake-cargo.log" ||
  fail "release did not run the workspace check"
grep -Fxq 'fmt --all -- --check' "$test_root/success-fake-cargo.log" ||
  fail "release did not run the formatting check"
grep -Fxq 'test --workspace --locked' "$test_root/success-fake-cargo.log" ||
  fail "release did not run workspace tests"

run_release "$success_fixture" v2.0.0
assert_equal "2.0.0" "$(fixture_version "$success_fixture")" \
  "explicit release version"
git -C "$success_fixture" rev-parse --verify 'refs/tags/v2.0.0^{commit}' \
  >/dev/null || fail "explicit release tag is missing"

rollback_fixture="$(make_fixture rollback)"
rollback_manifest="$(sha256sum "$rollback_fixture/Cargo.toml")"
rollback_lock="$(sha256sum "$rollback_fixture/Cargo.lock")"
rollback_head="$(git -C "$rollback_fixture" rev-parse HEAD)"
rollback_log="$test_root/rollback-fake-cargo.log"
: >"$rollback_log"
if (
  cd "$rollback_fixture"
  PATH="$fake_bin:$PATH" FAKE_CARGO_LOG="$rollback_log" FAKE_CARGO_FAIL=test \
    ./scripts/release.sh
) >/dev/null 2>&1; then
  fail "a failing release unexpectedly succeeded"
fi
assert_equal "$rollback_manifest" "$(sha256sum "$rollback_fixture/Cargo.toml")" \
  "Cargo.toml rollback"
assert_equal "$rollback_lock" "$(sha256sum "$rollback_fixture/Cargo.lock")" \
  "Cargo.lock rollback"
assert_equal "$rollback_head" "$(git -C "$rollback_fixture" rev-parse HEAD)" \
  "failed release HEAD"
[[ -z "$(git -C "$rollback_fixture" status --porcelain)" ]] ||
  fail "failed release left a dirty fixture"
if git -C "$rollback_fixture" rev-parse --verify 'refs/tags/v1.2.4^{commit}' \
  >/dev/null 2>&1; then
  fail "failed release created a tag"
fi

dirty_fixture="$(make_fixture dirty)"
touch "$dirty_fixture/uncommitted"
if run_release "$dirty_fixture" >/dev/null 2>&1; then
  fail "release accepted a dirty worktree"
fi
if git --git-dir="$test_root/dirty-origin.git" rev-parse \
  'refs/tags/v1.2.4^{commit}' >/dev/null 2>&1; then
  fail "dirty-worktree release published a tag"
fi

echo "Release tooling regression tests passed."
