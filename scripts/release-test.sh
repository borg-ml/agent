#!/usr/bin/env bash
set -euo pipefail

test_root="$(mktemp -d "${TMPDIR:-/tmp}/borg-release-tests.XXXXXX")"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
release_script="$script_dir/release.sh"
repo_root="$(cd -- "$script_dir/.." && pwd)"

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

# Keep the tag workflow aligned with the self-contained Borg archive. Fixture
# copies used below do not contain workflows, so only enforce this contract when
# running from the real checkout.
if [[ -f "$repo_root/.github/workflows/release.yml" ]]; then
  release_workflow="$repo_root/.github/workflows/release.yml"
  if grep -Eq -- 'cargo test.*-p borg --lib' "$release_workflow"; then
    fail "release workflow still invokes --lib for the binary-only borg package"
  fi
  grep -Fq 'providers/claude' "$release_workflow" ||
    fail "release archive does not package the native Claude payload"
  grep -Fq 'docs/blu-extensions.md' "$release_workflow" ||
    fail "release archive does not package the Blu guide"
  grep -Fq 'configs/extension.example.toml' "$release_workflow" ||
    fail "release archive does not package the Blu manifest example"
  if grep -Eq 'providers/claude-sdk|BORG_CLAUDE_NATIVE|borg-claude-sdk' "$release_workflow"; then
    fail "release workflow still contains the removed Claude fallback"
  fi
fi

# Keep the manually dispatchable platform matrix honest even before a hosted
# run is available. This guards the six requested native targets and the
# binary-only test invocation against accidental workflow drift.
if [[ -f "$repo_root/.github/workflows/platform-ci.yml" ]]; then
  platform_workflow="$repo_root/.github/workflows/platform-ci.yml"
  for target in \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu \
    x86_64-apple-darwin \
    aarch64-apple-darwin \
    x86_64-pc-windows-msvc \
    aarch64-pc-windows-msvc
  do
    grep -Fq "target: $target" "$platform_workflow" ||
      fail "platform workflow is missing native target $target"
  done
  grep -Fq 'cargo test --locked --target ${{ matrix.target }} -p borg --no-fail-fast' \
    "$platform_workflow" ||
    fail "platform workflow is missing the binary-only native test matrix"
  grep -Fq 'providers/claude' "$platform_workflow" ||
    fail "platform archive does not package the native Claude payload"
  grep -Fq 'docs/blu-extensions.md' "$platform_workflow" ||
    fail "platform archive does not package the Blu guide"
  grep -Fq 'configs/extension.example.toml' "$platform_workflow" ||
    fail "platform archive does not package the Blu manifest example"
  if grep -Eq 'providers/claude-sdk|BORG_CLAUDE_NATIVE|borg-claude-sdk' "$platform_workflow"; then
    fail "platform workflow still contains the removed Claude fallback"
  fi
fi

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
  local version="${2:-1.2.3}"
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
    echo "version = \"$version\""
  } >"$fixture/Cargo.toml"

  {
    echo 'version = 4'
    for package in borg borg-provider borg-remote; do
      echo
      echo '[[package]]'
      echo "name = \"$package\""
      echo "version = \"$version\""
    done
  } >"$fixture/Cargo.lock"

  git -C "$fixture" add Cargo.toml Cargo.lock
  git -C "$fixture" commit --quiet -m "Release Borg CLI $version"
  git -C "$fixture" tag -a "v$version" -m "Borg CLI $version"

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
assert_equal "0.2.0" "$("$release_script" --next-minor 0.1.44)" \
  "minor version calculation"
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

interrupted_fixture="$(make_fixture interrupted)"
sed -i 's/1\.2\.3/1.2.4/g' \
  "$interrupted_fixture/Cargo.toml" "$interrupted_fixture/Cargo.lock"
git -C "$interrupted_fixture" add Cargo.toml Cargo.lock
git -C "$interrupted_fixture" commit --quiet -m "Bump workspace version to 1.2.4"
git -C "$interrupted_fixture" push --quiet origin main
interrupted_head="$(git -C "$interrupted_fixture" rev-parse HEAD)"
run_release "$interrupted_fixture"
assert_equal "$interrupted_head" "$(git -C "$interrupted_fixture" rev-parse HEAD)" \
  "interrupted release HEAD"
assert_equal "1.2.4" "$(fixture_version "$interrupted_fixture")" \
  "interrupted release version"
assert_equal "$interrupted_head" \
  "$(git -C "$interrupted_fixture" rev-parse 'refs/tags/v1.2.4^{commit}')" \
  "interrupted release tag"

prebumped_fixture="$(make_fixture prebumped 0.1.43)"
sed -i 's/0\.1\.43/0.1.44/g' \
  "$prebumped_fixture/Cargo.toml" "$prebumped_fixture/Cargo.lock"
echo "coalesced release change" >"$prebumped_fixture/status.txt"
git -C "$prebumped_fixture" add Cargo.toml Cargo.lock status.txt
git -C "$prebumped_fixture" commit --quiet \
  -m "Preserve status tracking and bump version to 0.1.44"
git -C "$prebumped_fixture" push --quiet origin main
prebumped_head="$(git -C "$prebumped_fixture" rev-parse HEAD)"
run_release "$prebumped_fixture"
assert_equal "$prebumped_head" "$(git -C "$prebumped_fixture" rev-parse HEAD)" \
  "pre-bumped release HEAD"
assert_equal "$prebumped_head" \
  "$(git -C "$prebumped_fixture" rev-parse 'refs/tags/v0.1.44^{commit}')" \
  "pre-bumped release tag"

minor_fixture="$(make_fixture minor 0.1.44)"
run_release "$minor_fixture" --minor
assert_equal "0.2.0" "$(fixture_version "$minor_fixture")" \
  "minor release version"
assert_equal "Release Borg CLI 0.2.0" \
  "$(git -C "$minor_fixture" log -1 --format=%s)" \
  "minor release commit"
git -C "$minor_fixture" rev-parse --verify 'refs/tags/v0.2.0^{commit}' \
  >/dev/null || fail "minor release tag is missing"

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
