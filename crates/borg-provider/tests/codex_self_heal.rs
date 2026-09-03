//! End-to-end proof that Borg can install Codex from nothing.
//!
//! Ignored by default: it downloads ~110 MiB from upstream. Run explicitly with
//!   cargo test -p borg-provider --all-features --test codex_self_heal -- --ignored
//!
//! The test installs into a scratch `CODEX_HOME`/`HOME` so it never touches the
//! developer's real Codex installation.

use std::path::PathBuf;

use borg_provider::Runtime;
use borg_provider::codex_install;

/// Point every path the installer uses at a temporary directory, and remove any
/// working `codex` from `PATH` so resolution genuinely starts from nothing.
fn isolate(home: &std::path::Path) {
    unsafe {
        std::env::set_var("HOME", home);
        std::env::set_var("CODEX_HOME", home.join(".codex"));
        // Keep only directories that cannot contain a usable codex.
        std::env::set_var("PATH", "/usr/bin:/bin");
        std::env::remove_var("BORG_CODEX_BIN");
        std::env::remove_var("BORG_AUTO_INSTALL");
        // Tests share one process, so a leaked override from an earlier test
        // would silently push a later one onto the fallback path.
        std::env::remove_var("BORG_CODEX_RELEASE_API_URL");
    }
}

#[tokio::test]
#[ignore = "downloads the real Codex package from upstream"]
async fn installs_codex_from_a_clean_machine() {
    let home = tempfile::tempdir().expect("temporary home");
    isolate(home.path());

    let (path, healed) = codex_install::ensure(Runtime::Codex)
        .await
        .expect("Borg should install Codex from nothing");

    // The version must come back from the direct, checksum-verified package
    // path — not the script fallback, which reports "(unknown version)".
    match &healed {
        borg_provider::Healed::Installed { version } => assert!(
            version.chars().next().is_some_and(|c| c.is_ascii_digit()),
            "expected a verified direct install, got version {version:?}"
        ),
        other => panic!("expected a fresh install, got {other:?}"),
    }
    assert!(path.is_file(), "installed codex should exist at {path:?}");

    // It must actually run, not merely exist.
    let output = tokio::process::Command::new(&path)
        .arg("--version")
        .output()
        .await
        .expect("installed codex should start");
    assert!(
        output.status.success(),
        "installed codex should report its version"
    );

    // And it must be published through the layout Codex's own updater expects,
    // so `codex update` keeps working after Borg installed it.
    let standalone: PathBuf = home.path().join(".codex/packages/standalone");
    assert!(
        standalone
            .join("current")
            .join("bin")
            .join("codex")
            .is_file(),
        "the `current` symlink should resolve to the installed binary"
    );
    assert!(
        home.path().join(".local/bin/codex").is_file(),
        "codex should be published into ~/.local/bin"
    );
}

/// A second call must be a no-op: healing is idempotent, and re-running must not
/// re-download.
#[tokio::test]
#[ignore = "downloads the real Codex package from upstream"]
async fn healing_is_idempotent() {
    let home = tempfile::tempdir().expect("temporary home");
    isolate(home.path());

    codex_install::ensure(Runtime::Codex)
        .await
        .expect("first install");
    let (_, healed) = codex_install::ensure(Runtime::Codex)
        .await
        .expect("second resolve");
    assert_eq!(
        healed,
        borg_provider::Healed::AlreadyWorking,
        "a second call must reuse the existing install"
    );
}

/// Deleting the published symlinks but leaving the release directory intact is
/// the XProtect aftermath. It must repair offline, without re-downloading.
#[tokio::test]
#[ignore = "downloads the real Codex package from upstream"]
async fn broken_symlinks_are_repaired_without_reinstalling() {
    let home = tempfile::tempdir().expect("temporary home");
    isolate(home.path());

    codex_install::ensure(Runtime::Codex)
        .await
        .expect("first install");

    // Simulate the quarantine: the published entry points vanish, the release
    // directory survives.
    std::fs::remove_file(home.path().join(".codex/packages/standalone/current")).ok();
    std::fs::remove_file(home.path().join(".local/bin/codex")).ok();

    // Forbid network installs so a re-download would fail the test rather than
    // silently paper over a missing repair.
    unsafe { std::env::set_var("BORG_CODEX_RELEASE_API_URL", "http://127.0.0.1:1/none") }

    let (path, healed) = codex_install::ensure(Runtime::Codex)
        .await
        .expect("Borg should repair the layout offline");
    assert_eq!(healed, borg_provider::Healed::Relinked);
    assert!(path.is_file());
}
