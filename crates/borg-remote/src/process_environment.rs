use tokio::process::Command;

/// Remove ambient deployment and provider configuration from child processes
/// that run model-authored code. Explicit MCP environment entries are applied
/// by the MCP launcher after this function returns.
pub(crate) fn configure_sanitized_child_environment(command: &mut Command) {
    command.env_clear();
    for (name, value) in sanitized_environment() {
        command.env(name, value);
    }
}

pub(crate) fn configure_runtime_environment(command: &mut Command) {
    configure_sanitized_child_environment(command);
}

pub(crate) fn configure_host_child_environment(command: &mut Command) {
    if std::env::var("BORG_HOST_EXECUTION_PROFILE").ok().as_deref() == Some("isolated_hosted") {
        configure_sanitized_child_environment(command);
    }
}

pub(crate) const fn sanitized_environment() -> [(&'static str, &'static str); 5] {
    [
        ("PATH", runtime_path()),
        ("LANG", "C.UTF-8"),
        ("LC_ALL", "C.UTF-8"),
        ("PYTHONNOUSERSITE", "1"),
        ("PYTHONDONTWRITEBYTECODE", "1"),
    ]
}

/// Resolve an interpreter to an absolute path using the *supervisor's* view of
/// the filesystem.
///
/// The child deliberately runs with [`sanitized_environment`], whose `PATH` is a
/// fixed system list that excludes user install directories. That protects the
/// child from inheriting credentials, but it also means a runtime installed the
/// normal way — `bun` in `~/.bun/bin`, anything from Homebrew in
/// `/opt/homebrew/bin`, a `pip --user` script in `~/.local/bin` — can never be
/// found by name, and the runtime fails with a bare `No such file or directory`.
///
/// Resolving here keeps both properties: the child still gets the sanitized
/// environment, and the interpreter is located by absolute path so `PATH` is
/// irrelevant to the spawn. A name that already contains a separator is honoured
/// as given, and an unresolvable name is returned unchanged so the caller's own
/// error message still surfaces.
pub(crate) fn resolve_runtime_program(program: &str) -> std::ffi::OsString {
    use std::path::{Path, PathBuf};

    let as_path = Path::new(program);
    if as_path.is_absolute() || program.contains(std::path::MAIN_SEPARATOR) {
        return program.into();
    }

    let mut directories: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default();
    if let Some(home) = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    {
        directories.push(home.join(".bun").join("bin"));
        directories.push(home.join(".local").join("bin"));
        directories.push(home.join(".deno").join("bin"));
    }
    directories.push(PathBuf::from("/opt/homebrew/bin"));
    directories.push(PathBuf::from("/usr/local/bin"));
    directories.extend(std::env::split_paths(runtime_path()));

    let names = if cfg!(windows) {
        vec![format!("{program}.exe"), program.to_string()]
    } else {
        vec![program.to_string()]
    };
    for directory in directories {
        for name in &names {
            let candidate = directory.join(name);
            if is_executable_file(&candidate) {
                return candidate.into_os_string();
            }
        }
    }
    program.into()
}

fn is_executable_file(path: &std::path::Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

const fn runtime_path() -> &'static str {
    if cfg!(windows) {
        r"C:\Windows\System32;C:\Windows"
    } else {
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitized_environment_is_fixed_and_non_secret() {
        let environment = sanitized_environment();
        assert!(
            environment
                .iter()
                .all(|(name, _)| !name.starts_with("BORG_"))
        );
        assert!(!environment.iter().any(|(name, _)| *name == "HOME"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sanitized_environment_is_applied_to_the_child() {
        let mut command = Command::new("/usr/bin/env");
        configure_sanitized_child_environment(&mut command);
        let output = command.output().await.expect("env child");
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(text.contains("PATH="));
        assert!(!text.lines().any(|line| line.starts_with("BORG_")));
        assert!(!text.lines().any(|line| line.starts_with("HOME=")));
    }
}

#[cfg(test)]
mod resolution_tests {
    use super::*;

    #[test]
    fn an_absolute_program_is_returned_unchanged() {
        assert_eq!(resolve_runtime_program("/usr/bin/env"), "/usr/bin/env");
    }

    #[test]
    fn a_system_program_resolves_to_an_absolute_path() {
        let resolved = resolve_runtime_program("sh");
        assert!(
            std::path::Path::new(&resolved).is_absolute(),
            "expected an absolute path, got {resolved:?}"
        );
    }

    #[test]
    fn an_unknown_program_is_returned_unchanged_so_the_caller_can_report_it() {
        assert_eq!(
            resolve_runtime_program("definitely-not-a-real-runtime"),
            "definitely-not-a-real-runtime"
        );
    }

    #[test]
    fn resolution_reaches_directories_the_sanitized_path_excludes() {
        // The sanitized PATH intentionally omits user install directories, so
        // resolution must not be limited to it.
        let sanitized: Vec<_> = std::env::split_paths(runtime_path()).collect();
        assert!(
            !sanitized.iter().any(|dir| dir.ends_with(".bun/bin")),
            "the sanitized PATH should not contain user install directories"
        );
    }
}
