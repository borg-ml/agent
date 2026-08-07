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
