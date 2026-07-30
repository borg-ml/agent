use anyhow::{Context, Result, bail};
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

const CLEAN_BASH_PATH: &str = "/bin/bash";

pub struct CleanShellEnv {
    dir: TempDir,
}

impl CleanShellEnv {
    pub fn new() -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("borg-clean-shell-")
            .tempdir()
            .context("failed to create clean-shell tempdir")?;
        if !Path::new(CLEAN_BASH_PATH).exists() {
            bail!("required bash shell missing at {CLEAN_BASH_PATH}");
        }

        for file_name in [".zshenv", ".zprofile", ".zlogin", ".zshrc"] {
            let path = dir.path().join(file_name);
            fs::write(&path, "")
                .with_context(|| format!("failed to create clean shell stub {}", path.display()))?;
        }

        Ok(Self { dir })
    }

    pub fn apply(&self, command: &mut Command) {
        command
            .env("SHELL", CLEAN_BASH_PATH)
            .env("BASH_ENV", "/dev/null")
            .env("ENV", "")
            .env("ZDOTDIR", self.dir.path());
    }

    pub fn codex_config_args(&self) -> Vec<String> {
        vec![
            "-c".to_string(),
            "allow_login_shell=false".to_string(),
            "-c".to_string(),
            "approval_policy=\"never\"".to_string(),
            "-c".to_string(),
            "sandbox_mode=\"danger-full-access\"".to_string(),
            "-c".to_string(),
            "features.image_generation=false".to_string(),
            "-c".to_string(),
            "shell_environment_policy.experimental_use_profile=false".to_string(),
            "-c".to_string(),
            // The point of this policy is a predictable *shell*, not a
            // stripped environment: `core` keeps only HOME/PATH/USER and the
            // like, so an agent cannot see the credentials and settings the
            // user exported for it. Inherit everything and keep the shell
            // itself clean through the `set` overrides below.
            "shell_environment_policy.inherit=\"all\"".to_string(),
            "-c".to_string(),
            "shell_environment_policy.ignore_default_excludes=true".to_string(),
            "-c".to_string(),
            format!("shell_environment_policy.set.SHELL=\"{CLEAN_BASH_PATH}\""),
            "-c".to_string(),
            "shell_environment_policy.set.BASH_ENV=\"/dev/null\"".to_string(),
            "-c".to_string(),
            "shell_environment_policy.set.ENV=\"\"".to_string(),
            "-c".to_string(),
            format!(
                "shell_environment_policy.set.ZDOTDIR=\"{}\"",
                self.dir.path().display()
            ),
        ]
    }

    #[cfg(test)]
    pub fn shell_root(&self) -> &Path {
        self.dir.path()
    }
}

#[cfg(test)]
mod tests {
    use super::CleanShellEnv;
    use std::fs;

    #[test]
    fn clean_shell_env_sets_bash_and_clean_zsh_stubs() {
        let env = CleanShellEnv::new().expect("clean shell env");
        for file_name in [".zshenv", ".zprofile", ".zlogin", ".zshrc"] {
            let path = env.shell_root().join(file_name);
            assert!(path.exists(), "missing {}", path.display());
            assert_eq!(fs::read_to_string(path).expect("stub"), "");
        }
        let mut command = std::process::Command::new("env");
        env.apply(&mut command);
        let output = command.output().expect("env output");
        let rendered = String::from_utf8(output.stdout).expect("utf8");
        assert!(rendered.contains("SHELL=/bin/bash"));
        assert!(rendered.contains("BASH_ENV=/dev/null"));
        assert!(rendered.contains("ENV="));
        assert!(rendered.contains(&format!("ZDOTDIR={}", env.shell_root().display())));
    }

    #[test]
    fn codex_config_args_force_bash_and_disable_login_shells() {
        let env = CleanShellEnv::new().expect("clean shell env");
        let args = env.codex_config_args();
        assert!(args.contains(&"allow_login_shell=false".to_string()));
        assert!(args.contains(&"approval_policy=\"never\"".to_string()));
        assert!(args.contains(&"sandbox_mode=\"danger-full-access\"".to_string()));
        assert!(args.contains(&"features.image_generation=false".to_string()));
        assert!(
            args.contains(&"shell_environment_policy.experimental_use_profile=false".to_string())
        );
        assert!(args.contains(&"shell_environment_policy.set.SHELL=\"/bin/bash\"".to_string()));
    }

    /// The clean shell must not double as a credential filter: an agent runs
    /// what the user exported for it, and `core` silently hid all of it.
    #[test]
    fn codex_config_args_inherit_the_whole_environment() {
        let env = CleanShellEnv::new().expect("clean shell env");
        let args = env.codex_config_args();
        assert!(args.contains(&"shell_environment_policy.inherit=\"all\"".to_string()));
        assert!(
            args.contains(&"shell_environment_policy.ignore_default_excludes=true".to_string())
        );
        assert!(!args.contains(&"shell_environment_policy.inherit=\"core\"".to_string()));
        // Each config value is preceded by its own `-c`.
        assert_eq!(
            args.iter().filter(|arg| *arg == "-c").count(),
            args.len() / 2
        );
    }
}
