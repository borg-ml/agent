pub(crate) fn nonempty_var(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => {
            let value = value.trim().to_string();
            (!value.is_empty()).then_some(value)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::warn!(variable = name, "environment value is not valid UTF-8");
            None
        }
    }
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static tokio::sync::Mutex<()> {
    // Tests that mutate process-wide environment variables serialize here, so
    // a scratch BORG_HOME in one module cannot move another's key store. It is
    // an async mutex because the scratch environment must stay in place across
    // awaits; synchronous tests take it with `blocking_lock`.
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &LOCK
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EnvGuard {
        name: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var(name).ok();
            unsafe {
                std::env::set_var(name, value);
            }
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = self.previous.as_ref() {
                    std::env::set_var(self.name, previous);
                } else {
                    std::env::remove_var(self.name);
                }
            }
        }
    }

    #[test]
    fn nonempty_var_trims_and_discards_empty_values() {
        let _guard = EnvGuard::set("BORG_TEST_NONEMPTY_VAR", "  value  ");
        assert_eq!(
            nonempty_var("BORG_TEST_NONEMPTY_VAR").as_deref(),
            Some("value")
        );

        let _guard = EnvGuard::set("BORG_TEST_EMPTY_VAR", "   ");
        assert_eq!(nonempty_var("BORG_TEST_EMPTY_VAR"), None);
    }
}
