pub(crate) fn nonempty_var(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => {
            let value = value.trim().to_string();
            (!value.is_empty()).then_some(value)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{name} contains invalid unicode");
        }
    }
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
