//! Redaction of high-confidence secrets from command output before it reaches
//! the model or the durable journal.
//!
//! Shell stdout/stderr is returned verbatim into the model's context and into
//! the session journal (which can be exported or synced). A leaked key there is
//! both an exfiltration risk and a way for a reasoning model to echo a
//! credential back into a later message. This scrubber removes the credential
//! shapes that are unambiguous enough to redact without mangling ordinary
//! output: it never fires on prose, only on the exact token grammars below.
//!
//! It is deliberately conservative. Missing a bespoke internal token is
//! acceptable; corrupting a legitimate build log is not. Callers apply it at
//! the boundary where a full, line-complete output string is assembled, so a
//! pattern is never split across a read boundary.

use std::borrow::Cow;
use std::sync::LazyLock;

use regex::Regex;

struct Rule {
    kind: &'static str,
    pattern: Regex,
}

/// The capturing group whose span is replaced. When a rule needs to keep
/// surrounding context (for example the `Bearer ` prefix or an `KEY=` name),
/// it captures only the secret in group 1; otherwise the whole match is
/// replaced.
fn rules() -> &'static [Rule] {
    static RULES: LazyLock<Vec<Rule>> = LazyLock::new(|| {
        let rule = |kind, pattern: &str| Rule {
            kind,
            pattern: Regex::new(pattern).expect("secret scrub pattern compiles"),
        };
        vec![
            // PEM private key blocks, including the BEGIN/END fences.
            rule(
                "private-key",
                r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
            ),
            // Anthropic keys are checked before the generic `sk-` rule so the
            // more specific label wins.
            rule("anthropic-key", r"sk-ant-[A-Za-z0-9_-]{20,}"),
            rule("openai-key", r"sk-(?:proj-)?[A-Za-z0-9_-]{20,}"),
            rule("github-token", r"gh[pousr]_[A-Za-z0-9]{36,}"),
            rule("aws-access-key-id", r"AKIA[0-9A-Z]{16}"),
            rule("google-api-key", r"AIza[0-9A-Za-z_-]{35}"),
            rule("slack-token", r"xox[baprs]-[A-Za-z0-9-]{10,}"),
            rule(
                "jwt",
                r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
            ),
            // `Authorization: Bearer <token>` / `Bearer <token>`: keep the
            // scheme, drop the credential (group 1).
            rule("bearer-token", r"[Bb]earer\s+([A-Za-z0-9._~+/-]{16,}=*)"),
        ]
    });
    &RULES
}

/// Replace high-confidence secrets in `text` with `[redacted:<kind>]`.
///
/// Returns a borrowed `Cow` unchanged when nothing matched, so the common
/// no-secret path allocates nothing.
pub fn scrub_secrets(text: &str) -> Cow<'_, str> {
    let mut out: Option<String> = None;
    for rule in rules() {
        let source: &str = out.as_deref().unwrap_or(text);
        if !rule.pattern.is_match(source) {
            continue;
        }
        let replaced = rule
            .pattern
            .replace_all(source, |caps: &regex::Captures<'_>| {
                // Keep any captured prefix context; redact only the secret. A
                // rule with a group 1 redacts that group in place, preserving
                // the surrounding characters the whole match covered.
                if let Some(secret) = caps.get(1) {
                    let whole = caps.get(0).expect("match zero exists");
                    let prefix = &whole.as_str()[..secret.start() - whole.start()];
                    format!("{prefix}[redacted:{}]", rule.kind)
                } else {
                    format!("[redacted:{}]", rule.kind)
                }
            })
            .into_owned();
        out = Some(replaced);
    }
    match out {
        Some(scrubbed) => Cow::Owned(scrubbed),
        None => Cow::Borrowed(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_ordinary_output_untouched_without_allocating() {
        let text = "Compiling borg-remote v0.1.0\n   Finished in 12.3s\nsk-not (too short)";
        let scrubbed = scrub_secrets(text);
        assert!(matches!(scrubbed, Cow::Borrowed(_)));
        assert_eq!(scrubbed, text);
    }

    #[test]
    fn redacts_provider_keys_by_kind() {
        let anthropic =
            scrub_secrets("export ANTHROPIC_API_KEY=sk-ant-api03-abcdefghijklmnopqrstuvwx");
        assert_eq!(
            anthropic,
            "export ANTHROPIC_API_KEY=[redacted:anthropic-key]"
        );

        let openai = scrub_secrets("key: sk-proj-ABCDEFGHIJKLMNOPQRSTUVWXYZ012345");
        assert_eq!(openai, "key: [redacted:openai-key]");

        let github = scrub_secrets("token ghp_0123456789abcdefghijklmnopqrstuvwxyzAB done");
        assert_eq!(github, "token [redacted:github-token] done");
    }

    #[test]
    fn redacts_aws_google_slack_and_jwt() {
        assert_eq!(
            scrub_secrets("AKIAIOSFODNN7EXAMPLE"),
            "[redacted:aws-access-key-id]"
        );
        assert_eq!(
            scrub_secrets("AIzaSyA1234567890abcdefghijklmnopqrstuv"),
            "[redacted:google-api-key]"
        );
        assert_eq!(
            scrub_secrets("xoxb-1234567890-abcdefABCDEF"),
            "[redacted:slack-token]"
        );
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcDEF123_signature-here";
        assert_eq!(scrub_secrets(jwt), "[redacted:jwt]");
    }

    #[test]
    fn keeps_the_bearer_scheme_and_redacts_only_the_credential() {
        let scrubbed = scrub_secrets("Authorization: Bearer abcdef0123456789ABCDEF==");
        assert_eq!(scrubbed, "Authorization: Bearer [redacted:bearer-token]");
    }

    #[test]
    fn redacts_a_pem_private_key_block() {
        let text = "before\n-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA\nAAAA\n-----END OPENSSH PRIVATE KEY-----\nafter";
        let scrubbed = scrub_secrets(text);
        assert_eq!(scrubbed, "before\n[redacted:private-key]\nafter");
    }

    #[test]
    fn redacts_several_secrets_in_one_pass() {
        let text = "a sk-ant-api03-abcdefghijklmnopqrstuvwx b ghp_0123456789abcdefghijklmnopqrstuvwxyzAB c";
        let scrubbed = scrub_secrets(text);
        assert_eq!(
            scrubbed,
            "a [redacted:anthropic-key] b [redacted:github-token] c"
        );
    }
}
