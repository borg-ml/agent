use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use super::stream_result::{forward_progress, parse_structured_response, truncate};
use crate::bounded_io::read_open_file_bytes_with_limit;
use crate::provider::{
    ProviderAttemptTrace, ProviderCallError, ProviderCallResult, ProviderInvocation,
    ProviderProgress, ProviderProgressStream, provider_call_timeout,
};
use crate::runtime::{ProviderCallUsage, elapsed_millis_u64};
use crate::subprocess::{self, CommandOutcome};

const CODEX_EXEC_OUTPUT_MAX_BYTES: u64 = 128 * 1024 * 1024;

#[allow(clippy::result_large_err)]
pub(super) async fn codex_exec_structured_call(
    prompt: &str,
    schema: &Value,
    model: Option<String>,
    effort: Option<String>,
    system_prompt: &'static str,
    progress: Option<UnboundedSender<ProviderProgress>>,
) -> std::result::Result<ProviderCallResult, ProviderCallError> {
    let prompt = format_codex_exec_prompt(system_prompt, prompt, Some(schema));
    let schema = schema.clone();
    tokio::task::spawn_blocking(move || {
        codex_exec_call_blocking(prompt, Some(schema), model, effort, progress)
    })
    .await
    .map_err(|error| {
        ProviderCallError::new(
            format!("codex exec task failed: {error}"),
            empty_codex_exec_trace(),
        )
    })?
}

#[allow(clippy::result_large_err)]
pub(super) async fn codex_exec_freeform_call(
    prompt: &str,
    model: Option<String>,
    effort: Option<String>,
    system_prompt: &'static str,
    progress: Option<UnboundedSender<ProviderProgress>>,
) -> std::result::Result<ProviderCallResult, ProviderCallError> {
    let prompt = format_codex_exec_prompt(system_prompt, prompt, None);
    tokio::task::spawn_blocking(move || {
        codex_exec_call_blocking(prompt, None, model, effort, progress)
    })
    .await
    .map_err(|error| {
        ProviderCallError::new(
            format!("codex exec task failed: {error}"),
            empty_codex_exec_trace(),
        )
    })?
}

fn format_codex_exec_prompt(system_prompt: &str, prompt: &str, schema: Option<&Value>) -> String {
    let mut out = String::new();
    if !system_prompt.trim().is_empty() {
        out.push_str(system_prompt.trim());
        out.push_str("\n\n");
    }
    out.push_str(prompt);
    if schema.is_some() {
        out.push_str("\n\nReturn only JSON matching the supplied output schema.");
    }
    out
}

fn empty_codex_exec_trace() -> ProviderAttemptTrace {
    ProviderAttemptTrace {
        invocation: ProviderInvocation {
            provider_label: "codex-exec".to_string(),
            executable: "codex".to_string(),
            args: Vec::new(),
            cwd: None,
            model: None,
            effort: None,
        },
        exit_status: Some(1),
        stdout: String::new(),
        stderr: String::new(),
    }
}

#[allow(clippy::result_large_err)]
fn codex_exec_call_blocking(
    prompt: String,
    schema: Option<Value>,
    model: Option<String>,
    effort: Option<String>,
    progress: Option<UnboundedSender<ProviderProgress>>,
) -> std::result::Result<ProviderCallResult, ProviderCallError> {
    let started_at = Instant::now();
    let tempdir = tempfile::tempdir().map_err(|error| {
        ProviderCallError::new(
            format!("failed to create codex exec tempdir: {error}"),
            empty_codex_exec_trace(),
        )
    })?;
    let output_path = tempdir.path().join("last-message.txt");
    let schema_path = tempdir.path().join("schema.json");
    if let Some(schema_value) = schema.as_ref() {
        let schema_bytes = serde_json::to_vec_pretty(schema_value).map_err(|error| {
            ProviderCallError::new(
                format!("failed to serialize codex exec schema: {error}"),
                empty_codex_exec_trace(),
            )
        })?;
        std::fs::write(&schema_path, schema_bytes).map_err(|error| {
            ProviderCallError::new(
                format!("failed to write codex exec schema: {error}"),
                empty_codex_exec_trace(),
            )
        })?;
    }

    let mut args = vec![
        "exec".to_string(),
        "--dangerously-bypass-approvals-and-sandbox".to_string(),
        "--skip-git-repo-check".to_string(),
        "--output-last-message".to_string(),
        output_path.display().to_string(),
        "--color".to_string(),
        "never".to_string(),
        "-c".to_string(),
        "web_search=\"disabled\"".to_string(),
    ];
    if let Some(model_value) = model.as_ref() {
        args.push("-m".to_string());
        args.push(model_value.clone());
    }
    if let Some(effort_value) = effort.as_ref() {
        args.push("-c".to_string());
        args.push(format!("model_reasoning_effort=\"{effort_value}\""));
    }
    if schema.is_some() {
        args.push("--output-schema".to_string());
        args.push(schema_path.display().to_string());
    }
    args.push("-".to_string());

    let output = run_codex_exec_process(&args, prompt.as_bytes()).map_err(|error| {
        ProviderCallError::new(
            format!("failed to run codex exec: {error}"),
            empty_codex_exec_trace(),
        )
    })?;
    let stdout = output.stdout.clone();
    let stderr = output.stderr.clone();
    forward_progress(&progress, ProviderProgressStream::Stdout, stdout.as_bytes());
    forward_progress(&progress, ProviderProgressStream::Stderr, stderr.as_bytes());
    let trace = ProviderAttemptTrace {
        invocation: ProviderInvocation {
            provider_label: "codex-exec".to_string(),
            executable: "codex".to_string(),
            args,
            cwd: std::env::current_dir()
                .ok()
                .map(|path| path.display().to_string()),
            model: model.clone(),
            effort: effort.clone(),
        },
        exit_status: if output.timed_out {
            None
        } else {
            output.status_code
        },
        stdout: stdout.clone(),
        stderr: stderr.clone(),
    };
    if output.timed_out {
        return Err(ProviderCallError::new(
            format!(
                "codex exec timed out after {:.1}s",
                output.elapsed.as_secs_f64()
            ),
            trace,
        ));
    }
    if !output.success {
        return Err(ProviderCallError::new(
            format!("codex exec failed: {}", truncate(&stderr, 1000)),
            trace,
        ));
    }
    let text = read_codex_exec_output_text(&output_path, &stdout).map_err(|error| {
        ProviderCallError::new(
            format!("failed to read codex exec output: {error}"),
            trace.clone(),
        )
    })?;
    let value = if schema.is_some() {
        parse_structured_response(&text).map_err(|error| {
            ProviderCallError::new(
                format!(
                    "codex exec returned invalid JSON despite schema enforcement: {error} (text: {preview})",
                    preview = truncate(&text, 500)
                ),
                trace.clone(),
            )
        })?
    } else {
        Value::String(text.clone())
    };
    Ok(ProviderCallResult {
        value: value.clone(),
        raw_response: value,
        usage: ProviderCallUsage {
            duration_ms: elapsed_millis_u64(started_at),
            ..ProviderCallUsage::default()
        },
        trace,
        session_id: None,
    })
}

fn run_codex_exec_process(args: &[String], prompt: &[u8]) -> Result<CommandOutcome> {
    run_codex_exec_process_with_timeout("codex", args, prompt, provider_call_timeout())
}

fn run_codex_exec_process_with_timeout(
    program: &str,
    args: &[String],
    prompt: &[u8],
    timeout: Option<Duration>,
) -> Result<CommandOutcome> {
    subprocess::run_with_optional_timeout(
        program,
        args.iter().map(String::as_str),
        None,
        Some(prompt),
        timeout,
    )
}

fn read_codex_exec_output_text(output_path: &Path, stdout: &str) -> Result<String> {
    let file = match std::fs::File::open(output_path) {
        Ok(file) => file,
        Err(_) => return Ok(stdout.to_string()),
    };
    let bytes = read_open_file_bytes_with_limit(
        output_path,
        "codex exec output",
        file,
        CODEX_EXEC_OUTPUT_MAX_BYTES,
    )?;
    Ok(String::from_utf8(bytes).unwrap_or_else(|_| stdout.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_exec_process_marks_timeout() {
        let args = vec!["30".to_string()];
        let output = run_codex_exec_process_with_timeout(
            "sleep",
            &args,
            b"",
            Some(Duration::from_millis(100)),
        )
        .expect("sleep should spawn");

        assert!(output.timed_out);
        assert!(!output.success);
    }

    #[test]
    fn codex_exec_process_without_timeout_uses_bounded_runner() {
        let args = vec!["ok".to_string()];
        let output =
            run_codex_exec_process_with_timeout("printf", &args, b"", None).expect("printf");

        assert!(!output.timed_out);
        assert!(output.success);
        assert_eq!(output.stdout, "ok");
    }

    #[test]
    fn codex_exec_output_reader_falls_back_when_file_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = read_codex_exec_output_text(&dir.path().join("missing.txt"), "stdout fallback")
            .expect("missing file should fall back");

        assert_eq!(text, "stdout fallback");
    }

    #[test]
    fn codex_exec_output_reader_rejects_oversized_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("last-message.txt");
        let file = std::fs::File::create(&path).expect("create sparse output");
        file.set_len(CODEX_EXEC_OUTPUT_MAX_BYTES + 1)
            .expect("set sparse output length");

        let error = read_codex_exec_output_text(&path, "stdout fallback")
            .expect_err("oversized file should fail")
            .to_string();

        assert!(
            error.contains("codex exec output")
                && error.contains("exceeds")
                && error.contains("byte limit"),
            "unexpected error: {error}"
        );
    }
}
