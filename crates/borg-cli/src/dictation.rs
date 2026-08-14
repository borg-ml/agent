use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

const DEFAULT_LOCAL_DICTATION_BASE_URL: &str = "http://127.0.0.1:5092";
const DEFAULT_DICTATION_MODEL: &str = "whisper-1";
const DICTATION_TIMEOUT: Duration = Duration::from_secs(120);
const RECORDER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_AUDIO_BYTES: u64 = 128 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct LocalDictationConfig {
    base_url: String,
    model: String,
    api_key: Option<String>,
    record_command: Option<String>,
}

impl LocalDictationConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            base_url: env_value("BORG_CLI_DICTATION_BASE_URL")
                .or_else(|| env_value("BORG_DICTATION_BASE_URL"))
                .unwrap_or_else(|| DEFAULT_LOCAL_DICTATION_BASE_URL.to_string())
                .trim_end_matches('/')
                .to_string(),
            model: env_value("BORG_CLI_DICTATION_MODEL")
                .or_else(|| env_value("BORG_DICTATION_MODEL"))
                .unwrap_or_else(|| DEFAULT_DICTATION_MODEL.to_string()),
            api_key: env_value("BORG_CLI_DICTATION_API_KEY")
                .or_else(|| env_value("BORG_DICTATION_API_KEY")),
            record_command: env_value("BORG_CLI_DICTATION_RECORD_COMMAND"),
        }
    }
}

pub(crate) struct LocalDictationRecorder {
    child: Child,
    audio_path: TempPath,
}

impl LocalDictationRecorder {
    pub(crate) fn start(config: &LocalDictationConfig) -> Result<Self> {
        let audio = tempfile::Builder::new()
            .prefix("borg-dictation-")
            .suffix(".wav")
            .tempfile()
            .context("failed to create temporary dictation audio")?;
        let audio_path = audio.into_temp_path();
        let mut command = recorder_command(config, audio_path.as_ref())?;
        let child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context(
                "failed to start local microphone recorder; install ffmpeg or set \
                 BORG_CLI_DICTATION_RECORD_COMMAND with an {output} placeholder",
            )?;
        Ok(Self { child, audio_path })
    }

    pub(crate) async fn finish_and_transcribe(
        mut self,
        config: LocalDictationConfig,
    ) -> Result<String> {
        if let Some(mut stdin) = self.child.stdin.take() {
            stdin.write_all(b"q\n").await.ok();
            stdin.shutdown().await.ok();
        }
        let status = match tokio::time::timeout(RECORDER_SHUTDOWN_TIMEOUT, self.child.wait()).await
        {
            Ok(status) => status.context("failed waiting for microphone recorder")?,
            Err(_) => {
                self.child.kill().await.ok();
                self.child
                    .wait()
                    .await
                    .context("failed to stop microphone recorder")?
            }
        };
        if !status.success() {
            bail!("microphone recording failed with {status}");
        }
        transcribe(&config, self.audio_path.as_ref()).await
    }
}

fn recorder_command(config: &LocalDictationConfig, output: &std::path::Path) -> Result<Command> {
    if let Some(template) = config.record_command.as_deref() {
        let mut parts = shlex::split(template)
            .context("BORG_CLI_DICTATION_RECORD_COMMAND contains invalid shell-style quoting")?;
        anyhow::ensure!(
            parts.iter().any(|part| part.contains("{output}")),
            "BORG_CLI_DICTATION_RECORD_COMMAND must contain an {{output}} placeholder"
        );
        for part in &mut parts {
            *part = part.replace("{output}", &output.to_string_lossy());
        }
        let program = parts
            .first()
            .context("BORG_CLI_DICTATION_RECORD_COMMAND cannot be empty")?
            .clone();
        let mut command = Command::new(program);
        command.args(&parts[1..]);
        return Ok(command);
    }

    let mut command = Command::new("ffmpeg");
    command.args(["-hide_banner", "-loglevel", "error", "-y"]);
    #[cfg(target_os = "linux")]
    command.args(["-f", "pulse", "-i", "default"]);
    #[cfg(target_os = "macos")]
    command.args(["-f", "avfoundation", "-i", ":0"]);
    #[cfg(target_os = "windows")]
    command.args(["-f", "dshow", "-i", "audio=default"]);
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    bail!(
        "automatic microphone capture is not supported on this platform; set \
         BORG_CLI_DICTATION_RECORD_COMMAND with an {{output}} placeholder"
    );
    command.args(["-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le"]);
    command.arg(output);
    Ok(command)
}

async fn transcribe(config: &LocalDictationConfig, path: &std::path::Path) -> Result<String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .context("failed to inspect recorded dictation audio")?;
    anyhow::ensure!(metadata.len() > 44, "dictation recording is empty");
    anyhow::ensure!(
        metadata.len() <= MAX_AUDIO_BYTES,
        "dictation recording exceeds {} MiB",
        MAX_AUDIO_BYTES / (1024 * 1024)
    );
    let bytes = tokio::fs::read(path)
        .await
        .context("failed to read recorded dictation audio")?;
    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name("dictation.wav")
        .mime_str("audio/wav")
        .context("failed to prepare dictation audio")?;
    let form = reqwest::multipart::Form::new()
        .part("file", part)
        .text("model", config.model.clone())
        .text("response_format", "json");
    let client = reqwest::Client::builder()
        .timeout(DICTATION_TIMEOUT)
        .build()
        .context("failed to create local dictation client")?;
    let mut request = client
        .post(format!("{}/v1/audio/transcriptions", config.base_url))
        .multipart(form);
    if let Some(api_key) = config.api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.with_context(|| {
        format!(
            "local dictation model is unavailable at {}; start the Borg Parakeet service or set BORG_CLI_DICTATION_BASE_URL",
            config.base_url
        )
    })?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .context("failed to read local dictation response")?;
    anyhow::ensure!(
        body.len() <= MAX_RESPONSE_BYTES,
        "local dictation response is too large"
    );
    let body = String::from_utf8_lossy(&body);
    anyhow::ensure!(
        status.is_success(),
        "local dictation model returned {status}: {}",
        body.trim()
    );
    let text = transcription_text(&body).unwrap_or_else(|| body.trim().to_string());
    anyhow::ensure!(!text.is_empty(), "local dictation model returned no text");
    Ok(text)
}

fn transcription_text(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body).ok().and_then(|value| {
        value
            .get("text")
            .or_else(|| value.get("transcription"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    })
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::transcription_text;

    #[test]
    fn transcription_response_accepts_supported_local_shapes() {
        assert_eq!(
            transcription_text(r#"{"text":"  hello borg  "}"#).as_deref(),
            Some("hello borg")
        );
        assert_eq!(
            transcription_text(r#"{"transcription":"hello"}"#).as_deref(),
            Some("hello")
        );
        assert_eq!(transcription_text(r#"{"text":""}"#), None);
    }
}
