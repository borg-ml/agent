use std::io::Write;

use base64::Engine;

const OSC52_MAX_RAW_BYTES: usize = 100_000;

pub struct ClipboardLease {
    #[cfg(target_os = "linux")]
    _native: Option<arboard::Clipboard>,
}

pub fn copy(text: &str) -> Result<Option<ClipboardLease>, String> {
    if !is_remote_terminal() {
        match native_copy(text) {
            Ok(lease) => return Ok(lease),
            Err(error) => tracing::debug!(%error, "native clipboard copy failed"),
        }
    }
    osc52_copy(text)?;
    Ok(None)
}

#[cfg(not(target_os = "android"))]
fn native_copy(text: &str) -> Result<Option<ClipboardLease>, String> {
    let mut clipboard =
        arboard::Clipboard::new().map_err(|error| format!("clipboard unavailable: {error}"))?;
    clipboard
        .set_text(text)
        .map_err(|error| format!("clipboard write failed: {error}"))?;
    Ok(Some(ClipboardLease {
        #[cfg(target_os = "linux")]
        _native: Some(clipboard),
    }))
}

#[cfg(target_os = "android")]
fn native_copy(_text: &str) -> Result<Option<ClipboardLease>, String> {
    Err("native clipboard is unavailable on Android/Termux".to_string())
}

fn is_remote_terminal() -> bool {
    std::env::var_os("SSH_TTY").is_some() || std::env::var_os("SSH_CONNECTION").is_some()
}

fn osc52_copy(text: &str) -> Result<(), String> {
    let sequence = osc52_sequence(text, std::env::var_os("TMUX").is_some())?;
    #[cfg(unix)]
    if let Ok(mut tty) = std::fs::OpenOptions::new().write(true).open("/dev/tty")
        && tty.write_all(sequence.as_bytes()).is_ok()
    {
        return tty.flush().map_err(|error| error.to_string());
    }
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(sequence.as_bytes())
        .and_then(|()| stdout.flush())
        .map_err(|error| format!("OSC 52 clipboard write failed: {error}"))
}

fn osc52_sequence(text: &str, tmux: bool) -> Result<String, String> {
    if text.len() > OSC52_MAX_RAW_BYTES {
        return Err(format!(
            "copy is too large for OSC 52 ({} bytes; max {OSC52_MAX_RAW_BYTES})",
            text.len()
        ));
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
    if tmux {
        Ok(format!(
            "\u{1b}Ptmux;\u{1b}\u{1b}]52;c;{encoded}\u{7}\u{1b}\\"
        ))
    } else {
        Ok(format!("\u{1b}]52;c;{encoded}\u{7}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn osc52_round_trips_wide_unicode() {
        let text = "borg 👩🏽‍💻 漢字";
        let sequence = osc52_sequence(text, false).unwrap();
        let encoded = sequence
            .strip_prefix("\u{1b}]52;c;")
            .unwrap()
            .strip_suffix('\u{7}')
            .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap();
        assert_eq!(decoded, text.as_bytes());
    }
}
