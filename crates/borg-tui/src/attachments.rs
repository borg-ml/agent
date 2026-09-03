use std::borrow::Cow;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use base64::Engine;
use image::{DynamicImage, ImageFormat, RgbaImage};
use uuid::Uuid;

const MAX_ATTACHMENT_BYTES: u64 = 50 * 1024 * 1024;
const MAX_INLINE_IMAGE_BYTES: usize = 25 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasteOutcome {
    pub text: String,
    pub attachments: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct AttachmentStore {
    root: PathBuf,
}

impl AttachmentStore {
    pub fn for_session(sessions_dir: &Path, session_id: Uuid) -> Result<Self> {
        let root = sessions_dir.join(format!("{session_id}.attachments"));
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create attachment store {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn stage_path(&self, source: &Path) -> Result<PathBuf> {
        let source = source
            .canonicalize()
            .with_context(|| format!("attachment does not exist: {}", source.display()))?;
        let metadata = fs::metadata(&source)
            .with_context(|| format!("failed to inspect attachment {}", source.display()))?;
        if !metadata.is_file() {
            bail!("attachment must be a regular file: {}", source.display());
        }
        if metadata.len() > MAX_ATTACHMENT_BYTES {
            bail!(
                "attachment is too large ({} bytes; max {MAX_ATTACHMENT_BYTES})",
                metadata.len()
            );
        }
        let original = source
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("attachment");
        let destination = self
            .root
            .join(format!("{}-{}", Uuid::new_v4(), safe_filename(original)));
        fs::copy(&source, &destination).with_context(|| {
            format!(
                "failed to persist attachment {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        Ok(destination)
    }

    pub fn stage_paste(&self, pasted: &str, cwd: &Path) -> Result<PasteOutcome> {
        if let Some((bytes, extension)) = decode_terminal_image(pasted)? {
            return Ok(PasteOutcome {
                text: String::new(),
                attachments: vec![self.write_image_bytes(&bytes, extension)?],
            });
        }

        let mut paths = pasted_paths(pasted, cwd);
        if paths.iter().any(|path| !is_supported_image(path)) {
            paths.clear();
        }
        paths.sort();
        paths.dedup();
        if paths.is_empty() {
            return Ok(PasteOutcome {
                text: normalize_newlines(pasted),
                attachments: Vec::new(),
            });
        }
        let attachments = paths
            .iter()
            .map(|path| self.stage_path(path))
            .collect::<Result<Vec<_>>>()?;
        Ok(PasteOutcome {
            text: String::new(),
            attachments,
        })
    }

    #[cfg(not(target_os = "android"))]
    pub fn capture_clipboard_paste(&self, cwd: &Path) -> Result<PasteOutcome> {
        let mut clipboard = arboard::Clipboard::new().context("system clipboard is unavailable")?;
        if let Ok(files) = clipboard.get().file_list()
            && let Some(path) = files.into_iter().find(|path| is_supported_image(path))
        {
            return Ok(PasteOutcome {
                text: String::new(),
                attachments: vec![self.stage_path(&path)?],
            });
        }
        if let Ok(image) = clipboard.get_image() {
            let width = u32::try_from(image.width).context("clipboard image is too wide")?;
            let height = u32::try_from(image.height).context("clipboard image is too tall")?;
            let rgba = RgbaImage::from_raw(width, height, image.bytes.into_owned())
                .context("clipboard returned an invalid RGBA image")?;
            let mut encoded = Vec::new();
            DynamicImage::ImageRgba8(rgba)
                .write_to(&mut Cursor::new(&mut encoded), ImageFormat::Png)
                .context("failed to encode clipboard image")?;
            return Ok(PasteOutcome {
                text: String::new(),
                attachments: vec![self.write_image_bytes(&encoded, "png")?],
            });
        }
        let text = clipboard
            .get_text()
            .context("the clipboard does not contain text or an image")?;
        self.stage_paste(&text, cwd)
    }

    #[cfg(target_os = "android")]
    pub fn capture_clipboard_paste(&self, _cwd: &Path) -> Result<PasteOutcome> {
        bail!("clipboard paste is unavailable on Android/Termux")
    }

    fn write_image_bytes(&self, bytes: &[u8], extension: &str) -> Result<PathBuf> {
        if bytes.len() > MAX_INLINE_IMAGE_BYTES {
            bail!(
                "pasted image is too large ({} bytes; max {MAX_INLINE_IMAGE_BYTES})",
                bytes.len()
            );
        }
        image::load_from_memory(bytes).context("pasted terminal payload is not a valid image")?;
        let path = self.root.join(format!("{}.{extension}", Uuid::new_v4()));
        fs::write(&path, bytes)
            .with_context(|| format!("failed to persist pasted image {}", path.display()))?;
        Ok(path)
    }
}

pub fn is_supported_image(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp")
    ) && path.is_file()
}

fn pasted_paths(pasted: &str, cwd: &Path) -> Vec<PathBuf> {
    let normalized = normalize_newlines(pasted);
    let lines = normalized
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let mut paths = Vec::new();
    for line in lines {
        let candidates: Vec<PathBuf> = if line.starts_with("file://") {
            url::Url::parse(line)
                .ok()
                .and_then(|url| url.to_file_path().ok())
                .into_iter()
                .collect()
        } else {
            shlex::split(line)
                .unwrap_or_else(|| vec![trim_matching_quotes(line).to_string()])
                .into_iter()
                .map(PathBuf::from)
                .collect()
        };
        if candidates.is_empty() {
            return Vec::new();
        }
        for path in candidates {
            let path = if path.is_absolute() {
                path
            } else {
                cwd.join(path)
            };
            if !path.exists() {
                return Vec::new();
            }
            paths.push(path);
        }
    }
    paths
}

fn decode_terminal_image(pasted: &str) -> Result<Option<(Vec<u8>, &'static str)>> {
    let value = pasted.trim();
    // iTerm2: OSC 1337 ; File=[metadata] : base64 BEL/ST
    if let Some(payload) = value
        .strip_prefix("\u{1b}]1337;File=")
        .and_then(|value| value.split_once(':').map(|(_, payload)| payload))
    {
        let payload = payload
            .trim_end_matches('\u{7}')
            .trim_end_matches("\u{1b}\\");
        return decode_bounded_base64(payload, "png").map(Some);
    }
    // Kitty graphics: APC G <control-data> ; <base64 payload> ST. Clipboard
    // paste only exposes this in terminals that forward the raw escape.
    if let Some(body) = value.strip_prefix("\u{1b}_G")
        && let Some((_control, payload)) = body.split_once(';')
    {
        let payload = payload.trim_end_matches("\u{1b}\\");
        return decode_bounded_base64(payload, "png").map(Some);
    }
    Ok(None)
}

fn decode_bounded_base64(value: &str, extension: &'static str) -> Result<(Vec<u8>, &'static str)> {
    if value.len() > MAX_INLINE_IMAGE_BYTES.saturating_mul(2) {
        bail!("terminal image payload is too large");
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .context("terminal image payload is not valid base64")?;
    Ok((bytes, extension))
}

fn normalize_newlines(value: &str) -> String {
    value.replace("\r\n", "\n").replace('\r', "\n")
}

fn trim_matching_quotes(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
}

fn safe_filename(value: &str) -> Cow<'_, str> {
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Cow::Borrowed(value);
    }
    Cow::Owned(
        value
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_paste_is_persisted_for_session_transport() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("screen shot.png");
        let image = DynamicImage::ImageRgba8(RgbaImage::new(2, 3));
        image.save(&source).unwrap();
        let store = AttachmentStore::for_session(temp.path(), Uuid::nil()).unwrap();

        let outcome = store
            .stage_paste(&format!("'{}'", source.display()), temp.path())
            .unwrap();

        assert!(outcome.text.is_empty());
        assert_eq!(outcome.attachments.len(), 1);
        assert!(outcome.attachments[0].is_file());
        assert_ne!(outcome.attachments[0], source);
        assert_eq!(
            fs::read(&outcome.attachments[0]).unwrap(),
            fs::read(source).unwrap()
        );
    }

    #[test]
    fn ordinary_multiline_paste_remains_text() {
        let temp = tempfile::tempdir().unwrap();
        let store = AttachmentStore::for_session(temp.path(), Uuid::nil()).unwrap();
        let outcome = store
            .stage_paste("explain this\r\nwithout treating it as a path", temp.path())
            .unwrap();
        assert_eq!(outcome.text, "explain this\nwithout treating it as a path");
        assert!(outcome.attachments.is_empty());
    }

    #[test]
    fn prose_that_mentions_an_image_is_not_misclassified() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("screen.png");
        DynamicImage::ImageRgba8(RgbaImage::new(1, 1))
            .save(&source)
            .unwrap();
        let store = AttachmentStore::for_session(temp.path(), Uuid::nil()).unwrap();
        let pasted = format!("please inspect {}", source.display());
        let outcome = store.stage_paste(&pasted, temp.path()).unwrap();

        assert_eq!(outcome.text, pasted);
        assert!(outcome.attachments.is_empty());
    }
}
