//! Desktop and window capture, clipboard restore around niri captures, and
//! image decoding for window localisation.

use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use serde_json::{Value, json};

use super::windows::{self, CompositorWindow, Located, Rgb};
use super::{
    Helper, MAX_IMAGE_BYTES, WindowCapture, command, g, run, run_bytes, stderr_text, which,
};

const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";
const PNG_END: &[u8] = b"IEND\xaeB`\x82";

pub(super) fn png_size(data: &[u8]) -> Result<(u32, u32)> {
    if !data.starts_with(PNG_SIGNATURE) || data.len() < 24 {
        bail!("capture did not return a PNG");
    }
    let word = |at: usize| u32::from_be_bytes(data[at..at + 4].try_into().expect("4 bytes"));
    Ok((word(16), word(20)))
}

pub(super) fn attachment(data: &[u8]) -> Value {
    json!([{"media_type": "image/png",
            "data_base64": base64::engine::general_purpose::STANDARD.encode(data)}])
}

/// A window capture decoded to RGB, with its alpha channel when it has one.
pub(super) type Decoded = (Rgb, Option<Vec<u8>>);

/// A fresh window capture: decoded, and the PNG it came from.
pub(super) type Fresh = (Decoded, Vec<u8>);

pub(super) fn decode_rgb(data: &[u8]) -> Result<Decoded> {
    let image = image::load_from_memory(data).context("cannot decode the capture")?;
    let alpha = image
        .color()
        .has_alpha()
        .then(|| image.to_rgba8().pixels().map(|p| p.0[3]).collect());
    let rgb = image.to_rgb8();
    Ok((
        Rgb {
            width: rgb.width() as usize,
            height: rgb.height() as usize,
            data: rgb.into_raw(),
        },
        alpha,
    ))
}

/// Downscale a capture that exceeds the 4 MiB attachment bound; returns (png, scale).
fn fit_image(data: Vec<u8>) -> Result<(Vec<u8>, f64)> {
    if data.len() <= MAX_IMAGE_BYTES {
        return Ok((data, 1.0));
    }
    let image = image::load_from_memory(&data).context("cannot decode the capture")?;
    let mut size = data.len();
    let mut scale = 1.0f64;
    for _ in 0..6 {
        scale *= 0.85 * (MAX_IMAGE_BYTES as f64 / size as f64).sqrt();
        let width = (f64::from(image.width()) * scale)
            .round_ties_even()
            .max(1.0) as u32;
        let height = (f64::from(image.height()) * scale)
            .round_ties_even()
            .max(1.0) as u32;
        let resized = image.resize_exact(width, height, image::imageops::FilterType::CatmullRom);
        let mut buffer = Vec::new();
        resized.write_with_encoder(image::codecs::png::PngEncoder::new_with_quality(
            &mut buffer,
            image::codecs::png::CompressionType::Best,
            image::codecs::png::FilterType::Adaptive,
        ))?;
        if buffer.len() <= MAX_IMAGE_BYTES {
            return Ok((buffer, scale));
        }
        size = buffer.len();
    }
    bail!("window screenshot exceeds 4 MiB even after downscaling")
}

enum Clipboard {
    Unavailable,
    Empty,
    Unreadable(String),
    Saved {
        kind: String,
        data: Vec<u8>,
        types: usize,
    },
}

/// The current clipboard (one MIME type) so a compositor screenshot can be undone.
fn clipboard_snapshot() -> Clipboard {
    if which("wl-paste").is_none() || which("wl-copy").is_none() {
        return Clipboard::Unavailable;
    }
    let types: Vec<String> = run(command(&["wl-paste", "--list-types"]), None, 3)
        .ok()
        .filter(|run| run.status.success())
        .map(|run| {
            String::from_utf8_lossy(&run.stdout)
                .split_whitespace()
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let Some(kind) = windows::clipboard_restore_type(&types) else {
        return Clipboard::Empty;
    };
    match run(
        command(&["wl-paste", "--no-newline", "--type", &kind]),
        None,
        3,
    ) {
        Ok(content) if content.status.success() && content.stdout.len() <= 32 * 1024 * 1024 => {
            Clipboard::Saved {
                kind,
                data: content.stdout,
                types: types.len(),
            }
        }
        _ => Clipboard::Unreadable(kind),
    }
}

/// `wl-copy` forks a daemon that serves the selection, so it runs detached
/// with no pipes this helper would wait on.
fn wl_copy(args: &[&str], input: Option<&[u8]>) {
    let mut command = Command::new("wl-copy");
    command
        .args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: setsid is async-signal-safe.
    unsafe {
        command.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    let Ok(mut child) = command.spawn() else {
        return;
    };
    if let (Some(input), Some(mut stdin)) = (input, child.stdin.take()) {
        let _ = stdin.write_all(input);
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if !matches!(child.try_wait(), Ok(None)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Put the saved clipboard back once the compositor's own image selection landed.
fn clipboard_restore(saved: &Clipboard, capture: Option<&[u8]>) -> String {
    match saved {
        Clipboard::Unavailable => {
            return "clipboard now holds the capture (install wl-clipboard so Borg can restore it)"
                .into();
        }
        Clipboard::Unreadable(kind) => {
            return format!(
                "clipboard now holds the capture; the previous {kind} content was too large or unreadable to restore"
            );
        }
        _ => {}
    }
    // niri sets its selection asynchronously; restoring earlier would be
    // overwritten, and if something else replaced it meanwhile (the human
    // copied), leave it alone.
    if let Some(capture) = capture {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut landed = false;
        while Instant::now() < deadline {
            let probe = run(
                command(&["wl-paste", "--no-newline", "--type", "image/png"]),
                None,
                3,
            );
            if probe.is_ok_and(|p| p.status.success() && p.stdout == capture) {
                landed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(30));
        }
        if !landed {
            return "clipboard not restored: it no longer held the capture (it changed during the capture)".into();
        }
    }
    match saved {
        Clipboard::Saved { kind, data, types } => {
            wl_copy(&["--type", kind], Some(data));
            let extra = if *types <= 1 {
                ""
            } else {
                " (other offered formats were not restored)"
            };
            format!("clipboard restored as {kind}{extra}")
        }
        _ => {
            wl_copy(&["--clear"], None);
            "clipboard was empty and was cleared again".into()
        }
    }
}

/// niri renders exactly this window's surfaces (on-screen or not) without
/// changing focus. niri also copies every screenshot to the clipboard and may
/// show a 'Screenshot captured' notification; the clipboard is restored.
fn niri_capture(entry: &CompositorWindow) -> Result<(Vec<u8>, String, Vec<String>)> {
    let directory = tempfile::Builder::new().prefix("borg-cua-").tempdir()?;
    let path = directory.path().join("window.png");
    let saved = clipboard_snapshot();
    let mut data = None;
    let requested = windows::niri_request(json!({"Action": {"ScreenshotWindow": {
        "id": entry.native_id, "write_to_disk": true, "show_pointer": false,
        "path": path.display().to_string()}}}));
    if requested.is_ok() {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            // niri writes the file non-atomically; wait for the IEND chunk.
            if let Ok(candidate) = std::fs::read(&path)
                && candidate.starts_with(PNG_SIGNATURE)
                && candidate.ends_with(PNG_END)
            {
                data = Some(candidate);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    let note = clipboard_restore(&saved, data.as_deref());
    requested?;
    let data = data.ok_or_else(|| {
        anyhow!("niri did not write the window screenshot; the window may have closed")
    })?;
    Ok((
        data,
        "niri screenshot-window (isolated window surfaces)".into(),
        vec![
            note,
            "niri may show a 'Screenshot captured' notification".into(),
        ],
    ))
}

pub(super) fn grim_region(geometry: &windows::Rect, scale: f64) -> Result<Vec<u8>> {
    let spec = format!(
        "{},{} {}x{}",
        g(geometry.x as f64 / scale),
        g(geometry.y as f64 / scale),
        g(geometry.width as f64 / scale),
        g(geometry.height as f64 / scale)
    );
    run_bytes(&["grim", "-g", &spec, "-"], 5)
}

/// The compositor's current entry for this window.
pub(super) fn refreshed(entry: &CompositorWindow) -> Result<CompositorWindow> {
    windows::compositor_list(Some(entry.backend))?
        .into_iter()
        .find(|c| c.id == entry.id)
        .ok_or_else(|| anyhow!("window closed; list_windows again"))
}

fn capture_compositor_window(entry: &CompositorWindow) -> Result<(Vec<u8>, String, Vec<String>)> {
    let backend = entry.backend;
    if backend == "niri" {
        return niri_capture(entry);
    }
    let mut notes = Vec::new();
    if let Some(identifier) = &entry.toplevel_identifier
        && which("grim").is_some()
    {
        // ext-image-copy-capture: isolated even when hidden, where supported.
        let capture = run(command(&["grim", "-T", identifier, "-"]), None, 5)?;
        if capture.status.success() {
            return Ok((
                capture.stdout,
                "grim -T (ext-image-copy-capture, isolated)".into(),
                notes,
            ));
        }
        notes.push(format!(
            "grim -T unsupported here: {}",
            stderr_text(&capture, 200)
        ));
    }
    if backend == "x11" && which("import").is_some() {
        let id = format!("{:#x}", entry.native_id.as_i64().unwrap_or_default());
        let data = run_bytes(&["import", "-silent", "-window", &id, "png:-"], 5)?;
        return Ok((
            data,
            "ImageMagick import -window (X11; overlapping windows can show through)".into(),
            notes,
        ));
    }
    if matches!(backend, "sway" | "hyprland") {
        let mut entry = entry.clone();
        let mut prior = None;
        if entry.visible != Some(true) {
            // Bring it into view, capture, then put the human's focus back.
            prior = windows::compositor_list(Some(backend))?
                .into_iter()
                .find(|c| c.focused);
            super::input::compositor_focus(&entry)?;
            std::thread::sleep(Duration::from_millis(250));
            entry = refreshed(&entry)?;
            notes.push("window was brought into view for capture and prior focus restored".into());
        }
        let captured = entry
            .geometry
            .context("the compositor reports no geometry for this window")
            .and_then(|geometry| grim_region(&geometry, entry.scale));
        if let Some(prior) = prior.filter(|p| p.id != entry.id) {
            let _ = super::input::compositor_focus(&prior);
        }
        return Ok((
            captured?,
            "grim -g compositor geometry (region of the composited desktop)".into(),
            notes,
        ));
    }
    bail!("isolated window capture is unavailable with the {backend} window backend")
}

impl Helper {
    pub(super) fn screenshot(
        &mut self,
        scope: Option<&Value>,
        wid: Option<&Value>,
    ) -> Result<Value> {
        match scope.and_then(Value::as_str) {
            Some("window") => {
                let wid = wid
                    .and_then(Value::as_str)
                    .context("scope=window requires window_id")?
                    .to_string();
                self.window_screenshot(&wid)
            }
            Some("desktop") => {
                let capture = run(command(&["grim", "-"]), None, 5)?;
                if !capture.status.success() {
                    bail!("desktop capture failed: {}", stderr_text(&capture, 1024));
                }
                let data = capture.stdout;
                let (width, height) = png_size(&data)?;
                if data.len() > MAX_IMAGE_BYTES {
                    bail!("screenshot exceeds 4 MiB");
                }
                self.screen = Some((width, height));
                Ok(json!({"scope": "desktop", "width": width, "height": height,
                          "coordinate_space": "screenshot pixels, not AT-SPI screen coordinates",
                          "borg_attachments": attachment(&data)}))
            }
            _ => bail!("scope must be \"desktop\" or \"window\" (window needs window_id)"),
        }
    }

    fn window_screenshot(&mut self, wid: &str) -> Result<Value> {
        let Some(entry) = self.compositor_for(wid)? else {
            let detail = match windows::compositor_backend() {
                Some(backend) => format!("{backend} does not list this window"),
                None => "none was detected in this session".into(),
            };
            bail!(
                "window capture needs a compositor window backend (niri, sway, Hyprland or X11 EWMH); {detail}"
            );
        };
        let (data, method, notes) = capture_compositor_window(&entry)?;
        let raw_size = png_size(&data)?;
        let (data, scale) = fit_image(data)?;
        let (width, height) = png_size(&data)?;
        self.window_captures.insert(
            wid.to_string(),
            WindowCapture {
                scale,
                size: raw_size,
            },
        );
        let notes: Vec<String> = notes.into_iter().filter(|n| !n.is_empty()).collect();
        Ok(
            json!({"scope": "window", "window_id": wid, "width": width, "height": height,
                  "scale": (scale * 1e6).round_ties_even() / 1e6,
                  "capture_backend": method, "notes": notes, "compositor": entry.public(),
                  "coordinate_space": "window screenshot pixels; pass coordinate_space=window to pointer ops to target them",
                  "borg_attachments": attachment(&data)}),
        )
    }

    /// Locate the window's pixels on a fresh desktop frame. Also returns a new
    /// window capture (decoded, raw) when one was taken.
    pub(super) fn locate_on_desktop(
        &mut self,
        entry: &CompositorWindow,
        window_image: Option<&Decoded>,
    ) -> Result<(Option<Located>, Option<Fresh>)> {
        let mut desktop = Command::new("grim")
            .args(["-t", "ppm", "-"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("cannot run grim")?;
        let mut pipe = desktop.stdout.take().context("grim stdout unavailable")?;
        let reader = std::thread::spawn(move || {
            let mut shot = Vec::new();
            let _ = pipe.read_to_end(&mut shot);
            shot
        });
        let fresh = match window_image {
            Some(_) => Ok(None),
            None => {
                niri_capture(entry).and_then(|(image, _, _)| Ok(Some((decode_rgb(&image)?, image))))
            }
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(desktop.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let _ = desktop.kill();
        let _ = desktop.wait();
        let shot = reader.join().unwrap_or_default();
        let fresh = fresh?;
        let desk = decode_rgb(&shot)?.0;
        self.screen = Some((desk.width as u32, desk.height as u32));
        let (window, alpha) = match (&fresh, window_image) {
            (Some((decoded, _)), _) | (None, Some(decoded)) => decoded,
            (None, None) => unreachable!("a window image was captured or given"),
        };
        let located = windows::locate_window(window, &desk, alpha.as_deref());
        Ok((located, fresh))
    }
}
