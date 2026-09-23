//! Borg-owned Linux desktop worker: AT-SPI2 accessibility, compositor window
//! listing and capture, uinput input injection and the agent-private display.
//! Runs as `borg __computer-use-helper`, speaking one JSON request and one JSON
//! response per line on stdin/stdout, so a hung desktop call or a crash never
//! reaches the Borg host and killing the process invalidates every handle.

mod a11y;
mod capture;
mod input;
mod private;
mod windows;

use std::collections::HashMap;
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};

use a11y::{A11y, Obj, Observation};
use private::{BorgDisplay, PrivateApp};
use windows::{CompositorWindow, Rgb};

const MAX_IMAGE_BYTES: usize = 4 * 1024 * 1024;

/// Serve requests until stdin closes, then tear down the private display.
pub fn run_helper() -> Result<()> {
    let mut helper = Helper::new();
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        let response = match serde_json::from_str::<Value>(&line)
            .map_err(anyhow::Error::from)
            .and_then(|request| helper.dispatch(&request))
        {
            Ok(result) => json!({"ok": true, "result": result}),
            Err(error) => json!({"ok": false, "error": format!("{error:#}")}),
        };
        serde_json::to_writer(&mut stdout, &response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    helper.stop_display();
    Ok(())
}

/// A window as an operation targets it.
#[derive(Clone)]
enum Win {
    /// An AT-SPI window (possibly correlated with a compositor entry).
    Accessible(Obj),
    /// A compositor-listed window without an accessibility tree.
    Compositor(Box<CompositorWindow>),
}

#[derive(Clone, Copy)]
struct WindowCapture {
    /// Image pixels per window pixel of the latest window screenshot.
    scale: f64,
    size: (u32, u32),
}

/// Where a window was last found on the desktop.
struct LocatedWindow {
    image: Option<(Rgb, Option<Vec<u8>>)>,
    origin: (i64, i64),
    geometry: bool,
}

struct Focus {
    /// The human's window from before Borg moved focus.
    human: Option<CompositorWindow>,
    /// Borg's last focus target.
    borg: Option<String>,
    changed_at: Instant,
}

struct Helper {
    a11y: Option<A11y>,
    epoch: String,
    objects: HashMap<String, Obj>,
    object_ids: HashMap<Obj, String>,
    next_id: u64,
    observations: HashMap<String, Observation>,
    /// Compositor window id -> entry, refreshed by `windows()`.
    compositor: HashMap<String, CompositorWindow>,
    /// AT-SPI window id -> correlated compositor entry.
    accessible_compositor: HashMap<String, CompositorWindow>,
    compositor_error: Option<String>,
    window_captures: HashMap<String, WindowCapture>,
    focus: Focus,
    /// Window the helper last placed the pointer in.
    pointer_window: Option<String>,
    located: HashMap<String, LocatedWindow>,
    /// Desktop size in screenshot pixels.
    screen: Option<(u32, u32)>,
    input_device: Option<evdev::uinput::VirtualDevice>,
    relative_device: Option<evdev::uinput::VirtualDevice>,
    private: Option<BorgDisplay>,
    private_apps: HashMap<u32, PrivateApp>,
    private_pointer: Option<String>,
}

impl Helper {
    fn new() -> Self {
        Self {
            a11y: None,
            epoch: uuid::Uuid::new_v4().simple().to_string()[..12].to_string(),
            objects: HashMap::new(),
            object_ids: HashMap::new(),
            next_id: 0,
            observations: HashMap::new(),
            compositor: HashMap::new(),
            accessible_compositor: HashMap::new(),
            compositor_error: None,
            window_captures: HashMap::new(),
            focus: Focus {
                human: None,
                borg: None,
                changed_at: Instant::now(),
            },
            pointer_window: None,
            located: HashMap::new(),
            screen: None,
            input_device: None,
            relative_device: None,
            private: None,
            private_apps: HashMap::new(),
            private_pointer: None,
        }
    }

    fn dispatch(&mut self, args: &Value) -> Result<Value> {
        let op = args["op"].as_str().context("op is required")?;
        let display = args
            .get("display")
            .and_then(Value::as_str)
            .unwrap_or("desktop");
        if args.get("display").is_some_and(|d| !d.is_string())
            || !matches!(display, "desktop" | "private")
        {
            bail!("display must be \"desktop\" or \"private\"");
        }
        let private_window = is_private(&args["window_id"]);
        match op {
            "start_display" => return self.start_display(args),
            "stop_display" => return Ok(self.stop_display()),
            "attach_display" => return self.attach_display(args),
            "launch" => return self.launch(args),
            "list_windows" if display == "private" => {
                return Ok(
                    json!({"windows": self.private_windows(true)?, "display": self.display_status(None)?}),
                );
            }
            "screenshot" if display == "private" || private_window => {
                return self.private_screenshot(args);
            }
            "pointer_move" if private_window => return self.private_inject(args),
            _ => {}
        }
        match op {
            "capabilities" => self.capabilities(),
            "list_windows" => {
                let listed = self.windows()?;
                let mut result =
                    json!({"windows": listed, "window_backend": windows::compositor_backend()});
                if let Some(error) = &self.compositor_error {
                    result["window_backend_error"] = json!(error);
                }
                Ok(result)
            }
            "screenshot" => self.screenshot(args.get("scope"), args.get("window_id")),
            "observe" => self.snapshot(args),
            "click" | "set_value" => self.mutate(args),
            "type_text" | "key" | "pointer_click" | "scroll" | "drag" | "pointer_move" => {
                self.inject(args)
            }
            _ => bail!("unsupported operation: {op}"),
        }
    }

    fn capabilities(&mut self) -> Result<Value> {
        let missing = input::input_requirements();
        let (backend, window_capture, mut limitations) = window_capabilities();
        let mut operations: Vec<&str> = vec![
            "capabilities",
            "list_windows",
            "observe",
            "screenshot",
            "click",
            "set_value",
        ];
        let desktop_capture = which("grim").is_some() && env_set("WAYLAND_DISPLAY");
        if !desktop_capture {
            limitations.push(
                "Desktop screenshots need grim on a Wayland compositor with wlr-screencopy.".into(),
            );
        }
        if missing.is_empty() {
            operations.extend([
                "type_text",
                "key",
                "pointer_click",
                "scroll",
                "drag",
                "pointer_move",
            ]);
            limitations.push("Input injection focuses the target window through the compositor when it lists the window (pass restore_focus=true to hand focus back afterwards) and refuses if it did not become focused; events reach the focused window.".into());
            limitations.push("pointer_move emits raw relative motion (REL_X/REL_Y) from a separate Borg virtual mouse; games with pointer lock receive unaccelerated deltas, the visible cursor follows compositor acceleration. key hold_ms (up to 10 s) and pointer_move hold_keys hold keys for games.".into());
            if session_type() == Some("wayland") {
                limitations.push("On Wayland, element-targeted pointer_click/scroll map window-relative AT-SPI extents through the compositor's window position; they are refused when the compositor does not list the window.".into());
            }
        } else {
            limitations.push(format!(
                "Input injection unavailable: {}.",
                missing.join("; ")
            ));
        }
        let private = self.private_capabilities()?;
        if private["available"] == true {
            operations.extend(["start_display", "stop_display", "attach_display", "launch"]);
            for op in ["type_text", "key", "pointer_click", "scroll", "drag"] {
                if !operations.contains(&op) {
                    operations.push(op);
                }
            }
            limitations.push("Prefer the private display (launch, then display=private / pd: window ids) for app testing: its input and capture never touch the user's desktop. Desktop input goes to the user's focused window.".into());
        }
        let mut scopes = Vec::new();
        if desktop_capture {
            scopes.push("desktop");
        }
        if window_capture {
            scopes.push("window");
        }
        Ok(json!({
            "platform": "linux",
            "backend": "AT-SPI2",
            "desktop_available": self.a11y().is_ok(),
            "session_type": session_type(),
            "operations": operations,
            "window_backend": backend,
            "capture_scopes": scopes,
            "input_backend": missing.is_empty().then(|| format!("evdev uinput + {}", typing_tool().unwrap_or_default())),
            "input_coordinate_space": "desktop screenshot pixels (top-left origin); coordinate_space=window uses window screenshot pixels",
            "limitations": limitations,
            "private_display": private,
        }))
    }
}

/// Window backend, whether window capture works, and honest limitations.
fn window_capabilities() -> (Option<&'static str>, bool, Vec<String>) {
    let backend = windows::compositor_backend();
    let mut limitations = Vec::new();
    let capture = match backend {
        Some("niri") => {
            limitations.push(format!("niri: scope=window uses niri's own window screenshot, which renders only that window's surfaces even when it is on another workspace or scrolled off-screen, without changing focus. niri also copies each capture to the clipboard and shows a transient 'Screenshot captured' notification; Borg restores the previous clipboard contents in one format{}.", if which("wl-copy").is_some() { "" } else { " once wl-clipboard is installed (it is missing)" }));
            limitations.push("niri: coordinate_space=window and element-targeted pointer ops use niri's IPC geometry for floating windows (no capture). niri exposes no position for tiled windows, so for those the helper falls back to locating the window by matching a fresh window capture on the desktop (another capture + notification per op), requires the position to hold for 300 ms, re-checks it right before pressing, and refuses covered, off-screen, featureless or duplicate-looking windows.".into());
            true
        }
        Some(name @ ("sway" | "hyprland")) => {
            limitations.push(format!("{name}: scope=window crops the composited desktop to the compositor's window geometry (grim -g; grim -T when the compositor exposes an ext-foreign-toplevel identifier); overlapping windows appear in the crop and hidden windows are briefly brought into view, then prior focus is restored."));
            which("grim").is_some()
        }
        Some("x11") => {
            let capture = which("import").is_some();
            limitations.push(format!("X11: windows come from EWMH _NET_CLIENT_LIST; scope=window uses ImageMagick import -window{}, where overlapping windows can show through without a compositor.", if capture { "" } else { " (not installed: install imagemagick)" }));
            capture
        }
        Some("foreign-toplevel") => {
            limitations.push("wlr/ext foreign-toplevel via lswt: windows are listed without geometry or pid; scope=window needs grim -T support in the compositor.".into());
            which("grim").is_some()
        }
        _ => {
            limitations.push("No compositor window backend detected (niri, sway, Hyprland, lswt or X11 EWMH): list_windows shows only AT-SPI windows and scope=window is unavailable.".into());
            false
        }
    };
    (backend, capture, limitations)
}

// ---- shared helpers -----------------------------------------------------------

/// Python's `round()`: half to even.
pub(super) fn py_round(value: f64) -> i64 {
    value.round_ties_even() as i64
}

fn env_set(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

pub(super) fn which(program: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(program))
        .find(|path| {
            use std::os::unix::fs::PermissionsExt;
            path.metadata()
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
}

fn session_type() -> Option<&'static str> {
    if env_set("WAYLAND_DISPLAY") {
        Some("wayland")
    } else if env_set("DISPLAY") {
        Some("x11")
    } else {
        None
    }
}

fn typing_tool() -> Option<&'static str> {
    match session_type()? {
        "wayland" => Some("wtype"),
        _ => Some("xdotool"),
    }
}

pub(super) struct Run {
    pub status: std::process::ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Run a command to completion with a deadline, killing it on timeout.
pub(super) fn run(mut command: Command, input: Option<&[u8]>, timeout: u64) -> Result<Run> {
    let program = command.get_program().to_string_lossy().into_owned();
    command
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("cannot run {program}"))?;
    if let Some(input) = input {
        let mut stdin = child.stdin.take().context("stdin unavailable")?;
        let input = input.to_vec();
        std::thread::spawn(move || stdin.write_all(&input));
    }
    let reader = |mut pipe: Box<dyn Read + Send>| {
        std::thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = pipe.read_to_end(&mut buffer);
            buffer
        })
    };
    let stdout = reader(Box::new(child.stdout.take().context("stdout unavailable")?));
    let stderr = reader(Box::new(child.stderr.take().context("stderr unavailable")?));
    let deadline = Instant::now() + Duration::from_secs(timeout);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("{program} timed out after {timeout} s");
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    Ok(Run {
        status,
        stdout: stdout.join().unwrap_or_default(),
        stderr: stderr.join().unwrap_or_default(),
    })
}

fn command(argv: &[&str]) -> Command {
    let mut command = Command::new(argv[0]);
    command.args(&argv[1..]);
    command
}

fn stderr_text(run: &Run, limit: usize) -> String {
    String::from_utf8_lossy(&run.stderr)
        .trim()
        .chars()
        .take(limit)
        .collect()
}

pub(super) fn run_bytes(argv: &[&str], timeout: u64) -> Result<Vec<u8>> {
    let run = run(command(argv), None, timeout)?;
    if !run.status.success() {
        bail!("{} failed: {}", argv[0], stderr_text(&run, 1024));
    }
    Ok(run.stdout)
}

pub(super) fn run_text(argv: &[&str], timeout: u64) -> Result<String> {
    let run = run(command(argv), None, timeout)?;
    if !run.status.success() {
        bail!("{} failed: {}", argv[0], stderr_text(&run, 512));
    }
    Ok(String::from_utf8_lossy(&run.stdout).into_owned())
}

/// A finite JSON number (not a boolean).
fn number(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64).filter(|v| v.is_finite())
}

/// An optional integer argument within bounds.
fn bounded_int(args: &Value, name: &str, default: i64, low: i64, high: i64) -> Result<i64> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(default),
        Some(value) => value
            .as_i64()
            .filter(|v| (low..=high).contains(v))
            .ok_or_else(|| anyhow!("{name} must be an integer between {low} and {high}")),
    }
}

/// A numeric argument that defaults to zero when absent.
fn arg_or_zero(args: &Value, key: &str) -> Option<f64> {
    match args.get(key) {
        None => Some(0.0),
        value => number(value),
    }
}

fn is_private(window_id: &Value) -> bool {
    window_id
        .as_str()
        .is_some_and(|id| id.starts_with(private::PREFIX))
}

fn window_id(args: &Value) -> Result<String> {
    args["window_id"]
        .as_str()
        .map(str::to_string)
        .context("window_id is required")
}

/// `f"{value:g}"` for messages and command arguments.
fn g(value: f64) -> String {
    let text = format!("{value}");
    text.strip_suffix(".0").map_or(text.clone(), str::to_string)
}
