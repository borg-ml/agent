//! Agent-private display. Apps run on a Borg-owned headless compositor
//! (borg-display) whose owner control socket carries every input event and
//! capture, so nothing here touches the user's seat, pointer, focus or screen.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::{Value, json};

use super::a11y::merge;
use super::capture::attachment;
use super::input::{button_code, notches, parse_keys};
use super::{Helper, arg_or_zero, number, run, which, window_id};

pub(in crate::computer_use) const PREFIX: &str = "pd:";
const SCRUBBED_DISPLAY_ENV: &[&str] = &[
    "DISPLAY",
    "WAYLAND_DISPLAY",
    "WAYLAND_SOCKET",
    "NIRI_SOCKET",
    "SWAYSOCK",
    "HYPRLAND_INSTANCE_SIGNATURE",
    "XAUTHORITY",
    "DESKTOP_STARTUP_ID",
    "XDG_ACTIVATION_TOKEN",
];
const DISPLAY_MISSING: &str = "the private display needs borg-display, which is not installed next to borg or on PATH; update or reinstall Borg (Linux release archives, `borg update` and `just cli` install it beside borg), or set BORG_DISPLAY_BIN to a borg-display binary";
/// Headless-capable compositors another backend could drive; reported only.
const ALTERNATIVE_BACKENDS: &[&str] = &["sway", "cage", "labwc", "weston"];

pub(super) type PrivateApp = Child;

pub(in crate::computer_use) struct PrivateWindow {
    /// The compositor's own window id.
    pub id: Value,
    pub title: String,
    pub pid: Option<i64>,
    pub bounds: Value,
}

/// Remove the display environment so children only see the private display.
fn scrub(command: &mut Command) {
    for name in SCRUBBED_DISPLAY_ENV {
        command.env_remove(name);
    }
}

/// Start the child in its own session; optionally deliver SIGTERM to it when
/// this helper dies, even by SIGKILL.
fn detach(command: &mut Command, die_with_helper: bool) {
    // SAFETY: setsid and prctl are async-signal-safe.
    unsafe {
        command.pre_exec(move || {
            libc::setsid();
            if die_with_helper {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            }
            Ok(())
        });
    }
}

/// SIGTERM each child's process group, wait once for all, then SIGKILL.
fn terminate_groups(processes: &mut [&mut Child], grace: Duration) {
    let signal = |processes: &[&mut Child], signal| {
        for process in processes {
            // SAFETY: signalling a process group this helper started.
            unsafe {
                libc::killpg(process.id() as i32, signal);
            }
        }
    };
    signal(processes, libc::SIGTERM);
    let deadline = Instant::now() + grace;
    while processes
        .iter_mut()
        .any(|p| matches!(p.try_wait(), Ok(None)))
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(50));
    }
    signal(processes, libc::SIGKILL);
    for process in processes.iter_mut() {
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(process.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn running(child: &mut Child) -> bool {
    matches!(child.try_wait(), Ok(None))
}

fn runtime_dir() -> Result<PathBuf> {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|dir| dir.is_dir())
        .context("XDG_RUNTIME_DIR is required for the private display sockets")
}

fn log_tail(path: &Path, chars: usize) -> String {
    let text = String::from_utf8_lossy(&std::fs::read(path).unwrap_or_default()).into_owned();
    let skip = text.chars().count().saturating_sub(chars);
    text.chars().skip(skip).collect()
}

/// Private display backend on borg-display.
pub(super) struct BorgDisplay {
    pub display_id: String,
    pub owned: bool,
    pub directory: PathBuf,
    pub wayland_display: String,
    process: Option<Child>,
    stream: Option<UnixStream>,
    reader: Option<BufReader<UnixStream>>,
    closed: bool,
    /// xwayland-satellite and its X display, when an X11 app was launched.
    x11: Option<(Child, String)>,
}

impl BorgDisplay {
    fn binary() -> Option<PathBuf> {
        if let Some(configured) = std::env::var_os("BORG_DISPLAY_BIN").map(PathBuf::from) {
            use std::os::unix::fs::PermissionsExt;
            if configured
                .metadata()
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            {
                return Some(configured);
            }
        }
        which("borg-display")
    }

    fn start(width: i64, height: i64, render_node: Option<String>) -> Result<Self> {
        let binary = Self::binary().context(DISPLAY_MISSING)?;
        let runtime = runtime_dir()?;
        Self::sweep(&runtime);
        // display_id names both sockets so another session can attach to it.
        let display_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_string();
        let directory = runtime.join(format!("borg-display-{display_id}"));
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        }
        let control = directory.join("control");
        let log_path = directory.join("display.log");
        let mut command = Command::new(&binary);
        command
            .arg("--socket")
            .arg(format!("borg-private-{display_id}"))
            .arg("--control")
            .arg(&control)
            .arg("--size")
            .arg(format!("{width}x{height}"));
        if let Some(node) = render_node.filter(|n| !n.is_empty()) {
            command.arg("--render-node").arg(node);
        }
        scrub(&mut command);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(&log_path)?);
        // SAFETY: prctl is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                Ok(())
            });
        }
        let mut process = command.spawn().context("cannot start borg-display")?;
        let ready = process
            .stdout
            .as_ref()
            .and_then(|stdout| {
                let mut poll = libc::pollfd {
                    fd: stdout.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: one valid pollfd.
                (unsafe { libc::poll(&mut poll, 1, 15_000) } > 0).then_some(())
            })
            .and_then(|()| {
                let mut line = Vec::new();
                let stdout = process.stdout.as_mut()?;
                let mut byte = [0u8; 1];
                // Read only the readiness line; the rest stays unread.
                while stdout.read(&mut byte).ok()? == 1 && byte[0] != b'\n' {
                    line.push(byte[0]);
                }
                serde_json::from_slice::<Value>(&line).ok()
            })
            .filter(|ready| ready["ready"] == true);
        let Some(ready) = ready else {
            let _ = process.kill();
            let _ = process.wait();
            let detail = log_tail(&log_path, 1024).trim().to_string();
            let _ = std::fs::remove_dir_all(&directory);
            bail!(
                "borg-display failed to start: {}",
                if detail.is_empty() {
                    "no readiness report"
                } else {
                    &detail
                }
            );
        };
        let mut display = Self {
            display_id,
            owned: true,
            directory,
            wayland_display: ready["wayland_display"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            process: Some(process),
            stream: None,
            reader: None,
            closed: false,
            x11: None,
        };
        display.connect(&control)?;
        Ok(display)
    }

    /// Share a display another session started, as a non-owner: it keeps
    /// running when this session detaches, and stops when its owner does.
    fn attach(display_id: Option<&Value>) -> Result<Self> {
        let display_id = display_id
            .and_then(Value::as_str)
            .filter(|id| id.len() == 16 && id.bytes().all(|b| b"0123456789abcdef".contains(&b)))
            .context(
                "display_id must be the 16-hex-digit display_id reported by the display's owner",
            )?;
        let directory = runtime_dir()?.join(format!("borg-display-{display_id}"));
        let control = directory.join("control");
        ensure!(
            control.exists(),
            "no running private display has that display_id"
        );
        let mut display = Self {
            display_id: display_id.to_string(),
            owned: false,
            directory,
            wayland_display: format!("borg-private-{display_id}"),
            process: None,
            stream: None,
            reader: None,
            closed: false,
            x11: None,
        };
        display.connect(&control)?;
        Ok(display)
    }

    fn connect(&mut self, control: &Path) -> Result<()> {
        let stream = UnixStream::connect(control)?;
        stream.set_read_timeout(Some(Duration::from_secs(20)))?;
        stream.set_write_timeout(Some(Duration::from_secs(20)))?;
        self.reader = Some(BufReader::new(stream.try_clone()?));
        self.stream = Some(stream);
        self.closed = false;
        Ok(())
    }

    /// Remove directories of displays whose helper was killed: the compositor
    /// deletes its control socket on exit, so only logs remain.
    fn sweep(runtime: &Path) {
        let Ok(entries) = std::fs::read_dir(runtime) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let stale = entry
                .file_name()
                .to_string_lossy()
                .starts_with("borg-display-")
                && !path.join("control").exists()
                && entry
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|m| m.elapsed().ok())
                    .is_some_and(|age| age > Duration::from_secs(60));
            if stale {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }

    fn running(&mut self) -> bool {
        !self.closed && self.process.as_mut().is_none_or(running)
    }

    fn call(&mut self, request: Value) -> Result<Value> {
        ensure!(
            self.running(),
            "the private display exited; call start_display again"
        );
        let mut line = Vec::new();
        let sent = (|| -> std::io::Result<()> {
            let stream = self.stream.as_mut().expect("connected");
            stream.write_all(format!("{request}\n").as_bytes())?;
            self.reader
                .as_mut()
                .expect("connected")
                .take(8 * 1024 * 1024)
                .read_until(b'\n', &mut line)?;
            Ok(())
        })();
        if let Err(error) = sent {
            self.closed = true;
            bail!("private display control failed: {error}");
        }
        if line.is_empty() {
            self.closed = true;
            bail!("the private display exited; call start_display again");
        }
        let response: Value = serde_json::from_slice(&line)?;
        if response["ok"] != true {
            bail!(
                "{}",
                response["error"]
                    .as_str()
                    .unwrap_or("private display request failed")
            );
        }
        Ok(response["result"].clone())
    }

    fn info(&mut self) -> Result<Value> {
        self.call(json!({"op": "info"}))
    }

    fn windows(&mut self) -> Result<Vec<Value>> {
        Ok(self.call(json!({"op": "windows"}))?["windows"]
            .as_array()
            .cloned()
            .unwrap_or_default())
    }

    fn focus(&mut self, window: &Value) -> Result<()> {
        self.call(json!({"op": "focus", "window": window}))
            .map(drop)
    }

    fn pointer_move(&mut self, x: f64, y: f64) -> Result<()> {
        self.call(json!({"op": "pointer_move", "x": x, "y": y}))
            .map(drop)
    }

    fn pointer_relative(&mut self, dx: f64, dy: f64) -> Result<()> {
        self.call(json!({"op": "pointer_move", "dx": dx, "dy": dy}))
            .map(drop)
    }

    fn button(&mut self, code: u16, pressed: bool) -> Result<()> {
        self.call(json!({"op": "button", "code": code, "pressed": pressed}))
            .map(drop)
    }

    fn axis(&mut self, dx: i64, dy: i64) -> Result<()> {
        self.call(json!({"op": "axis", "dx": dx, "dy": dy}))
            .map(drop)
    }

    fn key(&mut self, code: u16, pressed: bool) -> Result<()> {
        self.call(json!({"op": "key", "code": code, "pressed": pressed}))
            .map(drop)
    }

    fn stop(&mut self) {
        self.stream = None;
        self.reader = None;
        self.closed = true;
        if let Some((mut satellite, display)) = self.x11.take() {
            stop_x11(&mut satellite, &display);
        }
        if !self.owned {
            return;
        }
        if let Some(mut process) = self.process.take() {
            // Closing the owner socket makes it exit.
            let deadline = Instant::now() + Duration::from_secs(5);
            while running(&mut process) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = process.kill();
            let _ = process.wait();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Stop xwayland-satellite and remove the X socket and lock it leaves behind.
fn stop_x11(satellite: &mut Child, display: &str) {
    terminate_groups(&mut [satellite], Duration::from_secs(1));
    let number = display.trim_start_matches(':');
    for path in [
        format!("/tmp/.X11-unix/X{number}"),
        format!("/tmp/.X{number}-lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }
}

fn descends_from(mut pid: i64, ancestor: i64) -> bool {
    for _ in 0..64 {
        if pid == ancestor {
            return true;
        }
        let Some(parent) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| {
                let (_, rest) = stat.rsplit_once(')')?;
                rest.split_whitespace().nth(1)?.parse::<i64>().ok()
            })
        else {
            return false;
        };
        if parent <= 1 {
            return false;
        }
        pid = parent;
    }
    false
}

fn display_size(args: &Value) -> Result<(i64, i64)> {
    let mut size = [0; 2];
    for (slot, (key, default)) in size.iter_mut().zip([("width", 1920), ("height", 1080)]) {
        *slot = match args.get(key) {
            None => default,
            Some(value) => value
                .as_i64()
                .filter(|v| (64..=8192).contains(v))
                .with_context(|| format!("{key} must be an integer between 64 and 8192"))?,
        };
    }
    Ok((size[0], size[1]))
}

impl Helper {
    fn private_running(&mut self) -> bool {
        self.private.as_mut().is_some_and(BorgDisplay::running)
    }

    fn require_private(&mut self) -> Result<&mut BorgDisplay> {
        ensure!(
            self.private_running(),
            "the private display is not running; call start_display or launch"
        );
        Ok(self.private.as_mut().expect("running"))
    }

    pub(super) fn display_status(&mut self, info: Option<Value>) -> Result<Value> {
        if !self.private_running() {
            return Ok(json!({"display": "private", "running": false}));
        }
        let backend = self.private.as_mut().expect("running");
        let info = match info {
            Some(info) => info,
            None => backend.info()?,
        };
        let x11 = backend
            .x11
            .as_mut()
            .and_then(|(satellite, display)| running(satellite).then(|| display.clone()));
        let (display_id, owner, wayland) = (
            backend.display_id.clone(),
            backend.owned,
            backend.wayland_display.clone(),
        );
        let mut apps: Vec<u32> = self
            .private_apps
            .iter_mut()
            .filter_map(|(pid, app)| running(app).then_some(*pid))
            .collect();
        apps.sort_unstable();
        Ok(
            json!({"display": "private", "running": true, "backend": "borg-display",
                  "display_id": display_id, "owner": owner, "wayland_display": wayland,
                  "x11_display": x11, "width": info["width"], "height": info["height"],
                  "gpu_accelerated": info["hardware"], "gl_renderer": info["gl_renderer"],
                  "render_node": info["render_node"], "dmabuf": info["dmabuf"],
                  "session_apps": apps,
                  "coordinate_space": "private display pixels, top-left origin, scale 1"}),
        )
    }

    pub(super) fn start_display(&mut self, args: &Value) -> Result<Value> {
        let (width, height) = display_size(args)?;
        if self.private_running() {
            let info = self.private.as_mut().expect("running").info()?;
            let current = (info["width"].as_i64(), info["height"].as_i64());
            if (args.get("width").is_some() || args.get("height").is_some())
                && current != (Some(width), Some(height))
            {
                bail!(
                    "the private display is already running at {}x{}; stop_display first to change its size",
                    info["width"],
                    info["height"]
                );
            }
            return self.display_status(Some(info));
        }
        if self.private.is_some() {
            self.stop_display(); // the compositor died: reap what it left behind
        }
        let render_node = args
            .get("render_node")
            .and_then(Value::as_str)
            .filter(|n| !n.is_empty())
            .map(str::to_string)
            .or_else(|| std::env::var("BORG_DISPLAY_RENDER_NODE").ok());
        self.private = Some(BorgDisplay::start(width, height, render_node)?);
        self.display_status(None)
    }

    pub(super) fn attach_display(&mut self, args: &Value) -> Result<Value> {
        ensure!(
            !self.private_running(),
            "this session already uses a private display; stop_display before attaching to another"
        );
        if self.private.is_some() {
            self.stop_display();
        }
        self.private = Some(BorgDisplay::attach(args.get("display_id"))?);
        self.display_status(None)
    }

    pub(super) fn stop_display(&mut self) -> Value {
        let mut terminated: Vec<u32> = self.private_apps.keys().copied().collect();
        terminated.sort_unstable();
        let mut apps: Vec<Child> = self.private_apps.drain().map(|(_, app)| app).collect();
        terminate_groups(
            &mut apps.iter_mut().collect::<Vec<_>>(),
            Duration::from_secs(3),
        );
        let Some(mut backend) = self.private.take() else {
            return json!({"display": "private", "running": false, "stopped": false,
                          "terminated_pids": terminated});
        };
        backend.stop();
        json!({"display": "private", "running": false, "stopped": backend.owned,
               "detached": !backend.owned, "terminated_pids": terminated})
    }

    fn ensure_x11(&mut self) -> Result<String> {
        let backend = self.private.as_mut().expect("running");
        if let Some((satellite, display)) = backend.x11.as_mut()
            && running(satellite)
        {
            return Ok(display.clone());
        }
        let satellite = which("xwayland-satellite").context("X11 apps on the private display need xwayland-satellite (package xwayland-satellite) and Xwayland; launch a Wayland-native app or install it")?;
        let number = (100..1000)
            .find(|n| {
                !Path::new(&format!("/tmp/.X11-unix/X{n}")).exists()
                    && !Path::new(&format!("/tmp/.X{n}-lock")).exists()
            })
            .context("no free X display number")?;
        let log_path = backend.directory.join("xwayland.log");
        let log = std::fs::File::create(&log_path)?;
        let mut command = Command::new(satellite);
        command.arg(format!(":{number}"));
        scrub(&mut command);
        command
            .env("WAYLAND_DISPLAY", &backend.wayland_display)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        detach(&mut command, true);
        let mut process = command.spawn()?;
        let display = format!(":{number}");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !Path::new(&format!("/tmp/.X11-unix/X{number}")).exists() {
            if !running(&mut process) || Instant::now() > deadline {
                stop_x11(&mut process, &display);
                bail!(
                    "xwayland-satellite did not start; see {}",
                    log_path.display()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        backend.x11 = Some((process, display.clone()));
        Ok(display)
    }

    pub(super) fn launch(&mut self, args: &Value) -> Result<Value> {
        let argv: Vec<String> = args
            .get("argv")
            .and_then(Value::as_array)
            .filter(|argv| !argv.is_empty() && argv.len() <= 256)
            .and_then(|argv| {
                argv.iter()
                    .map(|a| a.as_str().map(str::to_string))
                    .collect()
            })
            .context("argv must be a non-empty list of strings")?;
        let overrides: Vec<(String, String)> = match args.get("env") {
            None | Some(Value::Null) => Vec::new(),
            Some(env) => env
                .as_object()
                .and_then(|env| {
                    env.iter()
                        .map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                        .collect()
                })
                .context("env must map strings to strings")?,
        };
        let mut redirected: Vec<&str> = overrides
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| SCRUBBED_DISPLAY_ENV.contains(k))
            .collect();
        redirected.sort_unstable();
        ensure!(
            redirected.is_empty(),
            "env may not set {}: launched apps always run on the private display",
            redirected.join(", ")
        );
        let cwd = match args.get("cwd") {
            None | Some(Value::Null) => None,
            Some(cwd) => Some(
                cwd.as_str()
                    .filter(|cwd| Path::new(cwd).is_dir())
                    .context("cwd must be an existing directory")?,
            ),
        };
        let wait = match args.get("wait") {
            None => 5.0,
            Some(wait) => wait
                .as_f64()
                .filter(|w| (0.0..=10.0).contains(w))
                .context("wait must be between 0 and 10 seconds")?,
        };
        let detached = args.get("detached") == Some(&json!(true));
        self.start_display(args)?;
        let x11 = if args.get("x11") == Some(&json!(true)) {
            Some(self.ensure_x11()?)
        } else {
            None
        };
        let backend = self.private.as_mut().expect("started");
        let log_path = backend.directory.join(format!(
            "app-{}.log",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        let log = std::fs::File::create(&log_path)?;
        let mut command = Command::new(&argv[0]);
        command.args(&argv[1..]);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        scrub(&mut command);
        command
            .env("WAYLAND_DISPLAY", &backend.wayland_display)
            .env("XDG_SESSION_TYPE", "wayland");
        if let Some(display) = &x11 {
            command.env("DISPLAY", display);
        }
        command
            .envs(overrides)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        detach(&mut command, !detached);
        let app = command
            .spawn()
            .map_err(|error| anyhow!("cannot launch {}: {error}", argv[0]))?;
        let pid = app.id();
        // Detached apps are not killed at teardown; still reap them here.
        self.private_apps.insert(pid, app);
        let deadline = Instant::now() + Duration::from_secs_f64(wait);
        let found = loop {
            let found: Vec<Value> = self
                .private_windows(false)?
                .into_iter()
                .filter(|w| {
                    w["pid"]
                        .as_i64()
                        .is_some_and(|p| descends_from(p, i64::from(pid)))
                        && w["bounds"]["width"]
                            .as_f64()
                            .is_some_and(|width| width > 0.0)
                })
                .collect();
            let exited = !running(self.private_apps.get_mut(&pid).expect("launched"));
            if !found.is_empty() || exited || Instant::now() >= deadline {
                break found;
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let mut result = json!({"pid": pid, "detached": detached,
                                "log": log_path.display().to_string()});
        let status = self
            .private_apps
            .get_mut(&pid)
            .expect("launched")
            .try_wait()?;
        if detached || status.is_some() {
            self.private_apps.remove(&pid);
        }
        result["windows"] = json!(found);
        result["display"] = self.display_status(None)?;
        if let Some(status) = status {
            result["exited"] = json!(status.code().map_or_else(
                || -i64::from(std::os::unix::process::ExitStatusExt::signal(&status).unwrap_or(0)),
                i64::from
            ));
            result["output_tail"] = json!(log_tail(&log_path, 2048));
        }
        Ok(result)
    }

    /// Title -> _NET_WM_PID on the private X server; X11 windows otherwise
    /// report xwayland-satellite's pid.
    fn x11_pids(&mut self) -> HashMap<String, Option<i64>> {
        let backend = self.private.as_mut().expect("running");
        let Some((satellite, display)) = backend.x11.as_mut() else {
            return HashMap::new();
        };
        if !running(satellite) || which("xdotool").is_none() {
            return HashMap::new();
        }
        let display = display.clone();
        let xdotool = |args: &[&str]| {
            let mut command = Command::new("xdotool");
            command.args(args);
            scrub(&mut command);
            command.env("DISPLAY", &display);
            run(command, None, 5).ok()
        };
        let mut pids = HashMap::new();
        let Some(found) = xdotool(&["search", "--onlyvisible", "--name", ""]) else {
            return pids;
        };
        for xid in String::from_utf8_lossy(&found.stdout)
            .split_whitespace()
            .take(256)
        {
            let name = xdotool(&["getwindowname", xid]);
            let pid = xdotool(&["getwindowpid", xid]);
            let title = name
                .map(|n| {
                    String::from_utf8_lossy(&n.stdout)
                        .trim_end_matches('\n')
                        .to_string()
                })
                .unwrap_or_default();
            let pid = pid.filter(|p| p.status.success()).and_then(|p| {
                String::from_utf8_lossy(&p.stdout)
                    .trim()
                    .parse::<i64>()
                    .ok()
            });
            if let Some(pid) = pid {
                let duplicate = pids.contains_key(&title);
                pids.insert(title, (!duplicate).then_some(pid));
            }
        }
        pids
    }

    pub(super) fn private_windows(&mut self, accessibility: bool) -> Result<Vec<Value>> {
        if !self.private_running() {
            return Ok(Vec::new());
        }
        let backend = self.private.as_mut().expect("running");
        let satellite = backend.x11.as_ref().map(|(child, _)| i64::from(child.id()));
        let listed = backend.windows()?;
        let x11 = if satellite.is_some() && listed.iter().any(|w| w["pid"].as_i64() == satellite) {
            self.x11_pids()
        } else {
            HashMap::new()
        };
        let mut result = Vec::new();
        for w in listed {
            let is_x11 = satellite.is_some() && w["pid"].as_i64() == satellite;
            let pid = if is_x11 {
                w["title"]
                    .as_str()
                    .and_then(|title| x11.get(title).copied().flatten())
                    .map_or(Value::Null, |pid| json!(pid))
            } else {
                w["pid"].clone()
            };
            let mut entry = json!({
                "id": format!("{PREFIX}{}", w["id"].as_str().map_or_else(|| w["id"].to_string(), str::to_string)),
                "title": w["title"], "application": w["app_id"], "app_id": w["app_id"],
                "pid": pid, "bounds": w["bounds"], "active": w["focused"], "focused": w["focused"],
                "display": "private", "x11": is_x11,
                "compositor": {"backend": "borg-display", "id": w["id"], "app_id": w["app_id"],
                               "pid": pid, "workspace": null, "output": "BORG-1",
                               "focused": w["focused"], "visible": true, "geometry": w["bounds"]},
            });
            if accessibility {
                let info = PrivateWindow {
                    id: w["id"].clone(),
                    title: w["title"].as_str().unwrap_or_default().to_string(),
                    pid: pid.as_i64(),
                    bounds: w["bounds"].clone(),
                };
                entry["accessible"] = json!(self.private_accessible(&info).is_some());
            }
            result.push(entry);
        }
        Ok(result)
    }

    pub(super) fn private_window(&mut self, window_id: &str) -> Result<PrivateWindow> {
        self.require_private()?;
        for w in self.private_windows(false)? {
            if w["id"] == window_id {
                return Ok(PrivateWindow {
                    id: w["compositor"]["id"].clone(),
                    title: w["title"].as_str().unwrap_or_default().to_string(),
                    pid: w["pid"].as_i64(),
                    bounds: w["bounds"].clone(),
                });
            }
        }
        bail!("stale or unknown private window_id; list_windows with display=private again")
    }

    pub(super) fn private_screenshot(&mut self, args: &Value) -> Result<Value> {
        self.require_private()?;
        let window_id = if args.get("scope") == Some(&json!("window")) {
            args.get("window_id").filter(|id| !id.is_null())
        } else {
            None
        };
        let window = match window_id {
            Some(id) => {
                let id = id
                    .as_str()
                    .filter(|id| id.starts_with(PREFIX))
                    .context("window capture on the private display needs a pd: window_id")?;
                Some(self.private_window(id)?.id)
            }
            None => None,
        };
        let backend = self.private.as_mut().expect("running");
        let path = backend.directory.join(format!(
            "shot-{}.png",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        let mut request = json!({"op": "screenshot", "path": path.display().to_string(),
                                 "cursor": args.get("cursor") == Some(&json!(true))});
        if let Some(window) = window {
            request["window"] = window;
        }
        let shot = backend.call(request)?;
        let data = std::fs::read(&path);
        let _ = std::fs::remove_file(&path);
        let data = data?;
        ensure!(
            data.len() <= super::MAX_IMAGE_BYTES,
            "screenshot exceeds 4 MiB; use a smaller private display or window capture"
        );
        let mut result = json!({
            "scope": if window_id.is_some() { "window" } else { "desktop" },
            "display": "private", "width": shot["width"], "height": shot["height"],
            "coordinate_space": if window_id.is_some() {
                "window pixels; pass coordinate_space=window to pointer ops"
            } else {
                "private display pixels; use these x,y for pointer ops on pd: windows"
            },
            "borg_attachments": attachment(&data),
        });
        if let Some(id) = window_id {
            result["window_id"] = id.clone();
        }
        if let Some(cursor) = shot.get("cursor") {
            result["cursor"] = cursor.clone();
        }
        Ok(result)
    }

    /// A private display pixel: an observed element's centre or x,y (display
    /// or window pixels), and whether it came from coordinates.
    fn private_point(
        &mut self,
        args: &Value,
        info: &PrivateWindow,
        accessible: bool,
        keys: (&str, &str),
    ) -> Result<((f64, f64), bool)> {
        let origin = (
            info.bounds["x"].as_f64().unwrap_or(0.0),
            info.bounds["y"].as_f64().unwrap_or(0.0),
        );
        if args.get("element_id").is_some_and(|e| !e.is_null()) && keys == ("x", "y") {
            ensure!(
                accessible,
                "this private window has no accessibility tree; use x,y from a private screenshot"
            );
            let (_, obj) = self.target(args)?;
            let a11y = self.a11y()?;
            let (x, y, width, height) = a11y.extents(&obj, atspi::CoordType::Window)?;
            let showing = a11y
                .state(&obj)
                .is_ok_and(|s| s.contains(atspi::State::Showing));
            ensure!(
                width > 0 && height > 0 && showing,
                "element has no on-screen bounds"
            );
            return Ok((
                (
                    origin.0 + f64::from(x) + f64::from(width) / 2.0,
                    origin.1 + f64::from(y) + f64::from(height) / 2.0,
                ),
                false,
            ));
        }
        let (Some(x), Some(y)) = (number(args.get(keys.0)), number(args.get(keys.1))) else {
            bail!(
                "pointer ops need element_id + observation_id or {} + {}",
                keys.0,
                keys.1
            );
        };
        match args.get("coordinate_space").map(|s| s.as_str()) {
            None | Some(Some("desktop")) => Ok(((x, y), true)),
            Some(Some("window")) => Ok(((x + origin.0, y + origin.1), true)),
            _ => bail!("coordinate_space must be \"desktop\" or \"window\""),
        }
    }

    pub(super) fn private_inject(&mut self, args: &Value) -> Result<Value> {
        let op = args["op"].as_str().unwrap_or_default().to_string();
        let wid = window_id(args)?;
        self.require_private()?;
        let info = self.private_window(&wid)?;
        let accessible = self.private_accessible(&info);
        let mut extra = json!({"display": "private"});
        // Element targets are validated against the observation before it is consumed.
        let point = if matches!(op.as_str(), "pointer_click" | "scroll") {
            Some(self.private_point(args, &info, accessible.is_some(), ("x", "y"))?)
        } else {
            None
        };
        self.observations.remove(&wid);
        match op.as_str() {
            "type_text" => {
                let text = args
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|t| t.chars().count() <= 16384)
                    .context("text must be a string of at most 16384 characters")?;
                let backend = self.require_private()?;
                backend.focus(&info.id)?;
                backend.call(json!({"op": "type", "text": text}))?;
            }
            "key" => {
                let (modifiers, key) = parse_keys(args.get("keys"))?;
                let hold = super::bounded_int(args, "hold_ms", 0, 0, 10_000)?;
                let backend = self.require_private()?;
                backend.focus(&info.id)?;
                for code in &modifiers {
                    backend.key(*code, true)?;
                }
                backend.key(key, true)?;
                std::thread::sleep(Duration::from_millis(hold as u64));
                backend.key(key, false)?;
                for code in modifiers.iter().rev() {
                    backend.key(*code, false)?;
                }
                extra["keys"] = args["keys"].clone();
            }
            "pointer_move" => {
                self.private_pointer_move(args, &info, accessible.is_some(), &mut extra)?
            }
            "pointer_click" | "scroll" => {
                let ((x, y), coordinate) = point.expect("resolved above");
                let backend = self.require_private()?;
                backend.focus(&info.id)?;
                backend.pointer_move(x, y)?;
                merge(
                    &mut extra,
                    json!({"coordinate_click": coordinate, "point": {"x": x, "y": y}}),
                );
                if op == "pointer_click" {
                    let count = args.get("count").map_or(Some(1), Value::as_i64);
                    ensure!(matches!(count, Some(1 | 2)), "count must be 1 or 2");
                    let button = button_code(args.get("button"))?;
                    let backend = self.require_private()?;
                    for _ in 0..count.unwrap_or(1) {
                        backend.button(button, true)?;
                        backend.button(button, false)?;
                        std::thread::sleep(Duration::from_millis(30));
                    }
                } else {
                    let (dx, dy) = (arg_or_zero(args, "dx"), arg_or_zero(args, "dy"));
                    let (Some(dx), Some(dy)) = (dx, dy) else {
                        bail!("scroll distance is limited to 10000 pixels");
                    };
                    ensure!(
                        dx.abs() <= 10_000.0 && dy.abs() <= 10_000.0,
                        "scroll distance is limited to 10000 pixels"
                    );
                    let (nx, ny) = (notches(dx), notches(dy));
                    // Positive dy scrolls content down.
                    self.require_private()?.axis(nx, ny)?;
                    merge(
                        &mut extra,
                        json!({"units": "wheel notches of about 120 pixels",
                               "notches": {"dx": nx, "dy": ny}}),
                    );
                }
                self.private_pointer = Some(info.id.to_string());
            }
            "drag" => {
                let accessible = accessible.is_some();
                let ((fx, fy), _) =
                    self.private_point(args, &info, accessible, ("from_x", "from_y"))?;
                let ((tx, ty), _) =
                    self.private_point(args, &info, accessible, ("to_x", "to_y"))?;
                let button = button_code(args.get("button"))?;
                let backend = self.require_private()?;
                backend.focus(&info.id)?;
                backend.pointer_move(fx, fy)?;
                backend.button(button, true)?;
                for step in 1..=12 {
                    let t = f64::from(step) / 12.0;
                    backend.pointer_move(fx + (tx - fx) * t, fy + (ty - fy) * t)?;
                    std::thread::sleep(Duration::from_millis(20));
                }
                backend.button(button, false)?;
                merge(
                    &mut extra,
                    json!({"from": {"x": fx, "y": fy}, "to": {"x": tx, "y": ty}}),
                );
            }
            _ => bail!("unsupported operation: {op}"),
        }
        std::thread::sleep(Duration::from_millis(100));
        let open = self.require_private()?.windows()?.iter().any(|w| {
            format!(
                "{PREFIX}{}",
                w["id"]
                    .as_str()
                    .map_or_else(|| w["id"].to_string(), str::to_string)
            ) == wid
        });
        let mut result = if !open {
            json!({"window_id": wid, "action": op, "dispatched": true, "window_closed": true,
                   "verification": "The window closed after the action; list_windows with display=private."})
        } else if let Some(obj) = accessible {
            return self.settle_and_snapshot(&super::Win::Accessible(obj), &wid, &op, extra);
        } else {
            json!({"window_id": wid, "action": op, "dispatched": true, "accessible": false,
                   "verification": "This window exposes no accessibility tree; take a private screenshot to verify."})
        };
        merge(&mut result, extra);
        Ok(result)
    }

    fn private_pointer_move(
        &mut self,
        args: &Value,
        info: &PrivateWindow,
        accessible: bool,
        extra: &mut Value,
    ) -> Result<()> {
        let (Some(dx), Some(dy)) = (arg_or_zero(args, "dx"), arg_or_zero(args, "dy")) else {
            bail!("pointer_move dx/dy are limited to 10000 pixels");
        };
        ensure!(
            dx.abs() <= 10_000.0 && dy.abs() <= 10_000.0,
            "pointer_move dx/dy are limited to 10000 pixels"
        );
        let steps = super::bounded_int(args, "steps", 1, 1, 1000)?;
        let duration = super::bounded_int(args, "duration_ms", 0, 0, 10_000)?;
        let held = match args.get("hold_keys") {
            None | Some(Value::Null) => None,
            Some(keys) => Some(parse_keys(Some(keys))?),
        };
        self.require_private()?.focus(&info.id)?;
        // Motion reaches only the surface under the pointer: enter the window
        // (at x, y when given, else its centre) before moving relatively.
        let given = args.get("x").is_some_and(|v| !v.is_null())
            || args.get("y").is_some_and(|v| !v.is_null());
        let place = if given {
            Some(self.private_point(args, info, accessible, ("x", "y"))?.0)
        } else if self.private_pointer.as_ref() != Some(&info.id.to_string()) {
            let b = &info.bounds;
            let n = |key: &str| b[key].as_f64().unwrap_or(0.0);
            Some((n("x") + n("width") / 2.0, n("y") + n("height") / 2.0))
        } else {
            None
        };
        let backend = self.require_private()?;
        if let Some((px, py)) = place {
            backend.pointer_move(px, py)?;
            extra["placement"] = json!({"x": px, "y": py});
            // Games lock the pointer only once it enters them; motion sent
            // before the lock reaches no relative-pointer object and is lost.
            let deadline = Instant::now() + Duration::from_millis(300);
            while Instant::now() < deadline && backend.info()?["pointer_constraint"].is_null() {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let codes: Vec<u16> = held
            .map(|(modifiers, key)| modifiers.into_iter().chain([key]).collect())
            .unwrap_or_default();
        for code in &codes {
            backend.key(*code, true)?;
        }
        let pause = Duration::from_secs_f64(duration as f64 / 1000.0 / steps as f64);
        let moved = (|| -> Result<()> {
            for _ in 0..steps {
                backend.pointer_relative(dx / steps as f64, dy / steps as f64)?;
                std::thread::sleep(pause);
            }
            Ok(())
        })();
        for code in codes.iter().rev() {
            backend.key(*code, false)?;
        }
        moved?;
        let constraint = backend.info()?["pointer_constraint"].clone();
        if place.is_some() {
            self.private_pointer = Some(info.id.to_string());
        }
        merge(
            extra,
            json!({"relative": {"dx": dx, "dy": dy}, "steps": steps,
                   "hold_keys": args.get("hold_keys"), "pointer_constraint": constraint}),
        );
        Ok(())
    }

    pub(super) fn private_capabilities(&mut self) -> Result<Value> {
        let binary = BorgDisplay::binary();
        let mut render_nodes: Vec<String> = std::fs::read_dir("/dev/dri")
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("renderD"))
            })
            .map(|p| p.display().to_string())
            .collect();
        render_nodes.sort();
        let mut status = json!({
            "available": binary.is_some() && std::env::var_os("XDG_RUNTIME_DIR").is_some_and(|d| !d.is_empty()),
            "backend": "borg-display (Borg-owned headless Wayland compositor)",
            "binary": binary.as_ref().map(|b| b.display().to_string()),
            "render_nodes": render_nodes,
            "x11": which("xwayland-satellite").map(|_| "xwayland-satellite"),
            "input_backend": "private seat through the display's control socket (never uinput or the user's focus)",
            "operations": ["start_display", "stop_display", "attach_display", "launch", "list_windows", "observe", "screenshot",
                           "click", "set_value", "type_text", "key", "pointer_click", "pointer_move", "scroll", "drag"],
            "gpu_accelerated": null,
            "gpu_note": "Measured when the display starts: true means hardware EGL on a render node plus dmabuf for Vulkan/GL clients; false means Mesa software rendering.",
            "detected_alternative_compositors": ALTERNATIVE_BACKENDS.iter().filter(|n| which(n).is_some()).collect::<Vec<_>>(),
            "limitations": [
                "Private apps share the user's session D-Bus and accessibility bus; single-instance apps that are already running on the desktop (browsers, some terminals) may open their window there instead, so launch a separate instance or profile.",
                "type_text types any Unicode text; characters outside the default layout go through a temporary keymap.",
                "Games and editor viewports can lock or confine the pointer (pointer-constraints); pointer_move dx/dy then arrive as exact relative motion while the pointer stays put.",
                "GTK4 reports element extents as 0,0; prefer semantic click/set_value or x,y from a private screenshot.",
                "Screenshots include the pointer only with cursor=true. X11 apps need launch x11=true and xwayland-satellite.",
                "Detached apps are not killed at teardown but lose their display when it stops.",
            ],
        });
        if binary.is_none() {
            status["reason"] = json!(DISPLAY_MISSING);
        }
        if self.private_running() {
            let running = self.display_status(None)?;
            merge(&mut status, running);
        }
        Ok(status)
    }
}
