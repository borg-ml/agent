//! Input injection: a Borg-owned evdev uinput device (keys, buttons, wheel,
//! absolute pointer), a separate relative mouse for pointer-locked games, and
//! wtype (Wayland) or xdotool (X11) for Unicode text.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use evdev::uinput::VirtualDevice;
use evdev::{
    AbsInfo, AbsoluteAxisCode, AttributeSet, BusType, EventType, InputEvent, InputId, KeyCode,
    RelativeAxisCode, UinputAbsSetup,
};
use serde_json::{Value, json};

use super::a11y::{Obj, extents_type, merge};
use super::capture::{grim_region, refreshed};
use super::windows::{self, CompositorWindow, Rect};
use super::{
    Helper, LocatedWindow, Win, arg_or_zero, bounded_int, command, g, number, py_round, run,
    run_text, session_type, stderr_text, typing_tool, which, window_id,
};

const ABS_MAX: i32 = 65535;

/// Linux input event codes by evdev name.
pub(super) fn code(name: &str) -> Option<u16> {
    let fixed = match name {
        "BTN_LEFT" => 272,
        "BTN_RIGHT" => 273,
        "BTN_MIDDLE" => 274,
        "KEY_ESC" => 1,
        "KEY_MINUS" => 12,
        "KEY_EQUAL" => 13,
        "KEY_BACKSPACE" => 14,
        "KEY_TAB" => 15,
        "KEY_LEFTBRACE" => 26,
        "KEY_RIGHTBRACE" => 27,
        "KEY_ENTER" => 28,
        "KEY_LEFTCTRL" => 29,
        "KEY_SEMICOLON" => 39,
        "KEY_APOSTROPHE" => 40,
        "KEY_GRAVE" => 41,
        "KEY_LEFTSHIFT" => 42,
        "KEY_BACKSLASH" => 43,
        "KEY_COMMA" => 51,
        "KEY_DOT" => 52,
        "KEY_SLASH" => 53,
        "KEY_LEFTALT" => 56,
        "KEY_SPACE" => 57,
        "KEY_F11" => 87,
        "KEY_F12" => 88,
        "KEY_HOME" => 102,
        "KEY_UP" => 103,
        "KEY_PAGEUP" => 104,
        "KEY_LEFT" => 105,
        "KEY_RIGHT" => 106,
        "KEY_END" => 107,
        "KEY_DOWN" => 108,
        "KEY_PAGEDOWN" => 109,
        "KEY_INSERT" => 110,
        "KEY_DELETE" => 111,
        "KEY_LEFTMETA" => 125,
        "KEY_0" => 11,
        _ => 0,
    };
    if fixed != 0 {
        return Some(fixed);
    }
    let rest = name.strip_prefix("KEY_")?;
    if let Some(n) = rest.strip_prefix('F').and_then(|n| n.parse::<u16>().ok())
        && (1..=10).contains(&n)
    {
        return Some(58 + n);
    }
    let mut chars = rest.chars();
    let (Some(c), None) = (chars.next(), chars.next()) else {
        return None;
    };
    if let Some(digit) = c.to_digit(10).filter(|d| (1..=9).contains(d)) {
        return Some(1 + digit as u16);
    }
    for (row, first) in [("QWERTYUIOP", 16), ("ASDFGHJKL", 30), ("ZXCVBNM", 44)] {
        if let Some(index) = row.find(c) {
            return Some(first + index as u16);
        }
    }
    None
}

fn modifier(part: &str) -> Option<&'static str> {
    Some(match part {
        "ctrl" | "control" => "KEY_LEFTCTRL",
        "alt" | "option" | "opt" => "KEY_LEFTALT",
        "shift" => "KEY_LEFTSHIFT",
        "cmd" | "command" | "meta" | "super" | "win" => "KEY_LEFTMETA",
        _ => return None,
    })
}

fn key_name(part: &str) -> Option<String> {
    let mut chars = part.chars();
    if let (Some(c), None) = (chars.next(), chars.next())
        && (c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return Some(format!("KEY_{}", c.to_ascii_uppercase()));
    }
    if let Some(n) = part.strip_prefix('f').and_then(|n| n.parse::<u16>().ok())
        && (1..=12).contains(&n)
        && part == format!("f{n}")
    {
        return Some(format!("KEY_F{n}"));
    }
    Some(
        match part {
            "-" => "KEY_MINUS",
            "=" => "KEY_EQUAL",
            "[" => "KEY_LEFTBRACE",
            "]" => "KEY_RIGHTBRACE",
            ";" => "KEY_SEMICOLON",
            "'" => "KEY_APOSTROPHE",
            "`" => "KEY_GRAVE",
            "\\" => "KEY_BACKSLASH",
            "," => "KEY_COMMA",
            "." => "KEY_DOT",
            "/" => "KEY_SLASH",
            "return" | "enter" => "KEY_ENTER",
            "tab" => "KEY_TAB",
            "space" => "KEY_SPACE",
            "backspace" | "delete" => "KEY_BACKSPACE",
            "forwarddelete" => "KEY_DELETE",
            "escape" | "esc" => "KEY_ESC",
            "home" => "KEY_HOME",
            "end" => "KEY_END",
            "pageup" => "KEY_PAGEUP",
            "pagedown" => "KEY_PAGEDOWN",
            "left" => "KEY_LEFT",
            "right" => "KEY_RIGHT",
            "up" => "KEY_UP",
            "down" => "KEY_DOWN",
            "insert" => "KEY_INSERT",
            _ => return None,
        }
        .to_string(),
    )
}

/// 'ctrl+shift+t' -> (modifier codes, key code); one non-modifier key per
/// call. A bare modifier ('shift') is itself the key, so it can be held.
pub(super) fn parse_keys(spec: Option<&Value>) -> Result<(Vec<u16>, u16)> {
    let spec = spec
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .context("keys is required")?;
    let mut modifiers = Vec::new();
    let mut key = None;
    for part in spec.to_lowercase().split('+').map(str::trim) {
        if let Some(name) = modifier(part) {
            let code = code(name).expect("modifier code");
            if !modifiers.contains(&code) {
                modifiers.push(code);
            }
        } else if let (Some(name), None) = (key_name(part), key) {
            key = code(&name);
        } else {
            bail!("unsupported key \"{part}\"; use one non-modifier key per call");
        }
    }
    let key = match key {
        Some(key) => key,
        None => modifiers.pop().context("keys must name a key")?,
    };
    Ok((modifiers, key))
}

fn all_key_codes() -> Vec<u16> {
    let mut names: Vec<String> = "abcdefghijklmnopqrstuvwxyz0123456789"
        .chars()
        .map(|c| format!("KEY_{}", c.to_ascii_uppercase()))
        .collect();
    names.extend((1..=12).map(|n| format!("KEY_F{n}")));
    names.extend(
        [
            "KEY_MINUS",
            "KEY_EQUAL",
            "KEY_LEFTBRACE",
            "KEY_RIGHTBRACE",
            "KEY_SEMICOLON",
            "KEY_APOSTROPHE",
            "KEY_GRAVE",
            "KEY_BACKSLASH",
            "KEY_COMMA",
            "KEY_DOT",
            "KEY_SLASH",
            "KEY_ENTER",
            "KEY_TAB",
            "KEY_SPACE",
            "KEY_BACKSPACE",
            "KEY_DELETE",
            "KEY_ESC",
            "KEY_HOME",
            "KEY_END",
            "KEY_PAGEUP",
            "KEY_PAGEDOWN",
            "KEY_LEFT",
            "KEY_RIGHT",
            "KEY_UP",
            "KEY_DOWN",
            "KEY_INSERT",
            "KEY_LEFTCTRL",
            "KEY_LEFTALT",
            "KEY_LEFTSHIFT",
            "KEY_LEFTMETA",
            "BTN_LEFT",
            "BTN_RIGHT",
            "BTN_MIDDLE",
        ]
        .map(str::to_string),
    );
    let mut codes: Vec<u16> = names.iter().filter_map(|name| code(name)).collect();
    codes.sort_unstable();
    codes.dedup();
    codes
}

/// Unmet prerequisites for injection, as human-readable strings.
pub(super) fn input_requirements() -> Vec<String> {
    let mut missing = Vec::new();
    let path = c"/dev/uinput";
    // SAFETY: a valid NUL-terminated path.
    if unsafe { libc::access(path.as_ptr(), libc::W_OK) } != 0 {
        missing.push(
            "/dev/uinput is not writable (add the user to the input group or install a udev rule)"
                .into(),
        );
    }
    match typing_tool() {
        None => missing.push("no Wayland or X11 display session".into()),
        Some(tool) if which(tool).is_none() => {
            missing.push(format!("{tool} is not installed (needed for type_text)"));
        }
        Some(_) => {}
    }
    missing
}

fn id(product: u16) -> InputId {
    InputId::new(BusType::BUS_USB, 0x1209, product, 1)
}

/// Map a desktop pixel to the uinput axis so libinput lands on its centre.
fn abs_coordinate(pixel: f64, extent: u32) -> i32 {
    py_round((pixel + 0.5) * f64::from(ABS_MAX + 1) / f64::from(extent))
        .clamp(0, i64::from(ABS_MAX)) as i32
}

/// Wheel notches for a pixel distance: at least one for any non-zero request.
pub(super) fn notches(pixels: f64) -> i64 {
    if pixels == 0.0 {
        return 0;
    }
    py_round(pixels.abs() / 120.0).max(1) * pixels.signum() as i64
}

pub(super) fn button_code(name: Option<&Value>) -> Result<u16> {
    let name = match name {
        None | Some(Value::Null) => "left",
        Some(value) => value.as_str().unwrap_or_default(),
    };
    match name {
        "left" => Ok(272),
        "right" => Ok(273),
        "middle" => Ok(274),
        _ => bail!("button must be left, right or middle"),
    }
}

fn coordinate_space(args: &Value) -> Result<&'static str> {
    match args.get("coordinate_space") {
        None => Ok("desktop"),
        Some(value) => match value.as_str() {
            Some("desktop") => Ok("desktop"),
            Some("window") => Ok("window"),
            _ => bail!("coordinate_space must be \"desktop\" or \"window\""),
        },
    }
}

pub(super) fn compositor_focus(entry: &CompositorWindow) -> Result<()> {
    let native = &entry.native_id;
    let text = native
        .as_str()
        .map_or_else(|| native.to_string(), str::to_string);
    match entry.backend {
        "niri" => {
            windows::niri_request(json!({"Action": {"FocusWindow": {"id": native}}}))?;
        }
        "sway" => {
            run_text(&["swaymsg", &format!("[con_id={text}]"), "focus"], 3)?;
        }
        "hyprland" => {
            run_text(
                &[
                    "hyprctl",
                    "dispatch",
                    "focuswindow",
                    &format!("address:{text}"),
                ],
                3,
            )?;
        }
        "x11" => {
            run_text(&["xdotool", "windowactivate", "--sync", &text], 3)?;
        }
        backend => bail!("the {backend} window backend cannot focus windows"),
    }
    Ok(())
}

fn focused_entry(backend: &str) -> Result<Option<CompositorWindow>> {
    Ok(windows::compositor_list(Some(backend))?
        .into_iter()
        .find(|c| c.focused))
}

fn type_text(text: &str) -> Result<()> {
    let tool = typing_tool();
    let argv: Vec<&str> = if tool == Some("wtype") {
        vec!["wtype", "-d", "5", "--", text]
    } else {
        vec!["xdotool", "type", "--delay", "5", "--file", "/dev/stdin"]
    };
    if which(argv[0]).is_none() {
        let missing = input_requirements();
        let detail = if missing.is_empty() {
            format!("{} is not installed", argv[0])
        } else {
            missing.join("; ")
        };
        bail!("input injection unavailable: {detail}");
    }
    let input = (tool != Some("wtype")).then_some(text.as_bytes());
    let result = run(command(&argv), input, 60)?;
    if !result.status.success() {
        bail!(
            "{} failed: {}",
            tool.unwrap_or(argv[0]),
            stderr_text(&result, 1024)
        );
    }
    Ok(())
}

/// What a pointer op points at, resolved to desktop pixels only after the
/// window is focused.
enum Spec {
    Element(Obj),
    Point(&'static str, f64, f64),
}

impl Helper {
    fn input_device(&mut self) -> Result<&mut VirtualDevice> {
        if self.input_device.is_none() {
            let missing = input_requirements();
            ensure!(
                missing.is_empty(),
                "input injection unavailable: {}",
                missing.join("; ")
            );
            let mut keys = AttributeSet::<KeyCode>::new();
            for code in all_key_codes() {
                keys.insert(KeyCode::new(code));
            }
            let mut wheels = AttributeSet::<RelativeAxisCode>::new();
            wheels.insert(RelativeAxisCode::REL_WHEEL);
            wheels.insert(RelativeAxisCode::REL_HWHEEL);
            let axis = AbsInfo::new(0, 0, ABS_MAX, 0, 0, 0);
            let device = VirtualDevice::builder()
                .and_then(|b| {
                    b.name("Borg virtual input")
                        .input_id(id(0xb0b6))
                        .with_keys(&keys)
                })
                .and_then(|b| b.with_relative_axes(&wheels))
                .and_then(|b| {
                    b.with_absolute_axis(&UinputAbsSetup::new(AbsoluteAxisCode::ABS_X, axis))
                })
                .and_then(|b| {
                    b.with_absolute_axis(&UinputAbsSetup::new(AbsoluteAxisCode::ABS_Y, axis))
                })
                .and_then(|b| b.build())
                .map_err(|error| anyhow!("cannot create the uinput device: {error}"))?;
            // Let the compositor's libinput pick the new device up.
            std::thread::sleep(Duration::from_millis(500));
            self.input_device = Some(device);
        }
        Ok(self.input_device.as_mut().expect("created above"))
    }

    /// A separate relative mouse: libinput classifies it as an ordinary mouse,
    /// so games with pointer lock receive the motion as relative deltas.
    fn relative_device(&mut self) -> Result<()> {
        if self.relative_device.is_some() {
            return Ok(());
        }
        let missing: Vec<String> = input_requirements()
            .into_iter()
            .filter(|m| !m.contains("type_text"))
            .collect();
        ensure!(
            missing.is_empty(),
            "input injection unavailable: {}",
            missing.join("; ")
        );
        let mut buttons = AttributeSet::<KeyCode>::new();
        for code in [272, 273, 274] {
            buttons.insert(KeyCode::new(code));
        }
        let mut axes = AttributeSet::<RelativeAxisCode>::new();
        for axis in [
            RelativeAxisCode::REL_X,
            RelativeAxisCode::REL_Y,
            RelativeAxisCode::REL_WHEEL,
            RelativeAxisCode::REL_HWHEEL,
        ] {
            axes.insert(axis);
        }
        let device = VirtualDevice::builder()
            .and_then(|b| {
                b.name("Borg virtual mouse")
                    .input_id(id(0xb0b7))
                    .with_keys(&buttons)
            })
            .and_then(|b| b.with_relative_axes(&axes))
            .and_then(|b| b.build())
            .map_err(|error| anyhow!("cannot create the uinput mouse device: {error}"))?;
        std::thread::sleep(Duration::from_millis(500));
        self.relative_device = Some(device);
        Ok(())
    }

    fn emit(&mut self, kind: EventType, code: u16, value: i32) -> Result<()> {
        self.input_device()?
            .emit(&[InputEvent::new(kind.0, code, value)])?;
        Ok(())
    }

    fn key(&mut self, code: u16, value: i32) -> Result<()> {
        self.emit(EventType::KEY, code, value)
    }

    fn press(&mut self, modifiers: &[u16], key: u16) -> Result<()> {
        for code in modifiers {
            self.key(*code, 1)?;
        }
        self.key(key, 1)
    }

    fn release(&mut self, modifiers: &[u16], key: u16) -> Result<()> {
        self.key(key, 0)?;
        for code in modifiers.iter().rev() {
            self.key(*code, 0)?;
        }
        Ok(())
    }

    fn screen_size(&mut self) -> Result<(u32, u32)> {
        if let Some(screen) = self.screen {
            return Ok(screen);
        }
        match session_type() {
            Some("wayland") if which("grim").is_some() => {
                if let Ok(probe) = run(command(&["grim", "-"]), None, 5)
                    && probe.status.success()
                {
                    self.screen = super::capture::png_size(&probe.stdout).ok();
                }
            }
            Some("x11") if which("xdotool").is_some() => {
                if let Ok(text) = run_text(&["xdotool", "getdisplaygeometry"], 5) {
                    let parts: Vec<u32> = text
                        .split_whitespace()
                        .filter_map(|p| p.parse().ok())
                        .collect();
                    if let [width, height] = parts[..] {
                        self.screen = Some((width, height));
                    }
                }
            }
            _ => {}
        }
        self.screen.context(
            "cannot determine the desktop size for pointer mapping; take a desktop screenshot first",
        )
    }

    fn move_pointer(&mut self, x: f64, y: f64) -> Result<()> {
        let (width, height) = self.screen_size()?;
        ensure!(
            (0.0..f64::from(width)).contains(&x) && (0.0..f64::from(height)).contains(&y),
            "point ({}, {}) is outside the {width}x{height} desktop",
            g(x),
            g(y)
        );
        self.emit(
            EventType::ABSOLUTE,
            AbsoluteAxisCode::ABS_X.0,
            abs_coordinate(x, width),
        )?;
        self.emit(
            EventType::ABSOLUTE,
            AbsoluteAxisCode::ABS_Y.0,
            abs_coordinate(y, height),
        )?;
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    }

    fn click_button(&mut self, code: u16, count: i64) -> Result<()> {
        for _ in 0..count {
            self.key(code, 1)?;
            std::thread::sleep(Duration::from_millis(30));
            self.key(code, 0)?;
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    /// Injected events go to the focused window: focus the target (through
    /// the compositor when it lists the window) and refuse unless it became
    /// active. Remembers the human's window from before Borg's first focus
    /// change, for restore_focus.
    fn ensure_active(&mut self, win: &Win, wid: &str) -> Result<()> {
        let entry = match win {
            Win::Compositor(entry) => Some((**entry).clone()),
            Win::Accessible(_) => self.compositor_for(wid)?,
        };
        if let Some(entry) = entry {
            let current = focused_entry(entry.backend)?;
            if let Some(current) = &current
                && self.focus.borg.as_deref() != Some(current.id.as_str())
            {
                // Focus moved since Borg last focused: remember the human's window.
                self.focus.human = Some(current.clone());
            }
            if current.as_ref().is_none_or(|c| c.id != entry.id) {
                compositor_focus(&entry)?;
                self.focus.borg = Some(entry.id.clone());
                self.focus.changed_at = Instant::now();
                let mut focused = false;
                for _ in 0..40 {
                    if focused_entry(entry.backend)?.is_some_and(|c| c.id == entry.id) {
                        focused = true;
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                ensure!(
                    focused,
                    "the compositor did not focus the target window; injected input would reach another window"
                );
                // Let the client see keyboard focus before input arrives.
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        let Win::Accessible(obj) = win else {
            return Ok(());
        };
        let a11y = self.a11y()?;
        if a11y.active(obj) {
            return Ok(());
        }
        let _ = a11y.grab_focus(obj);
        for _ in 0..10 {
            std::thread::sleep(Duration::from_millis(50));
            if a11y.active(obj) {
                return Ok(());
            }
        }
        bail!(
            "target window is not active and could not be raised; injected input would reach the focused window, so activate it first"
        )
    }

    /// Give focus back to the window the human had before Borg moved focus.
    pub(super) fn restore_focus(&mut self, args: &Value, wid: &str) -> Value {
        let Some(human) = self.focus.human.clone() else {
            return Value::Null;
        };
        if !args.get("restore_focus").is_some_and(super::a11y::truthy) || human.id == wid {
            return Value::Null;
        }
        match compositor_focus(&human) {
            Ok(()) => {
                self.focus.human = None;
                self.focus.borg = None;
                json!(human.id)
            }
            Err(error) => json!(format!("failed: {error:#}")),
        }
    }

    fn pointer_target(&mut self, args: &Value) -> Result<(Win, Spec)> {
        let wid = window_id(args)?;
        if args.get("element_id").is_some_and(|e| !e.is_null()) {
            let (win, obj) = self.target(args)?;
            self.observations.remove(&wid);
            return Ok((win, Spec::Element(obj)));
        }
        let win = self.window(&wid)?;
        let (Some(x), Some(y)) = (number(args.get("x")), number(args.get("y"))) else {
            bail!("pointer ops need element_id + observation_id or x + y");
        };
        self.observations.remove(&wid);
        Ok((win, Spec::Point(coordinate_space(args)?, x, y)))
    }

    /// Right before pressing, check the window has not moved since it was located.
    fn confirm_origin(&mut self, wid: &str, mapping: &Value) -> Result<()> {
        let entry = self.compositor_for(wid)?;
        let (Some(entry), Some(located)) = (entry, self.located.get(wid)) else {
            return Ok(());
        };
        if mapping.get("window_origin").is_none() || entry.backend != "niri" {
            return Ok(());
        }
        let origin = located.origin;
        let moved = "the window moved while it was being targeted; nothing was clicked, retry once it settles";
        if located.geometry {
            let geometry = refreshed(&entry)?.geometry;
            ensure!(geometry.is_some_and(|g| (g.x, g.y) == origin), moved);
            return Ok(());
        }
        let image = located.image.as_ref().map(|(rgb, alpha)| {
            (
                windows::Rgb {
                    width: rgb.width,
                    height: rgb.height,
                    data: rgb.data.clone(),
                },
                alpha.clone(),
            )
        });
        let (here, _) = self.locate_on_desktop(&entry, image.as_ref())?;
        ensure!(here.is_some_and(|h| (h.x, h.y) == origin), moved);
        Ok(())
    }

    /// Whether window screenshot pixels start at the compositor geometry (no
    /// shadows or popups widening the capture), so geometry maps them exactly.
    fn geometry_matches_capture(&self, wid: &str, entry: &CompositorWindow) -> bool {
        if entry.geometry.is_none() {
            return false;
        }
        match self.window_captures.get(wid) {
            None => true,
            Some(capture) => {
                entry.size == Some((i64::from(capture.size.0), i64::from(capture.size.1)))
            }
        }
    }

    /// After Borg focused a window its workspace may still be sliding into
    /// view: wait until two region grabs match, up to 1.5 s.
    fn settle_geometry(&self, entry: &CompositorWindow) -> Result<CompositorWindow> {
        let mut entry = entry.clone();
        let mut previous = None;
        while self.focus.changed_at.elapsed() < Duration::from_millis(1500) {
            entry = refreshed(&entry)?;
            let Some(geometry) = entry.geometry else {
                break;
            };
            let frame = grim_region(&geometry, entry.scale)?;
            if previous.as_ref() == Some(&frame)
                && self.focus.changed_at.elapsed() >= Duration::from_millis(300)
            {
                break;
            }
            previous = Some(frame);
            std::thread::sleep(Duration::from_millis(80));
        }
        let entry = refreshed(&entry)?;
        ensure!(
            entry.geometry.is_some(),
            "the window is no longer floating on a visible workspace"
        );
        Ok(entry)
    }

    /// Desktop pixel of the top-left corner of this window's screenshot image.
    ///
    /// Compositors with absolute geometry (sway, Hyprland, X11, niri floating)
    /// answer directly. niri does not expose the scroll position of tiled
    /// windows, so the helper captures the window and the desktop together
    /// and finds the window's pixels on the desktop; ambiguous or unmatched
    /// content is refused rather than guessed.
    fn desktop_origin(
        &mut self,
        wid: &str,
        entry: &CompositorWindow,
        element: bool,
    ) -> Result<((i64, i64), Value)> {
        let entry = refreshed(entry)?;
        if entry.backend != "niri" {
            if let Some(geometry) = entry.geometry
                && entry.visible != Some(false)
            {
                return Ok((
                    (geometry.x, geometry.y),
                    json!({"method": "compositor geometry"}),
                ));
            }
            bail!("the compositor reports no on-screen geometry for this window");
        }
        if (entry.geometry.is_some() && element) || self.geometry_matches_capture(wid, &entry) {
            // Floating window: niri IPC gives exact placement. Wait out a
            // workspace switch without extra captures, then use it.
            let entry = self.settle_geometry(&entry)?;
            let geometry = entry.geometry.expect("settled geometry");
            let origin = (geometry.x, geometry.y);
            self.located.insert(
                wid.to_string(),
                LocatedWindow {
                    image: None,
                    origin,
                    geometry: true,
                },
            );
            return Ok((
                origin,
                json!({"method": "niri floating-window geometry (IPC)"}),
            ));
        }
        ensure!(
            entry.visible != Some(false),
            "window is not on a visible workspace"
        );
        let mut window_image: Option<super::capture::Decoded> = None;
        let mut raw: Option<Vec<u8>> = None;
        let mut seen: Option<(i64, i64)> = None;
        let mut since = Instant::now();
        let mut frames = 0;
        for attempt in 0..14 {
            // Refresh the window pixels now and then.
            let refresh = window_image.is_none() || attempt % 5 == 4;
            let (here, fresh) =
                self.locate_on_desktop(&entry, if refresh { None } else { window_image.as_ref() })?;
            if let Some((decoded, image)) = fresh {
                window_image = Some(decoded);
                raw = Some(image);
            }
            // A focus change starts workspace/column animations: only trust a
            // position that holds over several frames spanning 300 ms.
            match here {
                Some(here) if seen == Some((here.x, here.y)) => {
                    frames += 1;
                    if frames >= 3 && since.elapsed() >= Duration::from_millis(300) {
                        let origin = (here.x, here.y);
                        self.located.insert(
                            wid.to_string(),
                            LocatedWindow {
                                image: window_image.take(),
                                origin,
                                geometry: false,
                            },
                        );
                        return Ok((
                            origin,
                            json!({"x": here.x, "y": here.y, "agreement": here.agreement,
                                   "votes": here.votes,
                                   "method": "matched the window capture on the desktop (stable for 300 ms)",
                                   "attempts": attempt + 1}),
                        ));
                    }
                }
                here => {
                    seen = here.map(|h| (h.x, h.y));
                    since = Instant::now();
                    frames = 1;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let (Some(geometry), Some(raw)) = (entry.geometry, raw)
            && entry.size.map(|s| s.0)
                == super::capture::png_size(&raw).ok().map(|s| i64::from(s.0))
        {
            return Ok((
                (geometry.x, geometry.y),
                json!({"method": "niri floating geometry"}),
            ));
        }
        bail!(
            "could not locate the window on the desktop (it may be covered, off-screen or showing no distinctive content); use desktop coordinates from a desktop screenshot instead"
        )
    }

    /// Desktop pixel for a pointer target, plus how it was mapped.
    fn resolve_point(&mut self, win: &Win, wid: &str, spec: &Spec) -> Result<((f64, f64), Value)> {
        match spec {
            Spec::Point("desktop", x, y) => Ok(((*x, *y), json!({"coordinate_space": "desktop"}))),
            Spec::Point(_, x, y) => {
                let entry = match win {
                    Win::Compositor(entry) => Some((**entry).clone()),
                    Win::Accessible(_) => self.compositor_for(wid)?,
                }
                .context("coordinate_space=window needs a compositor window backend that lists this window")?;
                let (origin, how) = self.desktop_origin(wid, &entry, false)?;
                let scale = self.window_captures.get(wid).map_or(1.0, |c| c.scale);
                let point =
                    windows::window_point(*x, *y, (origin.0 as f64, origin.1 as f64), scale);
                Ok((
                    point,
                    json!({"coordinate_space": "window", "window_origin": [origin.0, origin.1],
                           "image_scale": scale, "mapping": how}),
                ))
            }
            Spec::Element(obj) => {
                let a11y = self.a11y()?;
                let (x, y, width, height) = a11y.extents(obj, extents_type())?;
                let showing = a11y
                    .state(obj)
                    .is_ok_and(|s| s.contains(atspi::State::Showing));
                ensure!(
                    width > 0 && height > 0 && showing,
                    "element has no on-screen bounds"
                );
                let element = Rect {
                    x: i64::from(x),
                    y: i64::from(y),
                    width: i64::from(width),
                    height: i64::from(height),
                };
                if session_type() != Some("wayland") {
                    return Ok((
                        (
                            element.x as f64 + element.width as f64 / 2.0,
                            element.y as f64 + element.height as f64 / 2.0,
                        ),
                        json!({"coordinate_space": "element"}),
                    ));
                }
                let entry = self.compositor_for(wid)?.context("element-targeted pointer ops on Wayland need the compositor to list this window (AT-SPI extents are window-relative); use click/set_value or coordinate input")?;
                let Win::Accessible(win_obj) = win else {
                    bail!("element has no window");
                };
                let frame = self.a11y()?.extents(win_obj, atspi::CoordType::Window)?;
                let (origin, how) = self.desktop_origin(wid, &entry, true)?;
                let point = windows::element_point(
                    element,
                    (f64::from(frame.0), f64::from(frame.1)),
                    (origin.0 as f64, origin.1 as f64),
                    entry.scale,
                );
                Ok((
                    point,
                    json!({"coordinate_space": "element", "window_origin": [origin.0, origin.1],
                           "mapping": how}),
                ))
            }
        }
    }

    pub(super) fn inject(&mut self, args: &Value) -> Result<Value> {
        let op = args["op"].as_str().unwrap_or_default();
        let wid = window_id(args)?;
        if wid.starts_with(super::private::PREFIX) {
            return self.private_inject(args);
        }
        let mut extra = match op {
            "type_text" => {
                let text = args
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|t| t.chars().count() <= 16384)
                    .context("text must be a string of at most 16384 characters")?;
                let win = self.window(&wid)?;
                self.input_device()?; // surface missing prerequisites before touching focus
                self.observations.remove(&wid);
                self.ensure_active(&win, &wid)?;
                type_text(text)?;
                return self.finish(&win, &wid, op, args, json!({}));
            }
            "key" => {
                let (modifiers, key) = parse_keys(args.get("keys"))?;
                let hold = bounded_int(args, "hold_ms", 0, 0, 10_000)?;
                let win = self.window(&wid)?;
                self.input_device()?;
                self.observations.remove(&wid);
                self.ensure_active(&win, &wid)?;
                self.press(&modifiers, key)?;
                std::thread::sleep(Duration::from_millis(hold.max(20) as u64));
                self.release(&modifiers, key)?;
                return self.finish(
                    &win,
                    &wid,
                    op,
                    args,
                    json!({"keys": args["keys"], "held_ms": hold.max(20)}),
                );
            }
            "pointer_move" => return self.pointer_move(args, &wid),
            "pointer_click" | "scroll" => json!({}),
            "drag" => return self.drag(args, &wid),
            _ => bail!("unsupported operation: {op}"),
        };
        let code = if op == "pointer_click" {
            let code = button_code(args.get("button"))?;
            let count = args.get("count").map_or(Some(1), Value::as_i64);
            ensure!(matches!(count, Some(1 | 2)), "count must be 1 or 2");
            Some((code, count.unwrap_or(1)))
        } else {
            let (dx, dy) = (arg_or_zero(args, "dx"), arg_or_zero(args, "dy"));
            ensure!(
                dx.zip(dy)
                    .is_some_and(|(dx, dy)| dx.abs() <= 10_000.0 && dy.abs() <= 10_000.0),
                "scroll distance is limited to 10000 pixels"
            );
            None
        };
        let (win, spec) = self.pointer_target(args)?;
        self.input_device()?;
        self.ensure_active(&win, &wid)?;
        let ((x, y), mapping) = self.resolve_point(&win, &wid, &spec)?;
        self.move_pointer(x, y)?;
        self.pointer_window = Some(wid.clone());
        self.confirm_origin(&wid, &mapping)?;
        if let Some((code, count)) = code {
            self.click_button(code, count)?;
        } else {
            let dx = number(args.get("dx")).unwrap_or(0.0);
            let dy = number(args.get("dy")).unwrap_or(0.0);
            // Positive dy scrolls content down; REL_WHEEL is positive for
            // scrolling up. Positive dx scrolls right, like REL_HWHEEL.
            let (vertical, horizontal) = (-notches(dy), notches(dx));
            for _ in 0..vertical.abs() {
                self.emit(
                    EventType::RELATIVE,
                    RelativeAxisCode::REL_WHEEL.0,
                    vertical.signum() as i32,
                )?;
                std::thread::sleep(Duration::from_millis(10));
            }
            for _ in 0..horizontal.abs() {
                self.emit(
                    EventType::RELATIVE,
                    RelativeAxisCode::REL_HWHEEL.0,
                    horizontal.signum() as i32,
                )?;
                std::thread::sleep(Duration::from_millis(10));
            }
            extra = json!({"units": "wheel notches of about 120 pixels",
                           "notches": {"dx": horizontal, "dy": -vertical}});
        }
        merge(
            &mut extra,
            json!({"coordinate_click": !matches!(spec, Spec::Element(_)),
                   "point": {"x": x, "y": y}, "mapping": mapping}),
        );
        self.finish(&win, &wid, op, args, extra)
    }

    fn finish(
        &mut self,
        win: &Win,
        wid: &str,
        op: &str,
        args: &Value,
        mut extra: Value,
    ) -> Result<Value> {
        let restored = self.restore_focus(args, wid);
        merge(&mut extra, json!({"restored_focus": restored}));
        self.settle_and_snapshot(win, wid, op, extra)
    }

    fn pointer_move(&mut self, args: &Value, wid: &str) -> Result<Value> {
        let (Some(dx), Some(dy)) = (arg_or_zero(args, "dx"), arg_or_zero(args, "dy")) else {
            bail!("pointer_move needs dx, dy of at most 20000 counts");
        };
        ensure!(
            dx.abs() <= 20_000.0 && dy.abs() <= 20_000.0,
            "pointer_move needs dx, dy of at most 20000 counts"
        );
        let default_steps = ((dx.abs().max(dy.abs()) / 10.0).ceil() as i64).clamp(1, 200);
        let steps = bounded_int(args, "steps", default_steps, 1, 1000)?;
        let duration = bounded_int(args, "duration_ms", (steps * 8).min(2000), 0, 10_000)?;
        let held = match args.get("hold_keys") {
            None | Some(Value::Null) => None,
            Some(keys) => Some(parse_keys(Some(keys))?),
        };
        let win = self.window(wid)?;
        self.relative_device()?;
        self.input_device()?;
        let mut place = None;
        if args.get("x").is_some_and(|v| !v.is_null())
            || args.get("y").is_some_and(|v| !v.is_null())
        {
            let (Some(x), Some(y)) = (number(args.get("x")), number(args.get("y"))) else {
                bail!("pointer_move start point needs both x and y");
            };
            place = Some(Spec::Point(coordinate_space(args)?, x, y));
        }
        self.observations.remove(wid);
        self.ensure_active(&win, wid)?;
        let mut placement = Value::Null;
        if place.is_none() && self.pointer_window.as_deref() != Some(wid) {
            // Wayland sends motion (and grants pointer lock) only to the
            // surface under the pointer, so enter the window once first.
            let capture_scale = self.window_captures.get(wid).map_or(1.0, |c| c.scale);
            match self.compositor_for(wid)?.and_then(|entry| entry.size) {
                Some((width, height)) => {
                    place = Some(Spec::Point(
                        "window",
                        width as f64 * capture_scale / 2.0,
                        height as f64 * capture_scale / 2.0,
                    ));
                }
                None => {
                    placement = json!(
                        "pointer position unknown: pass x, y to place it inside the window first"
                    );
                }
            }
        }
        if let Some(place) = place {
            let placed = self
                .resolve_point(&win, wid, &place)
                .and_then(|((px, py), mapping)| {
                    self.move_pointer(px, py)?;
                    Ok(json!({"point": {"x": px, "y": py}, "mapping": mapping}))
                });
            match placed {
                Ok(value) => {
                    self.pointer_window = Some(wid.to_string());
                    placement = value;
                }
                Err(error) if args.get("x").is_some_and(|v| !v.is_null()) => return Err(error),
                Err(error) => {
                    placement = json!(format!(
                        "pointer not placed ({error:#}); motion reaches whichever surface is under the pointer"
                    ));
                }
            }
        }
        if let Some((modifiers, key)) = &held {
            self.press(modifiers, *key)?;
        }
        let pause = Duration::from_secs_f64(duration as f64 / 1000.0 / steps as f64);
        let moved = (|| -> Result<()> {
            for (ex, ey) in windows::split_motion(dx, dy, steps as u32) {
                let mut events = Vec::new();
                if ex != 0 {
                    events.push(InputEvent::new(
                        EventType::RELATIVE.0,
                        RelativeAxisCode::REL_X.0,
                        ex,
                    ));
                }
                if ey != 0 {
                    events.push(InputEvent::new(
                        EventType::RELATIVE.0,
                        RelativeAxisCode::REL_Y.0,
                        ey,
                    ));
                }
                self.relative_device
                    .as_mut()
                    .expect("created above")
                    .emit(&events)?;
                std::thread::sleep(pause);
            }
            Ok(())
        })();
        if let Some((modifiers, key)) = &held {
            self.release(modifiers, *key)?;
        }
        moved?;
        self.finish(
            &win,
            wid,
            "pointer_move",
            args,
            json!({"sent": {"dx": py_round(dx), "dy": py_round(dy)}, "steps": steps,
                   "duration_ms": duration, "hold_keys": args.get("hold_keys"),
                   "placement": placement,
                   "units": "raw relative mouse counts; apps using relative-pointer (games) get them unaccelerated, the visible cursor follows compositor pointer acceleration"}),
        )
    }

    fn drag(&mut self, args: &Value, wid: &str) -> Result<Value> {
        let points: Vec<Option<f64>> = ["from_x", "from_y", "to_x", "to_y"]
            .iter()
            .map(|key| number(args.get(*key)))
            .collect();
        let [Some(sx), Some(sy), Some(ex), Some(ey)] = points[..] else {
            bail!("drag needs from_x, from_y, to_x, to_y");
        };
        let space = coordinate_space(args)?;
        let code = button_code(args.get("button"))?;
        let win = self.window(wid)?;
        self.input_device()?;
        self.observations.remove(wid);
        self.ensure_active(&win, wid)?;
        let ((fx, fy), mapping) = self.resolve_point(&win, wid, &Spec::Point(space, sx, sy))?;
        let (tx, ty) = if space == "desktop" {
            (ex, ey)
        } else {
            // Same window origin and screenshot scale as the start point.
            let scale = mapping["image_scale"].as_f64().unwrap_or(1.0);
            (fx + (ex - sx) / scale, fy + (ey - sy) / scale)
        };
        let (width, height) = self.screen_size()?;
        for (x, y) in [(fx, fy), (tx, ty)] {
            ensure!(
                (0.0..f64::from(width)).contains(&x) && (0.0..f64::from(height)).contains(&y),
                "point ({}, {}) is outside the {width}x{height} desktop",
                g(x),
                g(y)
            );
        }
        self.move_pointer(fx, fy)?;
        self.pointer_window = Some(wid.to_string());
        self.confirm_origin(wid, &mapping)?;
        self.key(code, 1)?;
        for step in 1..=12 {
            let t = f64::from(step) / 12.0;
            self.move_pointer(fx + (tx - fx) * t, fy + (ty - fy) * t)?;
            std::thread::sleep(Duration::from_millis(20));
        }
        self.key(code, 0)?;
        self.finish(
            &win,
            wid,
            "drag",
            args,
            json!({"from": {"x": fx, "y": fy}, "to": {"x": tx, "y": ty}, "mapping": mapping}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_specs_map_to_evdev_codes() {
        assert_eq!(
            parse_keys(Some(&json!("ctrl+shift+t"))).unwrap(),
            (vec![29, 42], 20)
        );
        assert_eq!(parse_keys(Some(&json!("shift"))).unwrap(), (vec![], 42));
        assert_eq!(parse_keys(Some(&json!("f12"))).unwrap(), (vec![], 88));
        assert_eq!(parse_keys(Some(&json!("f5"))).unwrap(), (vec![], 63));
        assert_eq!(parse_keys(Some(&json!("0"))).unwrap(), (vec![], 11));
        assert!(parse_keys(Some(&json!("a+b"))).is_err());
        assert!(parse_keys(Some(&json!("hyper"))).is_err());
    }

    #[test]
    fn pointer_axis_and_wheel_mapping() {
        assert_eq!(abs_coordinate(0.0, 1920), 17);
        assert_eq!(abs_coordinate(1919.0, 1920), 65519);
        assert_eq!((notches(0.0), notches(10.0), notches(-360.0)), (0, 1, -3));
    }
}
