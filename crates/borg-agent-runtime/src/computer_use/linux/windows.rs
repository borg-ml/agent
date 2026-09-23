//! Compositor window enumeration and window-to-desktop coordinate mapping.
//! Pure parsing and mapping plus thin IPC wrappers; no AT-SPI, so the parsers
//! are unit tested without a desktop.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Map, Value, json};
use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

use super::{py_round, run_text, which};

const MAX_COMPOSITOR_WINDOWS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Rect {
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
}

impl Rect {
    fn new(x: f64, y: f64, width: f64, height: f64) -> Self {
        Self {
            x: py_round(x),
            y: py_round(y),
            width: py_round(width),
            height: py_round(height),
        }
    }

    pub fn json(&self) -> Value {
        json!({"x": self.x, "y": self.y, "width": self.width, "height": self.height})
    }

    fn intersects(&self, area: &Rect) -> bool {
        self.x < area.x + area.width
            && area.x < self.x + self.width
            && self.y < area.y + area.height
            && area.y < self.y + self.height
    }
}

/// One window as a compositor lists it.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct CompositorWindow {
    pub backend: &'static str,
    /// niri/sway/X11 integer id, Hyprland address or foreign-toplevel identifier.
    pub native_id: Value,
    pub id: String,
    pub title: String,
    pub app_id: Option<String>,
    pub pid: Option<i64>,
    pub workspace: Value,
    pub output: Option<String>,
    pub focused: bool,
    pub floating: Option<bool>,
    pub visible: Option<bool>,
    pub geometry: Option<Rect>,
    pub size: Option<(i64, i64)>,
    pub toplevel_identifier: Option<String>,
    pub scale: f64,
}

impl CompositorWindow {
    /// The fields reported to callers.
    pub fn public(&self) -> Value {
        json!({
            "backend": self.backend,
            "native_id": self.native_id,
            "title": self.title,
            "app_id": self.app_id,
            "pid": self.pid,
            "workspace": self.workspace,
            "output": self.output,
            "focused": self.focused,
            "floating": self.floating,
            "visible": self.visible,
            "geometry": self.geometry.map(|g| g.json()),
            "size": self.size.map(|(width, height)| json!({"width": width, "height": height})),
        })
    }
}

fn f(value: &Value) -> Option<f64> {
    value.as_f64().filter(|value| value.is_finite())
}

fn string(value: &Value) -> Option<String> {
    value.as_str().filter(|s| !s.is_empty()).map(str::to_string)
}

/// grim renders the whole desktop at the greatest output scale.
fn desktop_scale(scales: impl IntoIterator<Item = Option<f64>>) -> f64 {
    scales
        .into_iter()
        .flatten()
        .filter(|scale| *scale > 0.0)
        .fold(None, |best: Option<f64>, scale| {
            Some(best.map_or(scale, |best| best.max(scale)))
        })
        .unwrap_or(1.0)
}

// ---- niri: JSON IPC on $NIRI_SOCKET ----------------------------------------

pub(super) fn niri_request(request: Value) -> Result<Value> {
    let path = std::env::var_os("NIRI_SOCKET").context("NIRI_SOCKET is not set")?;
    let mut stream = UnixStream::connect(path)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    stream.write_all(format!("{request}\n").as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut data = Vec::new();
    stream
        .take(16 * 1024 * 1024)
        .read_to_end(&mut data)
        .context("niri IPC read failed")?;
    let reply: Value = serde_json::from_slice(&data)?;
    if let Some(error) = reply.get("Err") {
        let error = error
            .as_str()
            .map_or_else(|| error.to_string(), str::to_string);
        bail!("niri refused the request: {error}");
    }
    reply
        .get("Ok")
        .cloned()
        .ok_or_else(|| anyhow!("niri returned no result"))
}

/// Normalise niri Windows/Workspaces/Outputs replies.
///
/// niri reports absolute placement only for floating windows
/// (tile_pos_in_workspace_view); tiled windows scroll with a view offset that
/// IPC does not expose, so their geometry is null and visibility is unknown
/// unless their workspace is inactive.
pub(super) fn niri_windows(
    windows: &[Value],
    workspaces: &[Value],
    outputs: &Map<String, Value>,
) -> Vec<CompositorWindow> {
    let scale = desktop_scale(outputs.values().map(|o| f(&o["logical"]["scale"])));
    let spaces: HashMap<String, &Value> = workspaces
        .iter()
        .map(|w| (w["id"].to_string(), w))
        .collect();
    let mut result = Vec::new();
    for w in windows.iter().take(MAX_COMPOSITOR_WINDOWS) {
        let space = spaces.get(&w["workspace_id"].to_string()).copied();
        let output = space
            .and_then(|s| s["output"].as_str())
            .and_then(|name| outputs.get(name));
        let logical = output.map(|o| &o["logical"]).filter(|l| l.is_object());
        let layout = &w["layout"];
        let size = [
            f(&layout["window_size"][0]).unwrap_or(0.0),
            f(&layout["window_size"][1]).unwrap_or(0.0),
        ];
        let active = space.is_some_and(|s| s["is_active"].as_bool() == Some(true));
        let tile = &layout["tile_pos_in_workspace_view"];
        let mut geometry = None;
        if let (Some(tx), Some(ty), Some(logical)) = (f(&tile[0]), f(&tile[1]), logical)
            && active
        {
            let offset = &layout["window_offset_in_tile"];
            let (ox, oy) = (f(&offset[0]).unwrap_or(0.0), f(&offset[1]).unwrap_or(0.0));
            let (lx, ly) = (
                f(&logical["x"]).unwrap_or(0.0),
                f(&logical["y"]).unwrap_or(0.0),
            );
            geometry = Some(Rect::new(
                (lx + tx + ox) * scale,
                (ly + ty + oy) * scale,
                size[0] * scale,
                size[1] * scale,
            ));
        }
        let output_area = logical.map(|l| {
            Rect::new(
                f(&l["x"]).unwrap_or(0.0) * scale,
                f(&l["y"]).unwrap_or(0.0) * scale,
                f(&l["width"]).unwrap_or(0.0) * scale,
                f(&l["height"]).unwrap_or(0.0) * scale,
            )
        });
        let visible = match (space, geometry, output_area) {
            // Not on any workspace (e.g. unmapped), or on an inactive one.
            (None, _, _) => Some(false),
            _ if !active => Some(false),
            (_, Some(geometry), Some(area)) => Some(geometry.intersects(&area)),
            _ => None,
        };
        let workspace = space.map_or(Value::Null, |s| {
            if s["name"].as_str().is_some_and(|n| !n.is_empty()) {
                s["name"].clone()
            } else {
                s["idx"].clone()
            }
        });
        result.push(CompositorWindow {
            backend: "niri",
            native_id: w["id"].clone(),
            id: format!("niri:{}", w["id"]),
            title: w["title"].as_str().unwrap_or_default().to_string(),
            app_id: w["app_id"].as_str().map(str::to_string),
            pid: w["pid"].as_i64(),
            workspace,
            output: space.and_then(|s| s["output"].as_str()).map(str::to_string),
            focused: w["is_focused"].as_bool() == Some(true),
            floating: Some(w["is_floating"].as_bool() == Some(true)),
            visible,
            geometry,
            size: Some((py_round(size[0] * scale), py_round(size[1] * scale))),
            toplevel_identifier: None,
            scale: logical.and_then(|l| f(&l["scale"])).unwrap_or(1.0),
        });
    }
    result
}

fn niri_list() -> Result<Vec<CompositorWindow>> {
    let outputs = niri_request(json!("Outputs"))?;
    let workspaces = niri_request(json!("Workspaces"))?;
    let windows = niri_request(json!("Windows"))?;
    Ok(niri_windows(
        windows["Windows"].as_array().map_or(&[][..], Vec::as_slice),
        workspaces["Workspaces"]
            .as_array()
            .map_or(&[][..], Vec::as_slice),
        outputs["Outputs"].as_object().unwrap_or(&Map::new()),
    ))
}

// ---- sway: swaymsg -t get_tree ----------------------------------------------

/// Leaf containers of a sway tree. Rects are absolute logical coordinates.
pub(super) fn sway_windows(tree: &Value) -> Vec<CompositorWindow> {
    let scale = desktop_scale(
        tree["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|o| o["type"] == "output")
            .map(|o| f(&o["scale"])),
    );
    let mut result = Vec::new();
    walk_sway(tree, None, Value::Null, scale, &mut result);
    result
}

fn walk_sway(
    node: &Value,
    mut output: Option<String>,
    mut workspace: Value,
    scale: f64,
    result: &mut Vec<CompositorWindow>,
) {
    match node["type"].as_str() {
        Some("output") => output = node["name"].as_str().map(str::to_string),
        Some("workspace") => workspace = node["name"].clone(),
        _ => {}
    }
    let children: Vec<&Value> = ["nodes", "floating_nodes"]
        .iter()
        .flat_map(|key| node[*key].as_array().into_iter().flatten())
        .collect();
    let kind = node["type"].as_str();
    let has_client = [&node["pid"], &node["app_id"], &node["window"]]
        .iter()
        .any(|v| !v.is_null() && *v != &json!(0) && *v != &json!(""));
    if children.is_empty() && matches!(kind, Some("con" | "floating_con")) && has_client {
        let (outer, inner) = (&node["rect"], &node["window_rect"]);
        let n = |v: &Value, key: &str| f(&v[key]).unwrap_or(0.0);
        let width = f(&inner["width"]).unwrap_or_else(|| n(outer, "width"));
        let height = f(&inner["height"]).unwrap_or_else(|| n(outer, "height"));
        let geometry = Rect::new(
            (n(outer, "x") + n(inner, "x")) * scale,
            (n(outer, "y") + n(inner, "y")) * scale,
            width * scale,
            height * scale,
        );
        result.push(CompositorWindow {
            backend: "sway",
            native_id: node["id"].clone(),
            id: format!("sway:{}", node["id"]),
            title: node["name"].as_str().unwrap_or_default().to_string(),
            app_id: string(&node["app_id"]).or_else(|| string(&node["window_properties"]["class"])),
            pid: node["pid"].as_i64(),
            workspace: workspace.clone(),
            output: output.clone(),
            focused: node["focused"].as_bool() == Some(true),
            floating: Some(kind == Some("floating_con")),
            visible: Some(node["visible"].as_bool() == Some(true)),
            geometry: (geometry.width > 0 && geometry.height > 0).then_some(geometry),
            size: Some((geometry.width, geometry.height)),
            toplevel_identifier: node["foreign_toplevel_identifier"]
                .as_str()
                .map(str::to_string),
            scale,
        });
    }
    for child in children {
        if result.len() >= MAX_COMPOSITOR_WINDOWS {
            return;
        }
        walk_sway(child, output.clone(), workspace.clone(), scale, result);
    }
}

// ---- Hyprland: hyprctl -j clients / monitors --------------------------------

pub(super) fn hyprland_windows(clients: &[Value], monitors: &[Value]) -> Vec<CompositorWindow> {
    let scale = desktop_scale(monitors.iter().map(|m| f(&m["scale"])));
    let shown: Vec<&Value> = monitors
        .iter()
        .flat_map(|m| [&m["activeWorkspace"]["id"], &m["specialWorkspace"]["id"]])
        .collect();
    let mut result = Vec::new();
    for c in clients.iter().take(MAX_COMPOSITOR_WINDOWS) {
        if c["mapped"].as_bool() == Some(false) {
            continue;
        }
        let at = [f(&c["at"][0]).unwrap_or(0.0), f(&c["at"][1]).unwrap_or(0.0)];
        let size = [
            f(&c["size"][0]).unwrap_or(0.0),
            f(&c["size"][1]).unwrap_or(0.0),
        ];
        let workspace = &c["workspace"];
        let address = c["address"].as_str().unwrap_or_default();
        result.push(CompositorWindow {
            backend: "hyprland",
            native_id: json!(address),
            id: format!("hyprland:{address}"),
            title: c["title"].as_str().unwrap_or_default().to_string(),
            app_id: c["class"].as_str().map(str::to_string),
            pid: c["pid"].as_i64(),
            workspace: if workspace["name"].as_str().is_some_and(|n| !n.is_empty()) {
                workspace["name"].clone()
            } else {
                workspace["id"].clone()
            },
            output: monitors
                .iter()
                .find(|m| m["id"] == c["monitor"])
                .and_then(|m| m["name"].as_str())
                .map(str::to_string),
            focused: c["focusHistoryID"] == json!(0),
            floating: Some(c["floating"].as_bool() == Some(true)),
            visible: Some(shown.contains(&&workspace["id"]) && c["hidden"].as_bool() != Some(true)),
            geometry: Some(Rect::new(
                at[0] * scale,
                at[1] * scale,
                size[0] * scale,
                size[1] * scale,
            )),
            size: Some((py_round(size[0] * scale), py_round(size[1] * scale))),
            toplevel_identifier: None,
            scale,
        });
    }
    result
}

// ---- X11 EWMH (X sessions): xprop + xdotool ---------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(super) enum XValue {
    Int(i64),
    Str(String),
}

impl XValue {
    fn text(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Str(value) => value.clone(),
        }
    }
}

/// Python's `int(text, 0)`: decimal without leading zeros or a 0x/0o/0b prefix.
fn parse_int(text: &str) -> Option<i64> {
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text.strip_prefix('+').unwrap_or(text)),
    };
    let lower = digits.to_ascii_lowercase();
    let value = if let Some(hex) = lower.strip_prefix("0x") {
        i64::from_str_radix(hex, 16).ok()?
    } else if let Some(octal) = lower.strip_prefix("0o") {
        i64::from_str_radix(octal, 8).ok()?
    } else if let Some(binary) = lower.strip_prefix("0b") {
        i64::from_str_radix(binary, 2).ok()?
    } else if !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits.len() == 1 || !digits.starts_with('0'))
    {
        digits.parse().ok()?
    } else {
        return None;
    };
    Some(sign * value)
}

/// Parse `xprop` output lines into {atom: [values]} (strings unquoted, ints decoded).
pub(super) fn xprop_values(text: &str) -> HashMap<String, Vec<XValue>> {
    let mut values = HashMap::new();
    for line in text.lines() {
        let (head, rest) = match line.split_once(" = ") {
            Some(parts) => parts,
            None => match line.split_once(": ") {
                Some((head, rest)) if rest.contains('#') => {
                    (head, rest.split_once('#').map_or("", |(_, r)| r))
                }
                _ => continue,
            },
        };
        let atom = head
            .split('(')
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        let mut items = Vec::new();
        let (mut current, mut raw) = (String::new(), String::new());
        let (mut quoted, mut escaped) = (false, false);
        for ch in rest.trim().chars() {
            if quoted {
                if escaped {
                    current.push(ch);
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    quoted = false;
                    items.push(std::mem::take(&mut current));
                } else {
                    current.push(ch);
                }
            } else if ch == '"' {
                quoted = true;
            } else if ch == ',' {
                if !raw.trim().is_empty() {
                    items.push(raw.trim().to_string());
                }
                raw.clear();
            } else {
                raw.push(ch);
            }
        }
        if !raw.trim().is_empty() {
            items.push(raw.trim().to_string());
        }
        let parsed = items
            .into_iter()
            .map(|item| parse_int(&item).map_or(XValue::Str(item), XValue::Int))
            .collect();
        values.insert(atom, parsed);
    }
    values
}

fn first<'a>(props: &'a HashMap<String, Vec<XValue>>, atom: &str) -> Option<&'a XValue> {
    props.get(atom).and_then(|values| values.first())
}

pub(super) fn x11_window(
    xid: i64,
    props: &HashMap<String, Vec<XValue>>,
    geometry: Option<Rect>,
    active: Option<i64>,
) -> CompositorWindow {
    let name = first(props, "_NET_WM_NAME")
        .or_else(|| first(props, "WM_NAME"))
        .map_or_else(String::new, XValue::text);
    let state = props.get("_NET_WM_STATE").cloned().unwrap_or_default();
    let int = |atom| match first(props, atom) {
        Some(XValue::Int(value)) => Some(*value),
        _ => None,
    };
    CompositorWindow {
        backend: "x11",
        native_id: json!(xid),
        id: format!("x11:{xid:#x}"),
        title: name,
        app_id: props
            .get("WM_CLASS")
            .and_then(|class| class.last())
            .map(XValue::text),
        pid: int("_NET_WM_PID"),
        workspace: int("_NET_WM_DESKTOP").map_or(Value::Null, |desktop| json!(desktop)),
        output: None,
        focused: Some(xid) == active,
        floating: None,
        visible: Some(!state.contains(&XValue::Str("_NET_WM_STATE_HIDDEN".into()))),
        geometry,
        size: geometry.map(|g| (g.width, g.height)),
        toplevel_identifier: None,
        scale: 1.0,
    }
}

pub(super) fn xdotool_geometry(text: &str) -> Option<Rect> {
    let fields: HashMap<&str, &str> = text.lines().filter_map(|l| l.split_once('=')).collect();
    let value = |key| fields.get(key)?.trim().parse::<i64>().ok();
    Some(Rect {
        x: value("X")?,
        y: value("Y")?,
        width: value("WIDTH")?,
        height: value("HEIGHT")?,
    })
}

fn x11_list() -> Result<Vec<CompositorWindow>> {
    let root = xprop_values(&run_text(
        &["xprop", "-root", "_NET_CLIENT_LIST", "_NET_ACTIVE_WINDOW"],
        3,
    )?);
    let active = match first(&root, "_NET_ACTIVE_WINDOW") {
        Some(XValue::Int(id)) => Some(*id),
        _ => None,
    };
    let mut result = Vec::new();
    let ids = root.get("_NET_CLIENT_LIST").cloned().unwrap_or_default();
    for xid in ids
        .iter()
        .filter_map(|v| match v {
            XValue::Int(id) => Some(*id),
            XValue::Str(_) => None,
        })
        .take(MAX_COMPOSITOR_WINDOWS)
    {
        let id = xid.to_string();
        let props = xprop_values(&run_text(
            &[
                "xprop",
                "-id",
                &id,
                "_NET_WM_NAME",
                "WM_NAME",
                "WM_CLASS",
                "_NET_WM_PID",
                "_NET_WM_DESKTOP",
                "_NET_WM_STATE",
            ],
            3,
        )?);
        let geometry = if which("xdotool").is_some() {
            xdotool_geometry(&run_text(
                &["xdotool", "getwindowgeometry", "--shell", &id],
                3,
            )?)
        } else {
            None
        };
        result.push(x11_window(xid, &props, geometry, active));
    }
    Ok(result)
}

// ---- wlr/ext foreign-toplevel via lswt (other wlroots compositors) ----------

pub(super) fn lswt_windows(data: &Value) -> Vec<CompositorWindow> {
    data["toplevels"]
        .as_array()
        .into_iter()
        .flatten()
        .take(MAX_COMPOSITOR_WINDOWS)
        .enumerate()
        .map(|(index, t)| {
            let identifier = string(&t["identifier"]);
            let native = identifier.clone().unwrap_or_else(|| index.to_string());
            CompositorWindow {
                backend: "foreign-toplevel",
                native_id: json!(native),
                id: format!("toplevel:{native}"),
                title: t["title"].as_str().unwrap_or_default().to_string(),
                app_id: string(&t["app-id"]).or_else(|| string(&t["app_id"])),
                pid: None,
                workspace: Value::Null,
                output: None,
                focused: t["activated"].as_bool() == Some(true),
                floating: None,
                visible: (t["minimized"].as_bool() == Some(true)).then_some(false),
                geometry: None,
                size: None,
                toplevel_identifier: identifier,
                scale: 1.0,
            }
        })
        .collect()
}

// ---- backend selection -------------------------------------------------------

fn env_set(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

/// The window backend for this session, chosen at call time.
pub(super) fn compositor_backend() -> Option<&'static str> {
    if env_set("NIRI_SOCKET") {
        Some("niri")
    } else if env_set("SWAYSOCK") && which("swaymsg").is_some() {
        Some("sway")
    } else if env_set("HYPRLAND_INSTANCE_SIGNATURE") && which("hyprctl").is_some() {
        Some("hyprland")
    } else if env_set("WAYLAND_DISPLAY") {
        which("lswt").map(|_| "foreign-toplevel")
    } else if env_set("DISPLAY") && which("xprop").is_some() {
        Some("x11")
    } else {
        None
    }
}

pub(super) fn compositor_list(backend: Option<&str>) -> Result<Vec<CompositorWindow>> {
    let json_of =
        |command: &[&str]| -> Result<Value> { Ok(serde_json::from_str(&run_text(command, 3)?)?) };
    let backend: Option<&str> = match backend {
        Some(backend) => Some(backend),
        None => compositor_backend(),
    };
    match backend {
        Some("niri") => niri_list(),
        Some("sway") => Ok(sway_windows(&json_of(&[
            "swaymsg", "-r", "-t", "get_tree",
        ])?)),
        Some("hyprland") => {
            let clients = json_of(&["hyprctl", "-j", "clients"])?;
            let monitors = json_of(&["hyprctl", "-j", "monitors"])?;
            Ok(hyprland_windows(
                clients.as_array().map_or(&[][..], Vec::as_slice),
                monitors.as_array().map_or(&[][..], Vec::as_slice),
            ))
        }
        Some("x11") => x11_list(),
        Some("foreign-toplevel") => Ok(lswt_windows(&json_of(&["lswt", "-j"])?)),
        _ => Ok(Vec::new()),
    }
}

/// Title without symbol decorations (spinners, bells) that apps animate between reads.
pub(super) fn title_key(title: &str) -> String {
    title
        .chars()
        .filter(|c| c.general_category() != GeneralCategory::OtherSymbol)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// An accessible window to correlate: (key, pid, title, active).
pub(super) type AccessibleWindow = (String, Option<i64>, String, bool);

/// Map AT-SPI window keys to compositor entries by pid, then (normalised) title.
///
/// X11 clients under XWayland report the bridge's pid to the compositor, so a
/// globally unique title also matches; focus state breaks ties between
/// same-titled windows. Ambiguous candidates stay unmatched rather than guessed.
pub(super) fn correlate(
    accessible: &[AccessibleWindow],
    compositor: &[CompositorWindow],
) -> HashMap<String, CompositorWindow> {
    let mut matches: HashMap<String, CompositorWindow> = HashMap::new();
    let mut used: Vec<String> = Vec::new();
    let mut pids: HashMap<i64, usize> = HashMap::new();
    for (_, pid, _, _) in accessible {
        if let Some(pid) = pid.filter(|pid| *pid != 0) {
            *pids.entry(pid).or_default() += 1;
        }
    }
    let identity = |title: &str| title.to_string();
    let normalisers: [&dyn Fn(&str) -> String; 2] = [&identity, &title_key];
    for normalise in normalisers {
        for (key, pid, title, active) in accessible {
            if matches.contains_key(key) {
                continue;
            }
            let pid = pid.filter(|pid| *pid != 0);
            let free: Vec<&CompositorWindow> = compositor
                .iter()
                .filter(|c| !used.contains(&c.id))
                .collect();
            let same_pid: Vec<&CompositorWindow> = free
                .iter()
                .copied()
                .filter(|c| pid.is_some() && c.pid == pid)
                .collect();
            let wanted = normalise(title);
            let mut pool = if same_pid.len() == 1 && pid.is_some_and(|pid| pids[&pid] == 1) {
                same_pid
            } else {
                let mut pool: Vec<&CompositorWindow> = same_pid
                    .into_iter()
                    .filter(|c| normalise(&c.title) == wanted)
                    .collect();
                if pool.is_empty() && !wanted.is_empty() {
                    pool = free
                        .iter()
                        .copied()
                        .filter(|c| normalise(&c.title) == wanted)
                        .collect();
                }
                pool
            };
            if pool.len() > 1 {
                pool.retain(|c| c.focused == *active);
            }
            if let [chosen] = pool[..] {
                used.push(chosen.id.clone());
                matches.insert(key.clone(), chosen.clone());
            }
        }
    }
    matches
}

// ---- coordinate mapping -------------------------------------------------------

/// Desktop pixel at the centre of an AT-SPI element.
///
/// On Wayland AT-SPI reports extents relative to the toplevel surface (the
/// window's own extents are then at 0,0); on X11 both are screen coordinates.
/// Either way the element's offset from the window's own extents, in logical
/// pixels, plus the window's desktop origin gives the desktop position.
pub(super) fn element_point(
    element: Rect,
    window: (f64, f64),
    origin: (f64, f64),
    scale: f64,
) -> (f64, f64) {
    (
        origin.0 + (element.x as f64 - window.0 + element.width as f64 / 2.0) * scale,
        origin.1 + (element.y as f64 - window.1 + element.height as f64 / 2.0) * scale,
    )
}

/// Desktop pixel for a point read from a window screenshot (possibly downscaled).
pub(super) fn window_point(x: f64, y: f64, origin: (f64, f64), image_scale: f64) -> (f64, f64) {
    (origin.0 + x / image_scale, origin.1 + y / image_scale)
}

/// Integer relative-motion events whose running sums track dx, dy exactly.
pub(super) fn split_motion(dx: f64, dy: f64, steps: u32) -> Vec<(i32, i32)> {
    let (mut sent_x, mut sent_y) = (0, 0);
    (1..=steps)
        .map(|step| {
            let fraction = f64::from(step) / f64::from(steps);
            let target = (
                py_round(dx * fraction) as i32,
                py_round(dy * fraction) as i32,
            );
            let event = (target.0 - sent_x, target.1 - sent_y);
            (sent_x, sent_y) = target;
            event
        })
        .collect()
}

// ---- window localisation --------------------------------------------------------

/// An RGB image, row-major, three bytes per pixel.
pub(super) struct Rgb {
    pub width: usize,
    pub height: usize,
    pub data: Vec<u8>,
}

impl Rgb {
    fn pixel(&self, x: usize, y: usize) -> &[u8] {
        let at = (y * self.width + x) * 3;
        &self.data[at..at + 3]
    }
}

/// (row, col) of the most textured horizontal strips, spread over the window.
///
/// Texture is the number of colour changes inside the strip; flat regions and
/// semi-transparent pixels (shadows, rounded corners) are skipped because the
/// desktop composites them differently.
fn distinctive_strips(window: &Rgb, alpha: Option<&[u8]>, strip: usize) -> Vec<(usize, usize)> {
    let (h, w) = (window.height, window.width);
    // Best (score, row, col) per 48x96 cell.
    let mut best: HashMap<(usize, usize), (i64, usize, usize)> = HashMap::new();
    for r in (0..h).step_by(4) {
        // csum[x] = colour changes between pixels 0..x of this row.
        let mut csum = vec![0i64; w];
        for x in 1..w {
            let changed = window.pixel(x - 1, r) != window.pixel(x, r);
            csum[x] = csum[x - 1] + i64::from(changed);
        }
        let clear: Option<Vec<i64>> = alpha.map(|alpha| {
            let mut clear = vec![0i64; w + 1];
            for x in 0..w {
                clear[x + 1] = clear[x] + i64::from(alpha[r * w + x] < 255);
            }
            clear
        });
        for c in (0..=w - strip).step_by(8) {
            let mut score = csum[c + strip - 1] - csum[c];
            if clear
                .as_ref()
                .is_some_and(|clear| clear[c + strip] - clear[c] > 0)
            {
                score = 0;
            }
            if score < 6 {
                continue;
            }
            let slot = best.entry((r / 48, c / 96)).or_insert((score, r, c));
            if score > slot.0 {
                *slot = (score, r, c);
            }
        }
    }
    let mut ranked: Vec<(i64, usize, usize)> = best.into_values().collect();
    ranked.sort_by(|a, b| b.cmp(a));
    ranked
        .into_iter()
        .take(48)
        .map(|(_, r, c)| (r, c))
        .collect()
}

#[derive(Debug, PartialEq)]
pub(super) struct Located {
    pub x: i64,
    pub y: i64,
    pub agreement: f64,
    pub votes: f64,
}

/// Evenly spaced integer samples like `numpy.linspace(start, end, num).astype(int)`.
fn linspace(start: i64, end: i64, num: i64) -> Vec<i64> {
    if num == 1 {
        return vec![start];
    }
    (0..num)
        .map(|i| (start as f64 + (end - start) as f64 * i as f64 / (num - 1) as f64) as i64)
        .collect()
}

/// Find where a window capture sits inside a desktop capture of the same scale.
///
/// The most textured strips of the window are searched exactly in the desktop
/// and vote for an offset (weighted by how unique each match is); the winner
/// must clearly beat the runner-up (repetitive content is refused) and is
/// verified by sampled pixel agreement, so occlusion or animation in parts of
/// the window still localises.
pub(super) fn locate_window(window: &Rgb, desktop: &Rgb, alpha: Option<&[u8]>) -> Option<Located> {
    let (wh, ww) = (window.height as i64, window.width as i64);
    let (dh, dw) = (desktop.height as i64, desktop.width as i64);
    let strip = 48.min(window.width);
    if strip < 8 || wh < 1 {
        return None;
    }
    let row_bytes = desktop.width * 3;
    let finder_haystack = &desktop.data[..];
    let mut votes: Vec<((i64, i64), f64)> = Vec::new();
    for (r, c) in distinctive_strips(window, alpha, strip) {
        let at = (r * window.width + c) * 3;
        let needle = &window.data[at..at + strip * 3];
        let finder = memchr::memmem::Finder::new(needle);
        let mut hits: Vec<(i64, i64)> = Vec::new();
        let mut start = 0;
        while hits.len() <= 8 {
            let Some(found) = finder.find(&finder_haystack[start..]) else {
                break;
            };
            let index = start + found;
            start = index + 1;
            let (y, rem) = (index / row_bytes, index % row_bytes);
            if rem % 3 == 0 && rem / 3 + strip <= desktop.width {
                let hit = (rem as i64 / 3 - c as i64, y as i64 - r as i64);
                if !hits.contains(&hit) {
                    hits.push(hit);
                }
            }
        }
        if !hits.is_empty() && hits.len() <= 8 {
            let weight = 1.0 / hits.len() as f64;
            for hit in hits {
                match votes.iter_mut().find(|(key, _)| *key == hit) {
                    Some((_, total)) => *total += weight,
                    None => votes.push((hit, weight)),
                }
            }
        }
    }
    // Stable: equal votes keep first-seen order, like Python's sorted().
    votes.sort_by(|a, b| b.1.total_cmp(&a.1));
    let ((x, y), weight) = *votes.first()?;
    if weight < 2.0 || votes.get(1).is_some_and(|second| second.1 * 1.5 >= weight) {
        return None;
    }
    let (x0, y0) = (x.max(0), y.max(0));
    let (x1, y1) = ((x + ww).min(dw), (y + wh).min(dh));
    if x1 - x0 < 8 || y1 - y0 < 8 {
        return None;
    }
    let ys = linspace(y0, y1 - 1, (y1 - y0).min(64));
    let xs = linspace(x0, x1 - 1, (x1 - x0).min(64));
    let mut agree = 0usize;
    for &sy in &ys {
        for &sx in &xs {
            let desk = desktop.pixel(sx as usize, sy as usize);
            let win = window.pixel((sx - x) as usize, (sy - y) as usize);
            let max = desk
                .iter()
                .zip(win)
                .map(|(a, b)| a.abs_diff(*b))
                .max()
                .unwrap_or(0);
            agree += usize::from(max <= 2);
        }
    }
    let agreement = agree as f64 / (ys.len() * xs.len()) as f64;
    if agreement < 0.5 {
        return None;
    }
    Some(Located {
        x,
        y,
        agreement: (agreement * 1000.0).round_ties_even() / 1000.0,
        votes: (weight * 10.0).round_ties_even() / 10.0,
    })
}

/// The single MIME type worth restoring after a compositor clobbered the clipboard.
pub(super) fn clipboard_restore_type(types: &[String]) -> Option<String> {
    for preferred in [
        "text/plain;charset=utf-8",
        "UTF8_STRING",
        "text/plain",
        "STRING",
        "TEXT",
    ] {
        if types.iter().any(|t| t == preferred) {
            return Some(preferred.into());
        }
    }
    let images: Vec<&String> = types.iter().filter(|t| t.starts_with("image/")).collect();
    if images.iter().any(|t| *t == "image/png") {
        return Some("image/png".into());
    }
    images.first().copied().or(types.first()).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parsers and mapping decide where injected clicks land; a regression
    /// would click the wrong pixels.
    #[test]
    fn niri_geometry_only_for_floating_windows_on_active_workspaces() {
        let outputs = json!({"DP-1": {"logical": {"x": 100, "y": 0, "width": 1280, "height": 720, "scale": 2.0}}});
        let workspaces = json!([
            {"id": 1, "idx": 1, "name": null, "output": "DP-1", "is_active": true},
            {"id": 2, "idx": 2, "name": "games", "output": "DP-1", "is_active": false}
        ]);
        let layout = json!({"window_size": [400, 300], "window_offset_in_tile": [4.0, 4.0], "tile_pos_in_workspace_view": null});
        let mut floating_layout = layout.clone();
        floating_layout["tile_pos_in_workspace_view"] = json!([10.0, 20.0]);
        let windows = json!([
            {"id": 7, "title": "Tiled", "app_id": "a", "pid": 1, "workspace_id": 1, "is_focused": true, "is_floating": false, "layout": layout},
            {"id": 8, "title": "Float", "app_id": "b", "pid": 2, "workspace_id": 1, "is_focused": false, "is_floating": true, "layout": floating_layout},
            {"id": 9, "title": "Hidden", "app_id": "UnrealEditor", "pid": 3, "workspace_id": 2, "is_focused": false, "is_floating": true, "layout": floating_layout}
        ]);
        let listed = niri_windows(
            windows.as_array().unwrap(),
            workspaces.as_array().unwrap(),
            outputs.as_object().unwrap(),
        );
        let [tiled, floating, hidden] = &listed[..] else {
            panic!("{listed:?}")
        };
        assert_eq!(
            (
                tiled.id.as_str(),
                tiled.geometry,
                tiled.visible,
                tiled.focused
            ),
            ("niri:7", None, None, true)
        );
        // (output x + tile x + border) * desktop scale
        assert_eq!(
            floating.geometry,
            Some(Rect {
                x: 228,
                y: 48,
                width: 800,
                height: 600
            })
        );
        assert_eq!(floating.visible, Some(true));
        assert_eq!(
            (hidden.visible, hidden.geometry, &hidden.workspace),
            (Some(false), None, &json!("games"))
        );
    }

    #[test]
    fn sway_leaf_geometry_is_absolute_content_rect() {
        let tree = json!({"type": "root", "nodes": [{"type": "output", "name": "HDMI-A-1", "scale": 2.0, "nodes": [
            {"type": "workspace", "name": "3", "nodes": [
                {"type": "con", "id": 41, "name": "Game", "app_id": null, "pid": 77, "focused": true, "visible": true,
                 "window_properties": {"class": "UnrealEditor"}, "window": 123,
                 "rect": {"x": 10, "y": 30, "width": 500, "height": 400},
                 "window_rect": {"x": 2, "y": 20, "width": 496, "height": 378}, "nodes": [], "floating_nodes": []}],
             "floating_nodes": []}]}]});
        let listed = sway_windows(&tree);
        let [game] = &listed[..] else {
            panic!("{listed:?}")
        };
        assert_eq!(game.id, "sway:41");
        assert_eq!(game.app_id.as_deref(), Some("UnrealEditor"));
        assert_eq!(
            (&game.workspace, game.output.as_deref()),
            (&json!("3"), Some("HDMI-A-1"))
        );
        assert_eq!(
            game.geometry,
            Some(Rect {
                x: 24,
                y: 100,
                width: 992,
                height: 756
            })
        );
    }

    #[test]
    fn hyprland_visibility_follows_monitor_workspaces() {
        let monitors = json!([{"id": 0, "name": "DP-2", "scale": 1.0, "activeWorkspace": {"id": 1}, "specialWorkspace": {"id": 0}}]);
        let clients = json!([
            {"address": "0xabc", "mapped": true, "hidden": false, "at": [5, 6], "size": [700, 500],
             "workspace": {"id": 1, "name": "1"}, "monitor": 0, "class": "steam_app_1", "title": "Game",
             "pid": 9, "focusHistoryID": 0},
            {"address": "0xdef", "mapped": true, "at": [0, 0], "size": [1, 1], "workspace": {"id": 4, "name": "4"},
             "monitor": 0, "class": "x", "title": "Other", "pid": 10, "focusHistoryID": 3}
        ]);
        let listed = hyprland_windows(clients.as_array().unwrap(), monitors.as_array().unwrap());
        let [game, other] = &listed[..] else {
            panic!("{listed:?}")
        };
        assert_eq!(
            (game.focused, game.visible, game.geometry.unwrap().x),
            (true, Some(true), 5)
        );
        assert_eq!((other.focused, other.visible), (false, Some(false)));
    }

    #[test]
    fn xprop_values_and_ewmh_window() {
        let root = xprop_values(
            "_NET_CLIENT_LIST(WINDOW): window id # 0x1a00003, 0x2c00007\n\
             _NET_ACTIVE_WINDOW(WINDOW): window id # 0x2c00007\n",
        );
        assert_eq!(
            root["_NET_CLIENT_LIST"],
            [XValue::Int(0x1a00003), XValue::Int(0x2c00007)]
        );
        let props = xprop_values(
            "_NET_WM_NAME(UTF8_STRING) = \"Unreal \\\"Editor\\\", 5.4\"\n\
             WM_CLASS(STRING) = \"UnrealEditor\", \"UnrealEditor\"\n\
             _NET_WM_PID(CARDINAL) = 4242\n\
             _NET_WM_STATE(ATOM) = _NET_WM_STATE_FOCUSED\n",
        );
        let geometry =
            xdotool_geometry("WINDOW=46137351\nX=12\nY=34\nWIDTH=800\nHEIGHT=600\nSCREEN=0\n");
        let active = match root["_NET_ACTIVE_WINDOW"][0] {
            XValue::Int(id) => Some(id),
            XValue::Str(_) => None,
        };
        let entry = x11_window(0x2c00007, &props, geometry, active);
        assert_eq!(
            (
                entry.id.as_str(),
                entry.title.as_str(),
                entry.app_id.as_deref(),
                entry.pid,
                entry.focused
            ),
            (
                "x11:0x2c00007",
                "Unreal \"Editor\", 5.4",
                Some("UnrealEditor"),
                Some(4242),
                true
            )
        );
        assert_eq!(
            entry.geometry,
            Some(Rect {
                x: 12,
                y: 34,
                width: 800,
                height: 600
            })
        );
    }

    #[test]
    fn lswt_toplevels() {
        let listed = lswt_windows(
            &json!({"toplevels": [{"title": "T", "app-id": "foot", "identifier": "abc", "activated": true}]}),
        );
        let [item] = &listed[..] else {
            panic!("{listed:?}")
        };
        assert_eq!(
            (
                item.id.as_str(),
                item.app_id.as_deref(),
                item.focused,
                item.toplevel_identifier.as_deref()
            ),
            ("toplevel:abc", Some("foot"), true, Some("abc"))
        );
    }

    fn window(native: i64, pid: i64, title: &str, focused: bool) -> CompositorWindow {
        CompositorWindow {
            backend: "niri",
            native_id: json!(native),
            id: format!("niri:{native}"),
            title: title.into(),
            app_id: None,
            pid: Some(pid),
            workspace: Value::Null,
            output: None,
            focused,
            floating: None,
            visible: None,
            geometry: None,
            size: None,
            toplevel_identifier: None,
            scale: 1.0,
        }
    }

    fn ids(matches: &HashMap<String, CompositorWindow>) -> Vec<(String, String)> {
        let mut ids: Vec<_> = matches
            .iter()
            .map(|(key, entry)| (key.clone(), entry.id.clone()))
            .collect();
        ids.sort();
        ids
    }

    fn accessible(key: &str, pid: i64, title: &str, active: bool) -> AccessibleWindow {
        (key.into(), Some(pid), title.into(), active)
    }

    #[test]
    fn correlation_uses_pid_title_and_decorated_titles() {
        let compositor = [
            window(1, 10, "⠋ Borg Agent • ~/x", false),
            window(2, 10, "🔔 Borg Agent • ~", false),
            window(3, 20, "Solo", false),
            window(4, 99, "gedit X11 via bridge", false),
        ];
        let matches = correlate(
            &[
                accessible("a", 10, "⠙ Borg Agent • ~/x", false),
                accessible("b", 10, "Borg Agent • ~", false),
                accessible("c", 20, "renamed", false),
                accessible("d", 5, "gedit X11 via bridge", false),
            ],
            &compositor,
        );
        let expected: Vec<(String, String)> = [
            ("a", "niri:1"),
            ("b", "niri:2"),
            ("c", "niri:3"),
            ("d", "niri:4"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .to_vec();
        assert_eq!(ids(&matches), expected);
    }

    #[test]
    fn ambiguous_titles_are_not_guessed_unless_focus_decides() {
        let mut compositor = [window(1, 10, "Same", false), window(2, 10, "Same", false)];
        let pair = [
            accessible("a", 10, "Same", false),
            accessible("b", 10, "Same", false),
        ];
        assert!(correlate(&pair, &compositor).is_empty());
        compositor[1].focused = true;
        let matches = correlate(
            &[
                accessible("a", 10, "Same", false),
                accessible("b", 10, "Same", true),
            ],
            &compositor,
        );
        assert_eq!(
            ids(&matches),
            [("a", "niri:1"), ("b", "niri:2")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .to_vec()
        );
    }

    #[test]
    fn element_point_wayland_relative_and_x11_absolute() {
        let element = Rect {
            x: 40,
            y: 10,
            width: 20,
            height: 10,
        };
        // Wayland: extents relative to the toplevel (window at 0,0), 2x scale.
        assert_eq!(
            element_point(element, (0.0, 0.0), (1000.0, 200.0), 2.0),
            (1100.0, 230.0)
        );
        // X11-style absolute extents: only the offset from the window matters.
        let absolute = Rect {
            x: 540,
            y: 310,
            width: 20,
            height: 10,
        };
        assert_eq!(
            element_point(absolute, (500.0, 300.0), (500.0, 300.0), 1.0),
            (550.0, 315.0)
        );
    }

    #[test]
    fn window_point_undoes_screenshot_downscale() {
        assert_eq!(
            window_point(50.0, 25.0, (100.0, 200.0), 0.5),
            (200.0, 250.0)
        );
    }

    #[test]
    fn split_motion_is_exact_and_smooth() {
        for (dx, dy, steps) in [
            (100.0, -37.0, 7),
            (3.0, 0.0, 10),
            (-500.0, 250.4, 50),
            (0.0, 1.0, 1),
        ] {
            let events = split_motion(dx, dy, steps);
            assert_eq!(events.len(), steps as usize);
            let sum = events.iter().fold((0i64, 0i64), |(x, y), e| {
                (x + i64::from(e.0), y + i64::from(e.1))
            });
            assert_eq!(sum, (py_round(dx), py_round(dy)));
            let widest = events.iter().map(|e| e.0.unsigned_abs()).max().unwrap();
            assert!(f64::from(widest) <= (dx.abs() / f64::from(steps)).ceil() + 1.0);
        }
    }

    #[test]
    fn clipboard_restore_prefers_text() {
        let owned = |types: &[&str]| types.iter().map(|t| t.to_string()).collect::<Vec<_>>();
        assert_eq!(
            clipboard_restore_type(&owned(&[
                "image/png",
                "text/plain",
                "text/plain;charset=utf-8"
            ]))
            .as_deref(),
            Some("text/plain;charset=utf-8")
        );
        assert_eq!(
            clipboard_restore_type(&owned(&["image/jpeg", "image/png"])).as_deref(),
            Some("image/png")
        );
        assert_eq!(clipboard_restore_type(&[]), None);
    }

    /// Deterministic noise for localisation fixtures.
    struct Noise(u64);

    impl Noise {
        fn image(&mut self, width: usize, height: usize) -> Rgb {
            let data = (0..width * height * 3)
                .map(|_| {
                    self.0 ^= self.0 << 13;
                    self.0 ^= self.0 >> 7;
                    self.0 ^= self.0 << 17;
                    (self.0 >> 24) as u8
                })
                .collect();
            Rgb {
                width,
                height,
                data,
            }
        }
    }

    fn paste(
        target: &mut Rgb,
        source: &Rgb,
        at: (usize, usize),
        from: (usize, usize, usize, usize),
    ) {
        let (sx, sy, sw, sh) = from;
        for row in 0..sh {
            for col in 0..sw {
                let src = ((sy + row) * source.width + sx + col) * 3;
                let dst = ((at.1 + row) * target.width + at.0 + col) * 3;
                target.data[dst..dst + 3].copy_from_slice(&source.data[src..src + 3]);
            }
        }
    }

    fn fill(target: &mut Rgb, rect: (usize, usize, usize, usize), colour: [u8; 3]) {
        let (x, y, w, h) = rect;
        for row in y..y + h {
            for col in x..x + w {
                let at = (row * target.width + col) * 3;
                target.data[at..at + 3].copy_from_slice(&colour);
            }
        }
    }

    fn at(found: Option<Located>) -> Option<(i64, i64)> {
        found.map(|found| (found.x, found.y))
    }

    #[test]
    fn locates_a_window_despite_occlusion_and_an_offscreen_part() {
        let mut noise = Noise(7);
        let mut desktop = noise.image(400, 300);
        let window = noise.image(160, 120);
        paste(&mut desktop, &window, (230, 50), (0, 0, 160, 120));
        fill(&mut desktop, (240, 60, 60, 40), [0, 0, 0]); // a popup over part of it
        assert_eq!(at(locate_window(&window, &desktop, None)), Some((230, 50)));
        // Scrolled partly off the left edge.
        let mut clipped = noise.image(400, 300);
        paste(&mut clipped, &window, (0, 100), (60, 0, 100, 120));
        assert_eq!(at(locate_window(&window, &clipped, None)), Some((-60, 100)));
    }

    #[test]
    fn repetitive_content_localises_only_by_its_unique_parts() {
        // Flat cells with dark borders, like a grid or tile map.
        let mut window = Rgb {
            width: 240,
            height: 160,
            data: vec![0; 240 * 160 * 3],
        };
        for ty in 0..4 {
            for tx in 0..6 {
                fill(
                    &mut window,
                    (tx * 40 + 2, ty * 40 + 2, 36, 36),
                    [40, 90, 160],
                );
            }
        }
        let mut desktop = Rgb {
            width: 600,
            height: 400,
            data: vec![0; 600 * 400 * 3],
        };
        paste(&mut desktop, &window, (200, 100), (0, 0, 240, 160));
        // Every period is an equally good answer: refuse.
        assert_eq!(at(locate_window(&window, &desktop, None)), None);
        let mut labels = Noise(2);
        for (y, x) in [(5, 60), (70, 150), (120, 20)] {
            let label = labels.image(60, 8);
            paste(&mut window, &label, (x, y), (0, 0, 60, 8));
        }
        paste(&mut desktop, &window, (200, 100), (0, 0, 240, 160));
        assert_eq!(at(locate_window(&window, &desktop, None)), Some((200, 100)));
    }

    #[test]
    fn refuses_absent_or_featureless_windows() {
        let mut noise = Noise(3);
        let mut desktop = noise.image(200, 200);
        let absent = noise.image(60, 50);
        assert_eq!(at(locate_window(&absent, &desktop, None)), None);
        let flat = Rgb {
            width: 60,
            height: 50,
            data: vec![30; 60 * 50 * 3],
        };
        paste(&mut desktop, &flat, (10, 10), (0, 0, 60, 50));
        assert_eq!(at(locate_window(&flat, &desktop, None)), None);
    }
}
