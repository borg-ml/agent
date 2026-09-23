//! AT-SPI2 over the accessibility bus with plain (uncached) D-Bus proxies.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use atspi::proxy::accessible::AccessibleProxyBlocking;
use atspi::proxy::action::ActionProxyBlocking;
use atspi::proxy::bus::BusProxyBlocking;
use atspi::proxy::component::ComponentProxyBlocking;
use atspi::proxy::editable_text::EditableTextProxyBlocking;
use atspi::proxy::text::TextProxyBlocking;
use atspi::{CoordType, Interface, InterfaceSet, ObjectRefOwned, Role, State, StateSet};
use serde_json::{Map, Value, json};
use zbus::blocking::Connection;
use zbus::proxy::CacheProperties;

use super::windows::{self, AccessibleWindow};
use super::{Helper, Win, session_type, window_id};

const MAX_HANDLES: usize = 10_000;

/// An accessible object: its application's bus name and object path.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(in crate::computer_use) struct Obj {
    name: String,
    path: String,
}

impl Obj {
    fn root() -> Self {
        Self {
            name: "org.a11y.atspi.Registry".into(),
            path: "/org/a11y/atspi/accessible/root".into(),
        }
    }

    fn from_ref(object: &ObjectRefOwned) -> Option<Self> {
        Some(Self {
            name: object.name_as_str()?.to_string(),
            path: object.path_as_str().to_string(),
        })
    }
}

pub(super) struct Observation {
    pub id: String,
    /// Nodes in breadth-first order.
    pub nodes: Vec<(String, Value)>,
}

pub(super) struct A11y {
    conn: Connection,
    dbus: zbus::blocking::fdo::DBusProxy<'static>,
}

macro_rules! proxy {
    ($a11y:expr, $kind:ident, $obj:expr) => {
        $kind::builder(&$a11y.conn)
            .destination($obj.name.clone())?
            .path($obj.path.clone())?
            .cache_properties(CacheProperties::No)
            .build()
    };
}

impl A11y {
    fn connect() -> Result<Self> {
        let address = match std::env::var("AT_SPI_BUS_ADDRESS") {
            Ok(address) if !address.is_empty() => address,
            _ => {
                let session = Connection::session().context("no D-Bus session bus")?;
                BusProxyBlocking::new(&session)?
                    .get_address()
                    .context("the accessibility bus is unavailable (is at-spi2-core running?)")?
            }
        };
        let conn = zbus::blocking::connection::Builder::address(address.as_str())?
            .method_timeout(Duration::from_millis(1500))
            .build()
            .context("cannot connect to the accessibility bus")?;
        let dbus = zbus::blocking::fdo::DBusProxy::new(&conn)?;
        Ok(Self { conn, dbus })
    }

    fn accessible(&self, obj: &Obj) -> Result<AccessibleProxyBlocking<'static>> {
        Ok(proxy!(self, AccessibleProxyBlocking, obj)?)
    }

    pub fn state(&self, obj: &Obj) -> Result<StateSet> {
        Ok(self.accessible(obj)?.get_state()?)
    }

    pub fn alive(&self, obj: &Obj) -> bool {
        self.state(obj)
            .is_ok_and(|state| !state.contains(State::Defunct))
    }

    pub fn active(&self, obj: &Obj) -> bool {
        self.state(obj)
            .is_ok_and(|state| state.contains(State::Active))
    }

    pub fn name(&self, obj: &Obj) -> Result<String> {
        Ok(self.accessible(obj)?.name()?)
    }

    pub fn role(&self, obj: &Obj) -> Result<Role> {
        Ok(self.accessible(obj)?.get_role()?)
    }

    fn interfaces(&self, obj: &Obj) -> InterfaceSet {
        self.accessible(obj)
            .and_then(|proxy| Ok(proxy.get_interfaces()?))
            .unwrap_or_default()
    }

    /// Up to `limit` children, in order.
    pub fn children(&self, obj: &Obj, limit: usize) -> Result<Vec<Obj>> {
        let proxy = self.accessible(obj)?;
        let count = usize::try_from(proxy.child_count()?).unwrap_or(0);
        if count <= limit
            && let Ok(children) = proxy.get_children()
        {
            return Ok(children.iter().filter_map(Obj::from_ref).collect());
        }
        (0..count.min(limit))
            .map(|index| {
                let child = proxy.get_child_at_index(index as i32)?;
                Obj::from_ref(&child).context("null child")
            })
            .collect()
    }

    pub fn child_count(&self, obj: &Obj) -> usize {
        self.accessible(obj)
            .and_then(|proxy| Ok(proxy.child_count()?))
            .map_or(0, |count| usize::try_from(count).unwrap_or(0))
    }

    pub fn parent(&self, obj: &Obj) -> Result<Option<Obj>> {
        let parent = self.accessible(obj)?.parent()?;
        Ok(Obj::from_ref(&parent).filter(|parent| parent.path != "/org/a11y/atspi/null"))
    }

    pub fn pid(&self, app: &Obj) -> Option<i64> {
        let name = zbus::names::BusName::try_from(app.name.as_str()).ok()?;
        self.dbus
            .get_connection_unix_process_id(name)
            .ok()
            .map(i64::from)
    }

    pub fn extents(&self, obj: &Obj, coord: CoordType) -> Result<(i32, i32, i32, i32)> {
        let component: ComponentProxyBlocking = proxy!(self, ComponentProxyBlocking, obj)?;
        Ok(component.get_extents(coord)?)
    }

    pub fn grab_focus(&self, obj: &Obj) -> Result<bool> {
        let component: ComponentProxyBlocking = proxy!(self, ComponentProxyBlocking, obj)?;
        Ok(component.grab_focus()?)
    }

    fn text(&self, obj: &Obj) -> Result<String> {
        let text: TextProxyBlocking = proxy!(self, TextProxyBlocking, obj)?;
        let count = text.character_count()?;
        Ok(text.get_text(0, count.min(2048))?)
    }

    pub fn action_names(&self, obj: &Obj) -> Result<Vec<String>> {
        let action: ActionProxyBlocking = proxy!(self, ActionProxyBlocking, obj)?;
        // GetName is the machine-readable name ("click"); GetActions
        // reports the localised display names.
        (0..action.n_actions()?)
            .map(|index| Ok(action.get_name(index)?))
            .collect()
    }

    pub fn do_action(&self, obj: &Obj, index: usize) -> Result<bool> {
        let action: ActionProxyBlocking = proxy!(self, ActionProxyBlocking, obj)?;
        Ok(action.do_action(index as i32)?)
    }

    pub fn set_text(&self, obj: &Obj, text: &str) -> Result<bool> {
        let editable: EditableTextProxyBlocking = proxy!(self, EditableTextProxyBlocking, obj)?;
        Ok(editable.set_text_contents(text)?)
    }

    /// Top-level applications on the accessibility bus.
    pub fn applications(&self) -> Result<Vec<Obj>> {
        self.children(&Obj::root(), 256)
    }
}

/// Wayland toolkits cannot know their screen position (GTK4 reports 0,0), but
/// window-relative extents are exact; X11 screen extents are real pixels.
pub(super) fn extents_type() -> CoordType {
    if session_type() == Some("wayland") {
        CoordType::Window
    } else {
        CoordType::Screen
    }
}

/// GTK4 exposes SENSITIVE without ENABLED for usable widgets.
pub(super) fn enabled(state: StateSet) -> bool {
    state.contains(State::Enabled) || state.contains(State::Sensitive)
}

fn truncate(text: &str, chars: usize) -> String {
    text.chars().take(chars).collect()
}

impl Helper {
    pub(super) fn a11y(&mut self) -> Result<&A11y> {
        if self.a11y.is_none() {
            self.a11y = Some(A11y::connect()?);
        }
        Ok(self.a11y.as_ref().expect("connected above"))
    }

    fn identify(&mut self, obj: &Obj) -> Result<String> {
        if let Some(key) = self.object_ids.get(obj) {
            return Ok(key.clone());
        }
        ensure!(
            self.objects.len() < MAX_HANDLES,
            "element handle limit reached; restart the desktop session"
        );
        self.next_id += 1;
        let key = format!("{}:{}", self.epoch, self.next_id);
        self.objects.insert(key.clone(), obj.clone());
        self.object_ids.insert(obj.clone(), key.clone());
        Ok(key)
    }

    fn describe(&mut self, obj: &Obj, parent: Option<&str>) -> Result<Value> {
        let id = self.identify(obj)?;
        let a11y = self.a11y()?;
        let state = a11y.state(obj)?;
        let role = a11y.role(obj)?;
        let mut node = json!({
            "id": id,
            "parent": parent,
            "role": role.name(),
            "name": truncate(&a11y.name(obj)?, 1024),
            "enabled": enabled(state),
            "focused": state.contains(State::Focused),
            "showing": state.contains(State::Showing),
        });
        let interfaces = a11y.interfaces(obj);
        if interfaces.contains(Interface::Component)
            && let Ok((x, y, width, height)) = a11y.extents(obj, extents_type())
            && width > 0
            && height > 0
        {
            node["bounds"] = json!({"x": x, "y": y, "width": width, "height": height});
        }
        if role != Role::PasswordText
            && interfaces.contains(Interface::Text)
            && let Ok(text) = a11y.text(obj)
        {
            node["text"] = json!(text);
        }
        if interfaces.contains(Interface::Action)
            && let Ok(actions) = a11y.action_names(obj)
        {
            node["actions"] = json!(actions);
        }
        Ok(node)
    }

    /// Breadth-first tree of up to `limit` nodes, and whether it was cut short.
    pub(super) fn tree(&mut self, win: &Obj, limit: usize) -> Result<(Vec<(String, Value)>, bool)> {
        let mut nodes: Vec<(String, Value)> = Vec::new();
        let mut queue: VecDeque<(Obj, Option<String>, usize)> =
            VecDeque::from([(win.clone(), None, 0)]);
        let mut truncated = false;
        while nodes.len() < limit {
            let Some((obj, parent, depth)) = queue.pop_front() else {
                break;
            };
            if !self.a11y()?.alive(&obj) {
                continue;
            }
            let node = self.describe(&obj, parent.as_deref())?;
            let id = node["id"].as_str().unwrap_or_default().to_string();
            nodes.push((id.clone(), node));
            let a11y = self.a11y()?;
            let children = a11y.child_count(&obj);
            let budget = if depth < 32 {
                limit.saturating_sub(nodes.len() + queue.len())
            } else {
                0
            };
            truncated |= children > budget;
            if budget > 0 && children > 0 {
                for child in a11y.children(&obj, budget).unwrap_or_default() {
                    queue.push_back((child, Some(id.clone()), depth + 1));
                }
            }
        }
        Ok((nodes, truncated || !queue.is_empty()))
    }

    /// AT-SPI windows merged with compositor-listed ones (which may have no
    /// accessibility tree).
    pub(super) fn windows(&mut self) -> Result<Vec<Value>> {
        let private_pids: HashSet<i64> = self
            .private_windows(false)?
            .iter()
            .filter_map(|w| w["pid"].as_i64())
            .collect();
        let mut result = Vec::new();
        let mut accessible: Vec<AccessibleWindow> = Vec::new();
        let listed_windows: Vec<(Obj, Option<i64>, String)> = match self.a11y() {
            Ok(a11y) => {
                let mut found = Vec::new();
                for app in a11y.applications().unwrap_or_default() {
                    let pid = a11y.pid(&app);
                    if pid.is_some_and(|pid| private_pids.contains(&pid)) {
                        continue; // listed under display=private
                    }
                    let app_name = a11y.name(&app).unwrap_or_default();
                    for win in a11y.children(&app, 256).unwrap_or_default() {
                        if a11y.alive(&win) {
                            found.push((win, pid, app_name.clone()));
                        }
                    }
                }
                found
            }
            Err(_) => Vec::new(),
        };
        for (win, pid, application) in listed_windows {
            let id = self.identify(&win)?;
            let a11y = self.a11y()?;
            let title = a11y.name(&win).unwrap_or_default();
            let active = a11y.active(&win);
            accessible.push((id.clone(), pid, title.clone(), active));
            result.push(json!({"id": id, "title": title, "application": application,
                               "active": active, "accessible": true}));
        }
        self.compositor.clear();
        self.accessible_compositor.clear();
        let listed = match windows::compositor_list(None) {
            Ok(listed) => {
                self.compositor_error = None;
                listed
            }
            // A broken compositor IPC must not hide accessible windows.
            Err(error) => {
                self.compositor_error = Some(truncate(&format!("{error:#}"), 512));
                Vec::new()
            }
        };
        let matches = windows::correlate(&accessible, &listed);
        for entry in &mut result {
            let id = entry["id"].as_str().unwrap_or_default().to_string();
            if let Some(matched) = matches.get(&id) {
                entry["compositor"] = matched.public();
                self.accessible_compositor.insert(id, matched.clone());
            }
        }
        let matched: HashSet<&str> = matches.values().map(|m| m.id.as_str()).collect();
        for item in &listed {
            self.compositor.insert(item.id.clone(), item.clone());
            if !matched.contains(item.id.as_str()) {
                result.push(
                    json!({"id": item.id, "title": item.title, "application": item.app_id,
                                   "active": item.focused, "accessible": false,
                                   "compositor": item.public()}),
                );
            }
        }
        Ok(result)
    }

    pub(super) fn window(&mut self, key: &str) -> Result<Win> {
        if key.starts_with(super::private::PREFIX) {
            let info = self.private_window(key)?;
            return self.private_accessible(&info).map(Win::Accessible).ok_or_else(|| {
                anyhow!("this private-display window exposes no accessibility tree; use a private screenshot with pointer/key input instead")
            });
        }
        let listed = self.windows()?;
        ensure!(
            listed.iter().any(|w| w["id"] == key),
            "stale or unknown window_id; list_windows again"
        );
        if let Some(entry) = self.compositor.get(key) {
            return Ok(Win::Compositor(Box::new(entry.clone())));
        }
        Ok(Win::Accessible(
            self.objects
                .get(key)
                .cloned()
                .context("unknown window handle")?,
        ))
    }

    /// Fresh compositor entry for a window id (compositor-only or correlated
    /// AT-SPI), if the compositor lists it.
    pub(super) fn compositor_for(
        &mut self,
        wid: &str,
    ) -> Result<Option<windows::CompositorWindow>> {
        self.windows()?;
        Ok(self
            .compositor
            .get(wid)
            .or_else(|| self.accessible_compositor.get(wid))
            .cloned())
    }

    /// The AT-SPI window of a private-display app (same accessibility bus,
    /// matched by pid and title).
    pub(super) fn private_accessible(
        &mut self,
        info: &super::private::PrivateWindow,
    ) -> Option<Obj> {
        let pid = info.pid?;
        let a11y = self.a11y().ok()?;
        let mut candidates = Vec::new();
        for app in a11y.applications().ok()? {
            if a11y.pid(&app) != Some(pid) {
                continue;
            }
            for win in a11y.children(&app, 256).unwrap_or_default() {
                if a11y.alive(&win) {
                    candidates.push(win);
                }
            }
        }
        let titled: Vec<Obj> = candidates
            .iter()
            .filter(|w| a11y.name(w).unwrap_or_default() == info.title)
            .cloned()
            .collect();
        let chosen = if titled.is_empty() {
            candidates
        } else {
            titled
        };
        match &chosen[..] {
            [only] => Some(only.clone()),
            _ => None,
        }
    }

    pub(super) fn snapshot(&mut self, args: &Value) -> Result<Value> {
        let wid = window_id(args)?;
        let win = self.window(&wid)?;
        let limit = match args.get("max_nodes") {
            None | Some(Value::Null) => 300,
            Some(value) => value
                .as_i64()
                .filter(|v| (1..=1000).contains(v))
                .context("max_nodes must be between 1 and 1000")?
                as usize,
        };
        let (nodes, truncated) = match &win {
            Win::Compositor(_) => (Vec::new(), false),
            Win::Accessible(obj) => self.tree(obj, limit)?,
        };
        let token = uuid::Uuid::new_v4().simple().to_string();
        let requested = args
            .get("since")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let previous = self.observations.get(&wid);
        if let Some(requested) = requested
            && previous.is_none_or(|p| p.id != requested)
        {
            bail!("unknown diff baseline; observe without since");
        }
        let coordinate_space = if wid.starts_with(super::private::PREFIX) {
            "window-relative AT-SPI coordinates; add the window bounds origin for private display pixels"
        } else if session_type() == Some("wayland") {
            "AT-SPI window-relative logical coordinates (Wayland)"
        } else {
            "AT-SPI screen logical coordinates"
        };
        let mut result = json!({"window_id": wid, "observation_id": token, "truncated": truncated,
                                "coordinate_space": coordinate_space});
        if requested.is_some() {
            let old: HashMap<&str, &Value> = previous
                .map(|p| p.nodes.iter().map(|(k, v)| (k.as_str(), v)).collect())
                .unwrap_or_default();
            let current: HashSet<&str> = nodes.iter().map(|(k, _)| k.as_str()).collect();
            result["changed"] = nodes
                .iter()
                .filter(|(k, n)| old.get(k.as_str()) != Some(&n))
                .map(|(_, n)| n.clone())
                .collect();
            result["removed"] = previous
                .map(|p| {
                    p.nodes
                        .iter()
                        .filter(|(k, _)| !current.contains(k.as_str()))
                        .map(|(k, _)| json!(k))
                        .collect()
                })
                .unwrap_or_default();
        } else {
            result["nodes"] = nodes.iter().map(|(_, n)| n.clone()).collect();
        }
        if let Win::Compositor(entry) = &win {
            result["accessible"] = json!(false);
            result["compositor"] = entry.public();
            result["note"] = json!(
                "This window exposes no accessibility tree; use screenshot scope=window and coordinate input."
            );
        }
        self.observations
            .insert(wid.clone(), Observation { id: token, nodes });
        if args.get("screenshot").is_some_and(truthy) {
            let shot = if wid.starts_with(super::private::PREFIX) {
                let scope = args
                    .get("screenshot_scope")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("window");
                self.private_screenshot(&json!({"scope": scope, "window_id": wid}))?
            } else {
                self.screenshot(args.get("screenshot_scope"), Some(&json!(wid)))?
            };
            merge(&mut result, shot);
        }
        Ok(result)
    }

    /// The observed element an action targets, re-validated against the tree.
    pub(super) fn target(&mut self, args: &Value) -> Result<(Win, Obj)> {
        let wid = window_id(args)?;
        let win = self.window(&wid)?;
        let Win::Accessible(win_obj) = &win else {
            bail!(
                "this window exposes no accessibility tree, so it has no elements; use coordinate input (x, y with coordinate_space=window) from a window screenshot"
            );
        };
        let observed = self
            .observations
            .get(&wid)
            .filter(|o| args.get("observation_id").and_then(Value::as_str) == Some(o.id.as_str()))
            .context("stale observation_id; observe the window again before acting")?;
        let key = args
            .get("element_id")
            .and_then(Value::as_str)
            .context("element_id is required")?;
        let node = observed
            .nodes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, node)| node.clone())
            .context("element_id was not present in this observation")?;
        let obj = self.objects[key].clone();
        let a11y = self.a11y()?;
        let mut cursor = obj.clone();
        let mut found = false;
        for _ in 0..64 {
            if &cursor == win_obj {
                found = true;
                break;
            }
            cursor = a11y
                .parent(&cursor)?
                .context("element no longer belongs to this window; observe again")?;
        }
        ensure!(found, "element ancestry is too deep");
        let alive = a11y.alive(&obj);
        let parent = node["parent"].as_str().map(str::to_string);
        ensure!(
            alive && self.describe(&obj, parent.as_deref())? == node,
            "element changed since observation; observe again"
        );
        ensure!(enabled(self.a11y()?.state(&obj)?), "element is disabled");
        Ok((win, obj))
    }

    pub(super) fn mutate(&mut self, args: &Value) -> Result<Value> {
        let (win, obj) = self.target(args)?;
        let op = args["op"].as_str().unwrap_or_default();
        let wid = window_id(args)?;
        // Consume the observation BEFORE issuing an effect, including failed effects.
        self.observations.remove(&wid);
        let a11y = self.a11y()?;
        match op {
            "click" => {
                let names = a11y.action_names(&obj).unwrap_or_default();
                let index = names
                    .iter()
                    .position(|name| {
                        matches!(name.to_lowercase().as_str(), "click" | "press" | "activate")
                    })
                    .context(
                        "element has no semantic click action; no coordinate fallback performed",
                    )?;
                ensure!(a11y.do_action(&obj, index)?, "AT-SPI action was rejected");
            }
            "set_value" => {
                let text = args
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| text.chars().count() <= 16384)
                    .context("text must be a string of at most 16384 characters")?;
                ensure!(
                    a11y.role(&obj)? != Role::PasswordText,
                    "password entry requires a human"
                );
                ensure!(
                    a11y.set_text(&obj, text)?,
                    "AT-SPI rejected text replacement"
                );
            }
            _ => bail!("unsupported operation: {op}"),
        }
        let restored = self.restore_focus(args, &wid);
        self.settle_and_snapshot(&win, &wid, op, json!({"restored_focus": restored}))
    }

    pub(super) fn settle_and_snapshot(
        &mut self,
        win: &Win,
        wid: &str,
        op: &str,
        extra: Value,
    ) -> Result<Value> {
        let (settled, verification) = match win {
            Win::Compositor(_) => {
                std::thread::sleep(Duration::from_millis(50));
                (
                    Value::Null,
                    "No accessibility tree: take screenshot scope=window to verify the effect.",
                )
            }
            Win::Accessible(obj) => {
                // Bounded settling: two matching trees; not a claim that
                // application work finished.
                let deadline = Instant::now() + Duration::from_millis(1500);
                let mut previous = None;
                let mut settled = false;
                while Instant::now() < deadline {
                    let (current, _) = self.tree(obj, 300)?;
                    if previous.as_ref() == Some(&current) {
                        settled = true;
                        break;
                    }
                    previous = Some(current);
                    std::thread::sleep(Duration::from_millis(100));
                }
                (
                    json!(settled),
                    "Inspect the returned tree for the requested application effect.",
                )
            }
        };
        let mut result = self.snapshot(&json!({"window_id": wid}))?;
        merge(
            &mut result,
            json!({"action": op, "dispatched": true, "tree_settled": settled,
                   "verification": verification}),
        );
        merge(&mut result, extra);
        Ok(result)
    }
}

pub(super) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// `dict.update`: copy every key of `extra` into `target`.
pub(super) fn merge(target: &mut Value, extra: Value) {
    if let (Some(target), Value::Object(extra)) = (target.as_object_mut(), extra) {
        let extra: Map<String, Value> = extra;
        target.extend(extra);
    }
}
