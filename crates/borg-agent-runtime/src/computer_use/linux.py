"""Borg-owned AT-SPI worker. JSONL on stdin/stdout; diagnostics on stderr."""
import base64
import json
import math
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from typing import TYPE_CHECKING

import gi

if TYPE_CHECKING:  # at runtime linux_windows.py is concatenated before this file
    from linux_windows import *  # noqa: F403

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # pyright: ignore[reportAttributeAccessIssue]

Atspi.set_timeout(1500, 3000)
EPOCH = uuid.uuid4().hex[:12]
objects = {}
object_ids = {}
next_id = 0
observations = {}
COMPOSITOR = {}  # compositor window id -> entry, refreshed by windows()
ACCESSIBLE_COMPOSITOR = {}  # AT-SPI window id -> correlated compositor entry
COMPOSITOR_STATE: dict = {"error": None}
WINDOW_CAPTURES = {}  # window id -> {"scale": image px per window px} of the latest window screenshot
FOCUS: dict = {"human": None, "borg": None, "changed_at": 0.0}  # the human's window before Borg moved focus, and Borg's last target
POINTER: dict = {"window": None}  # window the helper last placed the pointer in
LOCATED = {}  # window id -> {"image": decoded window capture, "origin": desktop origin} from the last localisation
PNG_SIGNATURE = bytes.fromhex("89504e470d0a1a0a")
MAX_IMAGE_BYTES = 4 * 1024 * 1024


def identify(obj):
    global next_id
    # AT-SPI proxies compare by their bus name and object path, not tree position.
    if obj in object_ids:
        return object_ids[obj]
    if len(objects) >= 10000:
        raise ValueError("element handle limit reached; restart the desktop session")
    next_id += 1
    key = f"{EPOCH}:{next_id}"
    objects[key] = obj
    object_ids[obj] = key
    return key


def states(obj):
    return obj.get_state_set()


def alive(obj):
    return not states(obj).contains(Atspi.StateType.DEFUNCT)


def windows():
    """AT-SPI windows merged with compositor-listed ones (which may have no accessibility tree)."""
    result, accessible = [], []
    private_pids = {w["pid"] for w in private_windows(accessibility=False)}
    desktop = Atspi.get_desktop(0)
    for ai in range(min(desktop.get_child_count(), 256)):
        app = desktop.get_child_at_index(ai)
        pid = app_pid(app)
        if private_pids and pid in private_pids:
            continue  # listed under display=private
        for wi in range(min(app.get_child_count(), 256)):
            win = app.get_child_at_index(wi)
            if alive(win):
                entry = {"id": identify(win), "title": win.get_name(), "application": app.get_name(),
                         "active": states(win).contains(Atspi.StateType.ACTIVE), "accessible": True}
                result.append(entry)
                accessible.append((entry["id"], pid, entry["title"], entry["active"]))
    COMPOSITOR.clear()
    ACCESSIBLE_COMPOSITOR.clear()
    try:
        listed = compositor_list()
        COMPOSITOR_STATE["error"] = None
    except Exception as error:  # a broken compositor IPC must not hide accessible windows
        listed = []
        COMPOSITOR_STATE["error"] = str(error)[:512]
    matches = correlate(accessible, listed)
    for entry in result:
        match = matches.get(entry["id"])
        if match:
            entry["compositor"] = public_window(match)
            ACCESSIBLE_COMPOSITOR[entry["id"]] = match
    matched = {m["id"] for m in matches.values()}
    for item in listed:
        COMPOSITOR[item["id"]] = item
        if item["id"] not in matched:
            result.append({"id": item["id"], "title": item["title"], "application": item["app_id"],
                           "active": item["focused"], "accessible": False, "compositor": public_window(item)})
    return result


def app_pid(app):
    try:
        return app.get_process_id()
    except Exception:
        return None


def window(key):
    if is_private(key):
        accessible = private_accessible(private_window(key))
        if accessible is None:
            raise ValueError("this private-display window exposes no accessibility tree; use a private screenshot "
                             "with pointer/key input instead")
        return accessible
    if key not in {w["id"] for w in windows()}:
        raise ValueError("stale or unknown window_id; list_windows again")
    return COMPOSITOR[key] if key in COMPOSITOR else objects[key]


def is_compositor(win):
    """Compositor-only windows are plain dicts; accessible windows are AT-SPI proxies."""
    return isinstance(win, dict)


PUBLIC_FIELDS = ("backend", "native_id", "title", "app_id", "pid", "workspace", "output", "focused", "floating",
                 "visible", "geometry", "size")


def public_window(entry):
    return {k: entry.get(k) for k in PUBLIC_FIELDS}


def compositor_for(wid):
    """Fresh compositor entry for a window id (compositor-only or correlated AT-SPI), or None."""
    windows()
    return COMPOSITOR.get(wid) or ACCESSIBLE_COMPOSITOR.get(wid)


def extents_type():
    """Wayland toolkits cannot know their screen position (GTK4 reports 0,0), but
    window-relative extents are exact; X11 screen extents are real pixels."""
    return Atspi.CoordType.WINDOW if session_type() == "wayland" else Atspi.CoordType.SCREEN


def enabled(state):
    # GTK4 exposes SENSITIVE without ENABLED for usable widgets.
    return state.contains(Atspi.StateType.ENABLED) or state.contains(Atspi.StateType.SENSITIVE)


def describe(obj, parent):
    state = states(obj)
    node = {"id": identify(obj), "parent": parent, "role": obj.get_role_name(),
            "name": (obj.get_name() or "")[:1024],
            "enabled": enabled(state),
            "focused": state.contains(Atspi.StateType.FOCUSED),
            "showing": state.contains(Atspi.StateType.SHOWING)}
    try:
        r = obj.get_component_iface().get_extents(extents_type())
        if r.width > 0 and r.height > 0:
            node["bounds"] = {"x": r.x, "y": r.y, "width": r.width, "height": r.height}
    except Exception:
        pass
    if obj.get_role() != Atspi.Role.PASSWORD_TEXT:
        try:
            text = obj.get_text_iface()
            node["text"] = Atspi.Text.get_text(text, 0, min(Atspi.Text.get_character_count(text), 2048))
        except Exception:
            pass
    try:
        action = obj.get_action_iface()
        node["actions"] = [action.get_action_name(i) for i in range(action.get_n_actions())]
    except Exception:
        pass
    return node


def tree(win, limit):
    win.clear_cache()
    nodes = {}
    queue = [(win, None, 0)]
    truncated = False
    while queue and len(nodes) < limit:
        obj, parent, depth = queue.pop(0)
        if not alive(obj):
            continue
        node = describe(obj, parent)
        nodes[node["id"]] = node
        children = obj.get_child_count()
        budget = max(0, limit - len(nodes) - len(queue)) if depth < 32 else 0
        truncated |= children > budget
        for i in range(min(children, budget)):
            queue.append((obj.get_child_at_index(i), node["id"], depth + 1))
    return nodes, truncated or bool(queue)


def png_size(data):
    if not data.startswith(PNG_SIGNATURE) or len(data) < 24:
        raise ValueError("capture did not return a PNG")
    return struct.unpack_from(">II", data, 16)


def attachment(data):
    return [{"media_type": "image/png", "data_base64": base64.b64encode(data).decode()}]


def screenshot(scope, wid=None):
    if scope == "window":
        if not isinstance(wid, str):
            raise ValueError("scope=window requires window_id")
        return window_screenshot(wid)
    if scope != "desktop":
        raise ValueError('scope must be "desktop" or "window" (window needs window_id)')
    capture = subprocess.run(["grim", "-"], capture_output=True, timeout=5)
    if capture.returncode:
        raise ValueError("desktop capture failed: " + capture.stderr.decode(errors="replace")[:1024])
    data = capture.stdout
    width, height = png_size(data)
    if len(data) > MAX_IMAGE_BYTES:
        raise ValueError("screenshot exceeds 4 MiB")
    global SCREEN
    SCREEN = (width, height)
    return {"scope": "desktop", "width": width, "height": height,
            "coordinate_space": "screenshot pixels, not AT-SPI screen coordinates",
            "borg_attachments": attachment(data)}


# ---- Window capture ---------------------------------------------------------

def run_bytes(command, timeout=5):
    run = subprocess.run(command, capture_output=True, timeout=timeout)
    if run.returncode:
        raise ValueError(f"{command[0]} failed: " + run.stderr.decode(errors="replace").strip()[:1024])
    return run.stdout


def clipboard_snapshot():
    """The current clipboard (one MIME type) so a compositor screenshot can be undone."""
    if not (shutil.which("wl-paste") and shutil.which("wl-copy")):
        return {"available": False}
    listing = subprocess.run(["wl-paste", "--list-types"], capture_output=True, text=True, timeout=3)
    types = listing.stdout.split() if listing.returncode == 0 else []
    kind = clipboard_restore_type(types)
    if kind is None:
        return {"available": True, "type": None}
    content = subprocess.run(["wl-paste", "--no-newline", "--type", kind], capture_output=True, timeout=3)
    if content.returncode or len(content.stdout) > 32 * 1024 * 1024:
        return {"available": True, "type": kind, "unreadable": True}
    return {"available": True, "type": kind, "data": content.stdout, "types": len(types)}


def clipboard_restore(saved, capture):
    """Put the saved clipboard back once the compositor's own image selection landed."""
    if not saved.get("available"):
        return "clipboard now holds the capture (install wl-clipboard so Borg can restore it)"
    if saved.get("unreadable"):
        return f"clipboard now holds the capture; the previous {saved['type']} content was too large or unreadable to restore"
    # niri sets its selection asynchronously; restoring earlier would be overwritten,
    # and if something else replaced it meanwhile (the human copied), leave it alone.
    deadline, landed = time.monotonic() + 1.0, False
    while capture and time.monotonic() < deadline:
        probe = subprocess.run(["wl-paste", "--no-newline", "--type", "image/png"], capture_output=True, timeout=3)
        if probe.returncode == 0 and probe.stdout == capture:
            landed = True
            break
        time.sleep(0.03)
    if capture and not landed:
        return "clipboard not restored: it no longer held the capture (it changed during the capture)"
    detached = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL, "start_new_session": True, "timeout": 3}
    if saved.get("type") is None:
        subprocess.run(["wl-copy", "--clear"], stdin=subprocess.DEVNULL, **detached)
        return "clipboard was empty and was cleared again"
    subprocess.run(["wl-copy", "--type", saved["type"]], input=saved["data"], **detached)
    extra = "" if saved.get("types", 1) <= 1 else " (other offered formats were not restored)"
    return f"clipboard restored as {saved['type']}{extra}"


def niri_capture(entry):
    """niri renders exactly this window's surfaces (on-screen or not) without changing focus.

    niri also copies every screenshot to the clipboard and may show a
    'Screenshot captured' notification; the clipboard is restored afterwards.
    """
    directory = tempfile.mkdtemp(prefix="borg-cua-")
    path = os.path.join(directory, "window.png")
    saved = clipboard_snapshot()
    data = None
    try:
        niri_request({"Action": {"ScreenshotWindow": {"id": entry["native_id"], "write_to_disk": True,
                                                      "show_pointer": False, "path": path}}})
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                with open(path, "rb") as handle:
                    candidate = handle.read()
                # niri writes the file non-atomically; wait for the IEND chunk.
                if candidate.startswith(PNG_SIGNATURE) and candidate.endswith(b"IEND\xaeB`\x82"):
                    data = candidate
                    break
            except FileNotFoundError:
                pass
            time.sleep(0.02)
    finally:
        note = clipboard_restore(saved, data)
        shutil.rmtree(directory, ignore_errors=True)
    if data is None:
        raise ValueError("niri did not write the window screenshot; the window may have closed")
    return data, "niri screenshot-window (isolated window surfaces)", [note, "niri may show a 'Screenshot captured' notification"]


def grim_region(geometry, scale):
    g = geometry
    spec = f"{g['x'] / scale:g},{g['y'] / scale:g} {g['width'] / scale:g}x{g['height'] / scale:g}"
    return run_bytes(["grim", "-g", spec, "-"])


def refreshed(entry):
    fresh = next((c for c in compositor_list(entry["backend"]) if c["id"] == entry["id"]), None)
    if fresh is None:
        raise ValueError("window closed; list_windows again")
    return fresh


def capture_compositor_window(entry):
    backend = entry["backend"]
    if backend == "niri":
        return niri_capture(entry)
    notes = []
    if entry.get("toplevel_identifier") and shutil.which("grim"):
        # ext-image-copy-capture: isolated even when hidden, where the compositor supports it.
        run = subprocess.run(["grim", "-T", entry["toplevel_identifier"], "-"], capture_output=True, timeout=5)
        if run.returncode == 0:
            return run.stdout, "grim -T (ext-image-copy-capture, isolated)", notes
        notes.append("grim -T unsupported here: " + run.stderr.decode(errors="replace").strip()[:200])
    if backend == "x11" and shutil.which("import"):
        data = run_bytes(["import", "-silent", "-window", f"{entry['native_id']:#x}", "png:-"])
        return data, "ImageMagick import -window (X11; overlapping windows can show through)", notes
    if backend in ("sway", "hyprland"):
        prior = None
        if not entry.get("visible"):
            # Bring it into view, capture, then put the human's focus back.
            prior = next((c for c in compositor_list(backend) if c["focused"]), None)
            compositor_focus(entry)
            time.sleep(0.25)
            entry = refreshed(entry)
            notes.append("window was brought into view for capture and prior focus restored")
        try:
            if not entry.get("geometry"):
                raise ValueError("the compositor reports no geometry for this window")
            return grim_region(entry["geometry"], entry.get("scale") or 1.0), "grim -g compositor geometry (region of the composited desktop)", notes
        finally:
            if prior is not None and prior["id"] != entry["id"]:
                compositor_focus(prior)
    raise ValueError(f"isolated window capture is unavailable with the {backend} window backend")


def fit_image(data):
    """Downscale a capture that exceeds the 4 MiB attachment bound; returns (png, scale)."""
    if len(data) <= MAX_IMAGE_BYTES:
        return data, 1.0
    try:
        from PIL import Image
    except ImportError:
        raise ValueError("window screenshot exceeds 4 MiB and python-pillow is not installed to downscale it")
    import io
    image = Image.open(io.BytesIO(data))
    scale = 1.0
    for _ in range(6):
        scale *= 0.85 * math.sqrt(MAX_IMAGE_BYTES / len(data))
        buffer = io.BytesIO()
        image.resize((max(1, round(image.width * scale)), max(1, round(image.height * scale)))).save(buffer, "PNG", optimize=True)
        if buffer.tell() <= MAX_IMAGE_BYTES:
            return buffer.getvalue(), scale
        data = buffer.getvalue()
    raise ValueError("window screenshot exceeds 4 MiB even after downscaling")


def window_screenshot(wid):
    entry = compositor_for(wid)
    if entry is None:
        backend = compositor_backend()
        raise ValueError("window capture needs a compositor window backend (niri, sway, Hyprland or X11 EWMH); " +
                         (f"{backend} does not list this window" if backend else "none was detected in this session"))
    data, method, notes = capture_compositor_window(entry)
    raw_size = png_size(data)
    data, scale = fit_image(data)
    width, height = png_size(data)
    WINDOW_CAPTURES[wid] = {"scale": scale, "size": raw_size}
    return {"scope": "window", "window_id": wid, "width": width, "height": height, "scale": round(scale, 6),
            "capture_backend": method, "notes": [n for n in notes if n],
            "compositor": public_window(entry),
            "coordinate_space": "window screenshot pixels; pass coordinate_space=window to pointer ops to target them",
            "borg_attachments": attachment(data)}


def snapshot(args):
    wid = args["window_id"]
    win = window(wid)
    limit = args.get("max_nodes", 300)
    if not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("max_nodes must be between 1 and 1000")
    nodes, truncated = ({}, False) if is_compositor(win) else tree(win, limit)
    token = uuid.uuid4().hex
    previous = observations.get(wid)
    requested = args.get("since")
    if requested and (not previous or previous["observation_id"] != requested):
        raise ValueError("unknown diff baseline; observe without since")
    result = {"window_id": wid, "observation_id": token, "truncated": truncated,
              "coordinate_space": ("window-relative AT-SPI coordinates; add the window bounds origin for "
                                   "private display pixels") if is_private(wid)
              else "AT-SPI window-relative logical coordinates (Wayland)" if session_type() == "wayland"
              else "AT-SPI screen logical coordinates"}
    if requested:
        assert previous is not None
        old = previous["nodes"]
        result.update({"changed": [n for k, n in nodes.items() if old.get(k) != n],
                       "removed": [k for k in old if k not in nodes]})
    else:
        result["nodes"] = list(nodes.values())
    if is_compositor(win):
        result.update({"accessible": False, "compositor": public_window(win),
                       "note": "This window exposes no accessibility tree; use screenshot scope=window and coordinate input."})
    observations[wid] = {"observation_id": token, "nodes": nodes}
    if args.get("screenshot"):
        if is_private(wid):
            result.update(private_screenshot({"scope": args.get("screenshot_scope") or "window", "window_id": wid}))
        else:
            result.update(screenshot(args.get("screenshot_scope"), wid))
    return result


def target(args):
    wid = args["window_id"]
    win = window(wid)
    if is_compositor(win):
        raise ValueError("this window exposes no accessibility tree, so it has no elements; use coordinate input (x, y with coordinate_space=window) from a window screenshot")
    observed = observations.get(wid)
    if not observed or observed["observation_id"] != args.get("observation_id"):
        raise ValueError("stale observation_id; observe the window again before acting")
    key = args["element_id"]
    if key not in observed["nodes"]:
        raise ValueError("element_id was not present in this observation")
    obj = objects[key]
    obj.clear_cache()
    cursor = obj
    for _ in range(64):
        if cursor == win:
            break
        cursor = cursor.get_parent()
        if cursor is None:
            raise ValueError("element no longer belongs to this window; observe again")
    else:
        raise ValueError("element ancestry is too deep")
    if not alive(obj) or describe(obj, observed["nodes"][key]["parent"]) != observed["nodes"][key]:
        raise ValueError("element changed since observation; observe again")
    if not enabled(states(obj)):
        raise ValueError("element is disabled")
    return win, obj


def mutate(args):
    win, obj = target(args)
    op = args["op"]
    # Consume the observation BEFORE issuing an effect, including failed effects.
    observations.pop(args["window_id"], None)
    if op == "click":
        action = obj.get_action_iface()
        names = [action.get_action_name(i).lower() for i in range(action.get_n_actions())]
        index = next((i for i, name in enumerate(names) if name in ("click", "press", "activate")), None)
        if index is None:
            raise ValueError("element has no semantic click action; no coordinate fallback performed")
        if not action.do_action(index):
            raise ValueError("AT-SPI action was rejected")
    elif op == "set_value":
        text = args["text"]
        if not isinstance(text, str) or len(text) > 16384:
            raise ValueError("text must be a string of at most 16384 characters")
        if obj.get_role() == Atspi.Role.PASSWORD_TEXT:
            raise ValueError("password entry requires a human")
        if not obj.get_editable_text_iface().set_text_contents(text):
            raise ValueError("AT-SPI rejected text replacement")
    else:
        raise ValueError(f"unsupported operation: {op}")
    return settle_and_snapshot(win, args["window_id"], op, {"restored_focus": restore_focus(args, args["window_id"])})


def settle_and_snapshot(win, wid, op, extra=None):
    if is_compositor(win):
        time.sleep(0.05)
        result = snapshot({"window_id": wid})
        result.update({"action": op, "dispatched": True, "tree_settled": None,
                       "verification": "No accessibility tree: take screenshot scope=window to verify the effect."})
        result.update(extra or {})
        return result
    # Bounded settling: two matching trees; not a claim that application work finished.
    deadline = time.monotonic() + 1.5
    previous = None
    settled = False
    while time.monotonic() < deadline:
        current, _ = tree(win, 300)
        if current == previous:
            settled = True
            break
        previous = current
        time.sleep(0.1)
    result = snapshot({"window_id": wid})
    result.update({"action": op, "dispatched": True, "tree_settled": settled,
                   "verification": "Inspect the returned tree for the requested application effect."})
    result.update(extra or {})
    return result


# ---- Input injection: a Borg-owned evdev uinput device (keys, buttons, wheel,
# absolute pointer) plus wtype (Wayland) or xdotool (X11) for Unicode text.

ABS_MAX = 65535
SCREEN = None  # (width, height) in desktop screenshot pixels; set by screenshot()/screen_size()
INPUT_DEVICE = None
RELATIVE_DEVICE = None
BUTTONS = {"left": "BTN_LEFT", "right": "BTN_RIGHT", "middle": "BTN_MIDDLE"}
MODIFIERS = {"ctrl": "KEY_LEFTCTRL", "control": "KEY_LEFTCTRL", "alt": "KEY_LEFTALT", "option": "KEY_LEFTALT",
             "opt": "KEY_LEFTALT", "shift": "KEY_LEFTSHIFT", "cmd": "KEY_LEFTMETA", "command": "KEY_LEFTMETA",
             "meta": "KEY_LEFTMETA", "super": "KEY_LEFTMETA", "win": "KEY_LEFTMETA"}
KEY_NAMES = {**{c: f"KEY_{c.upper()}" for c in "abcdefghijklmnopqrstuvwxyz0123456789"},
             **{f"f{n}": f"KEY_F{n}" for n in range(1, 13)},
             "-": "KEY_MINUS", "=": "KEY_EQUAL", "[": "KEY_LEFTBRACE", "]": "KEY_RIGHTBRACE", ";": "KEY_SEMICOLON",
             "'": "KEY_APOSTROPHE", "`": "KEY_GRAVE", "\\": "KEY_BACKSLASH", ",": "KEY_COMMA", ".": "KEY_DOT",
             "/": "KEY_SLASH", "return": "KEY_ENTER", "enter": "KEY_ENTER", "tab": "KEY_TAB", "space": "KEY_SPACE",
             "backspace": "KEY_BACKSPACE", "delete": "KEY_BACKSPACE", "forwarddelete": "KEY_DELETE",
             "escape": "KEY_ESC", "esc": "KEY_ESC", "home": "KEY_HOME", "end": "KEY_END", "pageup": "KEY_PAGEUP",
             "pagedown": "KEY_PAGEDOWN", "left": "KEY_LEFT", "right": "KEY_RIGHT", "up": "KEY_UP", "down": "KEY_DOWN",
             "insert": "KEY_INSERT"}


def session_type():
    if os.environ.get("WAYLAND_DISPLAY"):
        return "wayland"
    if os.environ.get("DISPLAY"):
        return "x11"
    return None


def typing_tool():
    return {"wayland": "wtype", "x11": "xdotool"}.get(session_type())


def input_requirements():
    """Unmet prerequisites for injection, as human-readable strings (empty when ready)."""
    missing = []
    try:
        import evdev  # noqa: F401
    except ImportError:
        missing.append("python-evdev is not installed")
    if not os.access("/dev/uinput", os.W_OK):
        missing.append("/dev/uinput is not writable (add the user to the input group or install a udev rule)")
    tool = typing_tool()
    if tool is None:
        missing.append("no Wayland or X11 display session")
    elif not shutil.which(tool):
        missing.append(f"{tool} is not installed (needed for type_text)")
    return missing


def input_device():
    global INPUT_DEVICE
    if INPUT_DEVICE is not None:
        return INPUT_DEVICE
    missing = input_requirements()
    if missing:
        raise ValueError("input injection unavailable: " + "; ".join(missing))
    from evdev import AbsInfo, UInput, ecodes
    keys = sorted({getattr(ecodes, name) for name in [*KEY_NAMES.values(), *MODIFIERS.values(), *BUTTONS.values()]})
    axis = AbsInfo(value=0, min=0, max=ABS_MAX, fuzz=0, flat=0, resolution=0)
    capabilities = {ecodes.EV_KEY: keys, ecodes.EV_REL: [ecodes.REL_WHEEL, ecodes.REL_HWHEEL],
                    ecodes.EV_ABS: [(ecodes.ABS_X, axis), (ecodes.ABS_Y, axis)]}
    try:
        INPUT_DEVICE = UInput(capabilities, name="Borg virtual input", vendor=0x1209, product=0xb0b6, version=1)
    except OSError as error:
        raise ValueError(f"cannot create the uinput device: {error}")
    time.sleep(0.5)  # let the compositor's libinput pick the new device up
    return INPUT_DEVICE


def relative_device():
    """A separate Borg-owned relative mouse (REL_X/REL_Y): libinput classifies it as an
    ordinary mouse, so games with pointer lock receive the motion as relative deltas.
    Kept apart from the absolute pointer so neither device is misclassified."""
    global RELATIVE_DEVICE
    if RELATIVE_DEVICE is not None:
        return RELATIVE_DEVICE
    missing = [m for m in input_requirements() if "type_text" not in m]
    if missing:
        raise ValueError("input injection unavailable: " + "; ".join(missing))
    from evdev import UInput, ecodes
    capabilities: dict = {ecodes.EV_KEY: [ecodes.BTN_LEFT, ecodes.BTN_RIGHT, ecodes.BTN_MIDDLE],
                    ecodes.EV_REL: [ecodes.REL_X, ecodes.REL_Y, ecodes.REL_WHEEL, ecodes.REL_HWHEEL]}
    try:
        RELATIVE_DEVICE = UInput(capabilities, name="Borg virtual mouse", vendor=0x1209, product=0xb0b7, version=1)
    except OSError as error:
        raise ValueError(f"cannot create the uinput mouse device: {error}")
    time.sleep(0.5)  # let the compositor's libinput pick the new device up
    return RELATIVE_DEVICE


def emit(kind, code, value):
    from evdev import ecodes
    device = input_device()
    device.write(getattr(ecodes, kind), getattr(ecodes, code) if isinstance(code, str) else code, int(value))
    device.syn()


def screen_size():
    global SCREEN
    if SCREEN:
        return SCREEN
    session = session_type()
    if session == "wayland" and shutil.which("grim"):
        probe = subprocess.run(["grim", "-"], capture_output=True, timeout=5)
        if probe.returncode == 0 and probe.stdout.startswith(bytes.fromhex("89504e470d0a1a0a")) and len(probe.stdout) >= 24:
            SCREEN = struct.unpack_from(">II", probe.stdout, 16)
    elif session == "x11" and shutil.which("xdotool"):
        probe = subprocess.run(["xdotool", "getdisplaygeometry"], capture_output=True, timeout=5, text=True)
        parts = probe.stdout.split()
        if probe.returncode == 0 and len(parts) == 2 and all(p.isdigit() for p in parts):
            SCREEN = (int(parts[0]), int(parts[1]))
    if not SCREEN:
        raise ValueError("cannot determine the desktop size for pointer mapping; take a desktop screenshot first")
    return SCREEN


def abs_coordinate(pixel, extent):
    """Map a desktop pixel to the uinput axis so libinput lands on that pixel's centre."""
    return max(0, min(ABS_MAX, round((pixel + 0.5) * (ABS_MAX + 1) / extent)))


def number(value):
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def compositor_focus(entry):
    backend, native = entry["backend"], entry["native_id"]
    if backend == "niri":
        niri_request({"Action": {"FocusWindow": {"id": native}}})
    elif backend == "sway":
        run_text(["swaymsg", f"[con_id={native}]", "focus"])
    elif backend == "hyprland":
        run_text(["hyprctl", "dispatch", "focuswindow", f"address:{native}"])
    elif backend == "x11":
        run_text(["xdotool", "windowactivate", "--sync", str(native)])
    else:
        raise ValueError(f"the {backend} window backend cannot focus windows")


def focused_entry(backend):
    return next((c for c in compositor_list(backend) if c["focused"]), None)


def ensure_active(win, wid=None):
    """Injected events go to the focused window: focus the target (through the
    compositor when it lists the window) and refuse unless it became active.
    Remembers the human's window from before Borg's first focus change, for restore_focus."""
    entry = win if is_compositor(win) else (compositor_for(wid) if wid else None)
    if entry is not None:
        current = focused_entry(entry["backend"])
        if current and current["id"] != FOCUS["borg"]:
            FOCUS["human"] = current  # focus moved since Borg last focused: remember the human's window
        if not (current and current["id"] == entry["id"]):
            compositor_focus(entry)
            FOCUS.update(borg=entry["id"], changed_at=time.monotonic())
            for _ in range(40):
                current = focused_entry(entry["backend"])
                if current and current["id"] == entry["id"]:
                    break
                time.sleep(0.025)
            else:
                raise ValueError("the compositor did not focus the target window; injected input would reach another window")
            time.sleep(0.05)  # let the client see keyboard focus before input arrives
    if is_compositor(win):
        return
    win.clear_cache()
    if states(win).contains(Atspi.StateType.ACTIVE):
        return
    try:
        win.get_component_iface().grab_focus()
    except Exception:
        pass
    for _ in range(10):
        time.sleep(0.05)
        win.clear_cache()
        if states(win).contains(Atspi.StateType.ACTIVE):
            return
    raise ValueError("target window is not active and could not be raised; injected input would reach the focused window, so activate it first")


def restore_focus(args, wid):
    """Give focus back to the window the human had before Borg started moving focus."""
    human = FOCUS["human"]
    if not args.get("restore_focus") or human is None or human["id"] == wid:
        return None
    try:
        compositor_focus(human)
        FOCUS.update(human=None, borg=None)
        return human["id"]
    except Exception as error:
        return f"failed: {error}"


def move_pointer(x, y):
    width, height = screen_size()
    if not (0 <= x < width and 0 <= y < height):
        raise ValueError(f"point ({x:g}, {y:g}) is outside the {width}x{height} desktop")
    emit("EV_ABS", "ABS_X", abs_coordinate(x, width))
    emit("EV_ABS", "ABS_Y", abs_coordinate(y, height))
    time.sleep(0.05)


def button_code(name):
    code = BUTTONS.get(name if name is not None else "left")
    if code is None:
        raise ValueError("button must be left, right or middle")
    return code


def parse_keys(spec):
    """'ctrl+shift+t' -> ([modifier codes], key code); one non-modifier key per call.
    A bare modifier ('shift') is itself the key, so it can be tapped or held."""
    if not isinstance(spec, str) or not spec.strip():
        raise ValueError("keys is required")
    modifiers, key = [], None
    for part in [p.strip() for p in spec.lower().split("+")]:
        if part in MODIFIERS:
            if MODIFIERS[part] not in modifiers:
                modifiers.append(MODIFIERS[part])
        elif part in KEY_NAMES and key is None:
            key = KEY_NAMES[part]
        else:
            raise ValueError(f'unsupported key "{part}"; use one non-modifier key per call')
    if key is None:
        if not modifiers:
            raise ValueError("keys must name a key")
        key = modifiers.pop()
    return modifiers, key


def type_text(text):
    tool = typing_tool()
    if tool == "wtype":
        command = ["wtype", "-d", "5", "--", text]
    else:
        command = ["xdotool", "type", "--delay", "5", "--file", "/dev/stdin"]
    if not shutil.which(command[0]):
        raise ValueError("input injection unavailable: " + "; ".join(input_requirements() or [f"{command[0]} is not installed"]))
    run = subprocess.run(command, input=None if tool == "wtype" else text.encode(), capture_output=True, timeout=60)
    if run.returncode:
        raise ValueError(f"{tool} failed: " + run.stderr.decode(errors="replace")[:1024])


def coordinate_space(args):
    space = args.get("coordinate_space", "desktop")
    if space not in ("desktop", "window"):
        raise ValueError('coordinate_space must be "desktop" or "window"')
    return space


def pointer_target(args):
    """What to point at: an observed element (validated like click) or an x,y in desktop or window space.
    Resolved to desktop pixels by resolve_point only after the window is focused."""
    wid = args["window_id"]
    if args.get("element_id") is not None:
        win, obj = target(args)
        observations.pop(wid, None)
        return win, ("element", obj)
    win = window(wid)
    x, y = number(args.get("x")), number(args.get("y"))
    if x is None or y is None:
        raise ValueError("pointer ops need element_id + observation_id or x + y")
    observations.pop(wid, None)
    return win, (coordinate_space(args), x, y)


def decode_rgb(data):
    import io
    import numpy
    from PIL import Image
    image = Image.open(io.BytesIO(data))
    alpha = numpy.asarray(image.getchannel("A")) if image.mode in ("RGBA", "LA") else None
    return numpy.asarray(image.convert("RGB")), alpha


def locate_on_desktop(entry, window_image=None):
    """Locate the window's pixels on a fresh desktop frame. Returns (match or None,
    (rgb, alpha) of a new window capture when one was taken)."""
    import io
    import numpy
    from PIL import Image
    global SCREEN
    desktop = subprocess.Popen(["grim", "-t", "ppm", "-"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    fresh = None
    try:
        if window_image is None:
            image, _, _ = niri_capture(entry)
            fresh = (decode_rgb(image), image)
            window_image = fresh[0]
    finally:
        shot, _ = desktop.communicate(timeout=5)
    desk = Image.open(io.BytesIO(shot))
    SCREEN = desk.size
    return locate_window(window_image[0], numpy.asarray(desk.convert("RGB")), window_image[1]), fresh


def confirm_origin(wid, entry, mapping):
    """Right before pressing a button, check the window has not moved since it was located."""
    located = LOCATED.get(wid)
    if not mapping or "window_origin" not in mapping or not located or entry is None or entry["backend"] != "niri":
        return
    if located.get("geometry"):
        geometry = refreshed(entry).get("geometry")
        if not geometry or (geometry["x"], geometry["y"]) != located["origin"]:
            raise ValueError("the window moved while it was being targeted; nothing was clicked, retry once it settles")
        return
    here, _ = locate_on_desktop(entry, located["image"])
    if not here or (here["x"], here["y"]) != located["origin"]:
        raise ValueError("the window moved while it was being targeted; nothing was clicked, retry once it settles")


def geometry_matches_capture(wid, entry):
    """Whether window screenshot pixels start at the compositor geometry (no
    shadows or popups widening the capture), so geometry maps them exactly."""
    if not entry.get("geometry"):
        return False
    captured = WINDOW_CAPTURES.get(wid, {}).get("size")
    size = entry.get("size") or {}
    return captured is None or tuple(captured) == (size.get("width"), size.get("height"))


def settle_geometry(entry):
    """After Borg focused a window the workspace may still be sliding into view:
    wait until two region grabs match (grim only, no side effects), up to 1.5 s."""
    previous = None
    while time.monotonic() - FOCUS["changed_at"] < 1.5:
        entry = refreshed(entry)
        if not entry.get("geometry"):
            break
        frame = grim_region(entry["geometry"], entry.get("scale") or 1.0)
        if frame == previous and time.monotonic() - FOCUS["changed_at"] >= 0.3:
            break
        previous = frame
        time.sleep(0.08)
    entry = refreshed(entry)
    if not entry.get("geometry"):
        raise ValueError("the window is no longer floating on a visible workspace")
    return entry


def desktop_origin(wid, entry, purpose="window"):
    """Desktop pixel of the top-left corner of this window's screenshot image.

    Compositors with absolute geometry (sway, Hyprland, X11, niri floating)
    answer directly. niri does not expose the scroll position of tiled
    windows, so the helper captures the window and the desktop together and
    finds the window's pixels on the desktop; ambiguous or unmatched content
    is refused rather than guessed.
    """
    entry = refreshed(entry)
    if entry["backend"] != "niri":
        if entry.get("geometry") and entry.get("visible") is not False:
            return (entry["geometry"]["x"], entry["geometry"]["y"]), {"method": "compositor geometry"}
        raise ValueError("the compositor reports no on-screen geometry for this window")
    if (entry.get("geometry") and purpose == "element") or geometry_matches_capture(wid, entry):
        # Floating window: niri IPC gives exact placement. Wait out a workspace
        # switch without extra captures, then use it.
        entry = settle_geometry(entry)
        origin = (entry["geometry"]["x"], entry["geometry"]["y"])
        LOCATED[wid] = {"origin": origin, "geometry": True}
        return origin, {"method": "niri floating-window geometry (IPC)"}
    try:
        import numpy  # noqa: F401
        from PIL import Image  # noqa: F401
    except ImportError:
        if entry.get("geometry"):
            return (entry["geometry"]["x"], entry["geometry"]["y"]), {"method": "niri floating geometry"}
        raise ValueError("locating a tiled niri window needs python-numpy and python-pillow")
    if entry.get("visible") is False:
        raise ValueError("window is not on a visible workspace")
    window_image, image, seen, since, frames = None, None, None, 0.0, 0
    for attempt in range(14):
        refresh = window_image is None or attempt % 5 == 4  # refresh the window pixels now and then
        here, image_now = locate_on_desktop(entry, None if refresh else window_image)
        if image_now is not None:
            window_image, image = image_now
        # A focus change starts workspace/column animations: only trust a
        # position that holds over several frames spanning 300 ms.
        position = (here["x"], here["y"]) if here else None
        if here is None or position != seen:
            seen, since, frames = position, time.monotonic(), 1
        else:
            frames += 1
            if frames >= 3 and time.monotonic() - since >= 0.3:
                origin = (here["x"], here["y"])
                LOCATED[wid] = {"image": window_image, "origin": origin}
                here.update({"method": "matched the window capture on the desktop (stable for 300 ms)", "attempts": attempt + 1})
                return origin, here
        time.sleep(0.1)
    if entry.get("geometry") and image is not None and (entry.get("size") or {}).get("width") == png_size(image)[0]:
        return (entry["geometry"]["x"], entry["geometry"]["y"]), {"method": "niri floating geometry"}
    raise ValueError("could not locate the window on the desktop (it may be covered, off-screen or showing no distinctive content); use desktop coordinates from a desktop screenshot instead")


def resolve_point(win, wid, spec):
    """Desktop pixel for a pointer target, plus how it was mapped."""
    if spec[0] == "desktop":
        return (spec[1], spec[2]), {"coordinate_space": "desktop"}
    if spec[0] == "window":
        entry = win if is_compositor(win) else compositor_for(wid)
        if entry is None:
            raise ValueError("coordinate_space=window needs a compositor window backend that lists this window")
        origin, how = desktop_origin(wid, entry)
        scale = WINDOW_CAPTURES.get(wid, {}).get("scale", 1.0)
        return window_point(spec[1], spec[2], origin, scale), {"coordinate_space": "window", "window_origin": list(origin),
                                                             "image_scale": scale, "mapping": how}
    obj = spec[1]
    obj.clear_cache()
    extents = obj.get_component_iface().get_extents(extents_type())
    if extents.width <= 0 or extents.height <= 0 or not states(obj).contains(Atspi.StateType.SHOWING):
        raise ValueError("element has no on-screen bounds")
    element = {"x": extents.x, "y": extents.y, "width": extents.width, "height": extents.height}
    if session_type() != "wayland":
        return (element["x"] + element["width"] / 2, element["y"] + element["height"] / 2), {"coordinate_space": "element"}
    entry = compositor_for(wid)
    if entry is None:
        raise ValueError("element-targeted pointer ops on Wayland need the compositor to list this window (AT-SPI extents are window-relative); use click/set_value or coordinate input")
    frame = win.get_component_iface().get_extents(Atspi.CoordType.WINDOW)
    origin, how = desktop_origin(wid, entry, "element")
    point = element_point(element, {"x": frame.x, "y": frame.y}, origin, entry.get("scale") or 1.0)
    return point, {"coordinate_space": "element", "window_origin": list(origin), "mapping": how}


def click_button(code, count):
    for _ in range(count):
        emit("EV_KEY", code, 1)
        time.sleep(0.03)
        emit("EV_KEY", code, 0)
        time.sleep(0.05)


def notches(pixels):
    """Wheel notches for a pixel distance: at least one for any non-zero request (~120 px per notch)."""
    return 0 if pixels == 0 else int(math.copysign(max(1, round(abs(pixels) / 120)), pixels))


def bounded_int(args, name, default, low, high):
    value = args.get(name, default)
    if not isinstance(value, int) or isinstance(value, bool) or not low <= value <= high:
        raise ValueError(f"{name} must be an integer between {low} and {high}")
    return value


def press(modifiers, key):
    for code in modifiers:
        emit("EV_KEY", code, 1)
    emit("EV_KEY", key, 1)


def release(modifiers, key):
    emit("EV_KEY", key, 0)
    for code in reversed(modifiers):
        emit("EV_KEY", code, 0)


def inject(args):
    op = args["op"]
    wid = args.get("window_id")
    if not isinstance(wid, str):
        raise ValueError("window_id is required")
    if is_private(wid):
        return private_inject(args)
    if op == "type_text":
        text = args.get("text")
        if not isinstance(text, str) or len(text) > 16384:
            raise ValueError("text must be a string of at most 16384 characters")
        win = window(wid)
        input_device()  # surface missing prerequisites before touching focus
        observations.pop(wid, None)
        ensure_active(win, wid)
        type_text(text)
        return settle_and_snapshot(win, wid, op, {"restored_focus": restore_focus(args, wid)})
    if op == "key":
        modifiers, key = parse_keys(args.get("keys"))
        hold = bounded_int(args, "hold_ms", 0, 0, 10000)
        win = window(wid)
        input_device()
        observations.pop(wid, None)
        ensure_active(win, wid)
        press(modifiers, key)
        try:
            time.sleep(max(0.02, hold / 1000))
        finally:
            release(modifiers, key)
        return settle_and_snapshot(win, wid, op, {"keys": args["keys"], "held_ms": max(20, hold),
                                                  "restored_focus": restore_focus(args, wid)})
    if op == "pointer_move":
        dx, dy = number(args.get("dx", 0)), number(args.get("dy", 0))
        if dx is None or dy is None or abs(dx) > 20000 or abs(dy) > 20000:
            raise ValueError("pointer_move needs dx, dy of at most 20000 counts")
        steps = bounded_int(args, "steps", max(1, min(200, math.ceil(max(abs(dx), abs(dy)) / 10))), 1, 1000)
        duration = bounded_int(args, "duration_ms", min(2000, steps * 8), 0, 10000)
        held = parse_keys(args["hold_keys"]) if args.get("hold_keys") is not None else None
        win = window(wid)
        device = relative_device()
        input_device()
        place = None
        if args.get("x") is not None or args.get("y") is not None:
            x, y = number(args.get("x")), number(args.get("y"))
            if x is None or y is None:
                raise ValueError("pointer_move start point needs both x and y")
            place = (coordinate_space(args), x, y)
        observations.pop(wid, None)
        ensure_active(win, wid)
        placement = None
        if place is None and POINTER["window"] != wid:
            # Wayland sends motion (and grants pointer lock) only to the surface
            # under the pointer, so enter the window once before moving relatively.
            try:
                size = (compositor_for(wid) or {}).get("size") or {}
                capture_scale = WINDOW_CAPTURES.get(wid, {}).get("scale", 1.0)
                place = ("window", size["width"] * capture_scale / 2, size["height"] * capture_scale / 2)
            except (KeyError, TypeError):
                placement = "pointer position unknown: pass x, y to place it inside the window first"
        if place is not None:
            try:
                (px, py), mapping = resolve_point(win, wid, place)
                move_pointer(px, py)
                POINTER["window"] = wid
                placement = {"point": {"x": px, "y": py}, "mapping": mapping}
            except ValueError as error:
                if args.get("x") is not None:
                    raise
                placement = f"pointer not placed ({error}); motion reaches whichever surface is under the pointer"
        if held:
            press(*held)
        try:
            from evdev import ecodes
            pause = duration / 1000 / steps
            for ex, ey in split_motion(dx, dy, steps):
                if ex:
                    device.write(ecodes.EV_REL, ecodes.REL_X, ex)
                if ey:
                    device.write(ecodes.EV_REL, ecodes.REL_Y, ey)
                device.syn()
                time.sleep(pause)
        finally:
            if held:
                release(*held)
        return settle_and_snapshot(win, wid, op, {
            "sent": {"dx": round(dx), "dy": round(dy)}, "steps": steps, "duration_ms": duration,
            "hold_keys": args.get("hold_keys"), "placement": placement,
            "units": "raw relative mouse counts; apps using relative-pointer (games) get them unaccelerated, the visible cursor follows compositor pointer acceleration",
            "restored_focus": restore_focus(args, wid)})
    if op == "pointer_click":
        code = button_code(args.get("button"))
        count = args.get("count", 1)
        if count not in (1, 2) or isinstance(count, bool):
            raise ValueError("count must be 1 or 2")
        win, spec = pointer_target(args)
        input_device()
        ensure_active(win, wid)
        (x, y), mapping = resolve_point(win, wid, spec)
        move_pointer(x, y)
        POINTER["window"] = wid
        confirm_origin(wid, compositor_for(wid), mapping)
        click_button(code, count)
        return settle_and_snapshot(win, wid, op, {"coordinate_click": spec[0] != "element", "point": {"x": x, "y": y},
                                                  "mapping": mapping, "restored_focus": restore_focus(args, wid)})
    if op == "scroll":
        dx, dy = number(args.get("dx", 0)), number(args.get("dy", 0))
        if dx is None or dy is None or abs(dx) > 10000 or abs(dy) > 10000:
            raise ValueError("scroll distance is limited to 10000 pixels")
        win, spec = pointer_target(args)
        input_device()
        ensure_active(win, wid)
        (x, y), mapping = resolve_point(win, wid, spec)
        move_pointer(x, y)
        POINTER["window"] = wid
        confirm_origin(wid, compositor_for(wid), mapping)
        # Positive dy scrolls content down; REL_WHEEL is positive for scrolling up.
        # Positive dx scrolls content right; REL_HWHEEL is positive for scrolling right.
        vertical, horizontal = -notches(dy), notches(dx)
        for _ in range(abs(vertical)):
            emit("EV_REL", "REL_WHEEL", int(math.copysign(1, vertical)))
            time.sleep(0.01)
        for _ in range(abs(horizontal)):
            emit("EV_REL", "REL_HWHEEL", int(math.copysign(1, horizontal)))
            time.sleep(0.01)
        return settle_and_snapshot(win, wid, op, {"coordinate_click": spec[0] != "element", "point": {"x": x, "y": y},
                                                  "mapping": mapping, "units": "wheel notches of about 120 pixels",
                                                  "notches": {"dx": horizontal, "dy": -vertical},
                                                  "restored_focus": restore_focus(args, wid)})
    if op == "drag":
        points = [number(args.get(k)) for k in ("from_x", "from_y", "to_x", "to_y")]
        if any(p is None for p in points):
            raise ValueError("drag needs from_x, from_y, to_x, to_y")
        space = coordinate_space(args)
        code = button_code(args.get("button"))
        win = window(wid)
        input_device()
        observations.pop(wid, None)
        ensure_active(win, wid)
        sx, sy, ex, ey = (p or 0.0 for p in points)  # validated as numbers above
        (fx, fy), mapping = resolve_point(win, wid, (space, sx, sy))
        if space == "desktop":
            tx, ty = ex, ey
        else:  # same window origin and screenshot scale as the start point
            scale = float(mapping["image_scale"])
            tx, ty = fx + (ex - sx) / scale, fy + (ey - sy) / scale
        width, height = screen_size()
        for x, y in ((fx, fy), (tx, ty)):
            if not (0 <= x < width and 0 <= y < height):
                raise ValueError(f"point ({x:g}, {y:g}) is outside the {width}x{height} desktop")
        move_pointer(fx, fy)
        POINTER["window"] = wid
        confirm_origin(wid, compositor_for(wid), mapping)
        emit("EV_KEY", code, 1)
        steps = 12
        for step in range(1, steps + 1):
            t = step / steps
            move_pointer(fx + (tx - fx) * t, fy + (ty - fy) * t)
            time.sleep(0.02)
        emit("EV_KEY", code, 0)
        return settle_and_snapshot(win, wid, op, {"from": {"x": fx, "y": fy}, "to": {"x": tx, "y": ty}, "mapping": mapping,
                                                  "restored_focus": restore_focus(args, wid)})
    raise ValueError(f"unsupported operation: {op}")


# ---- Agent-private display. Apps run on a Borg-owned headless compositor
# whose owner control socket carries every input event and capture, so
# nothing here touches the user's seat, pointer, focus or screen.

PRIVATE_PREFIX = "pd:"
PRIVATE = None  # the running private display backend
PRIVATE_APPS = {}  # pid -> Popen for session-owned apps (killed at teardown)
EVDEV_CODES = {
    "BTN_LEFT": 272, "BTN_RIGHT": 273, "BTN_MIDDLE": 274, "KEY_ESC": 1, "KEY_MINUS": 12, "KEY_EQUAL": 13,
    "KEY_BACKSPACE": 14, "KEY_TAB": 15, "KEY_LEFTBRACE": 26, "KEY_RIGHTBRACE": 27, "KEY_ENTER": 28,
    "KEY_LEFTCTRL": 29, "KEY_SEMICOLON": 39, "KEY_APOSTROPHE": 40, "KEY_GRAVE": 41, "KEY_LEFTSHIFT": 42,
    "KEY_BACKSLASH": 43, "KEY_COMMA": 51, "KEY_DOT": 52, "KEY_SLASH": 53, "KEY_LEFTALT": 56, "KEY_SPACE": 57,
    "KEY_F11": 87, "KEY_F12": 88, "KEY_HOME": 102, "KEY_UP": 103, "KEY_PAGEUP": 104, "KEY_LEFT": 105,
    "KEY_RIGHT": 106, "KEY_END": 107, "KEY_DOWN": 108, "KEY_PAGEDOWN": 109, "KEY_INSERT": 110,
    "KEY_DELETE": 111, "KEY_LEFTMETA": 125,
    **{f"KEY_{n}": 1 + n for n in range(1, 10)}, "KEY_0": 11,
    **{f"KEY_F{n}": 58 + n for n in range(1, 11)},
    **{f"KEY_{c}": code for row, first in (("QWERTYUIOP", 16), ("ASDFGHJKL", 30), ("ZXCVBNM", 44))
       for code, c in enumerate(row, first)},
}
SCRUBBED_DISPLAY_ENV = ("DISPLAY", "WAYLAND_DISPLAY", "WAYLAND_SOCKET", "NIRI_SOCKET", "SWAYSOCK",
                        "HYPRLAND_INSTANCE_SIGNATURE", "XAUTHORITY", "DESKTOP_STARTUP_ID", "XDG_ACTIVATION_TOKEN")
# Headless-capable compositors another backend could drive; detected and reported only.
ALTERNATIVE_BACKENDS = ("sway", "cage", "labwc", "weston")


def is_private(window_id):
    return isinstance(window_id, str) and window_id.startswith(PRIVATE_PREFIX)


def die_with_helper():
    """preexec_fn: deliver SIGTERM to the child when this helper dies, even by SIGKILL."""
    import ctypes
    import signal
    ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG


def display_env():
    return {k: v for k, v in os.environ.items() if k not in SCRUBBED_DISPLAY_ENV}


def terminate_groups(processes, grace=3.0):
    """SIGTERM each child's process group, wait once for all, then SIGKILL stragglers and leftover members."""
    import signal

    def signal_groups(sig):
        for process in processes:
            try:
                os.killpg(process.pid, sig)
            except (ProcessLookupError, PermissionError):
                pass
    signal_groups(signal.SIGTERM)
    deadline = time.monotonic() + grace
    while any(p.poll() is None for p in processes) and time.monotonic() < deadline:
        time.sleep(0.05)
    signal_groups(signal.SIGKILL)
    for process in processes:
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


class BorgDisplay:
    """Private display backend on borg-display. Another headless compositor can
    back the private display by providing the same methods: running, info,
    windows, focus, pointer_move, pointer_relative, button, axis, key,
    type_text, screenshot and stop, plus wayland_display and directory."""

    name = "borg-display"

    @staticmethod
    def binary():
        configured = os.environ.get("BORG_DISPLAY_BIN")
        if configured and os.access(configured, os.X_OK):
            return configured
        return shutil.which("borg-display")

    @staticmethod
    def runtime_dir():
        runtime = os.environ.get("XDG_RUNTIME_DIR")
        if not runtime or not os.path.isdir(runtime):
            raise ValueError("XDG_RUNTIME_DIR is required for the private display sockets")
        return runtime

    def __init__(self, width, height, render_node=None):
        import select
        binary, runtime = self.binary(), self.runtime_dir()
        if not binary:
            raise ValueError("borg-display is not installed; build it with `cargo install --path crates/borg-display` "
                             "or set BORG_DISPLAY_BIN")
        self.sweep(runtime)
        # display_id names both sockets so another session can attach_display to it.
        self.display_id, self.owned = uuid.uuid4().hex[:16], True
        self.directory = os.path.join(runtime, f"borg-display-{self.display_id}")
        os.mkdir(self.directory, 0o700)
        control = os.path.join(self.directory, "control")
        command = [binary, "--socket", f"borg-private-{self.display_id}", "--control", control,
                   "--size", f"{width}x{height}"]
        if render_node:
            command += ["--render-node", str(render_node)]
        log_path = os.path.join(self.directory, "display.log")
        with open(log_path, "wb") as log:
            self.process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log,
                                            env=display_env(), preexec_fn=die_with_helper)
        ready = None
        if select.select([self.process.stdout], [], [], 15)[0]:
            try:
                ready = json.loads(self.process.stdout.readline() or "null")
            except ValueError:
                pass
        if not isinstance(ready, dict) or not ready.get("ready"):
            self.process.kill()
            self.process.wait()
            with open(log_path, errors="replace") as failure:
                detail = failure.read()[-1024:].strip()
            shutil.rmtree(self.directory, ignore_errors=True)
            raise ValueError("borg-display failed to start: " + (detail or "no readiness report"))
        self.wayland_display = ready["wayland_display"]
        self.connect(control)

    @classmethod
    def attach(cls, display_id):
        """Share a display another session started, as a non-owner: it keeps
        running when this session detaches, and stops when its owner does."""
        if not isinstance(display_id, str) or len(display_id) != 16 or \
                any(c not in "0123456789abcdef" for c in display_id):
            raise ValueError("display_id must be the 16-hex-digit display_id reported by the display's owner")
        backend = cls.__new__(cls)
        backend.display_id, backend.owned, backend.process = display_id, False, None
        backend.directory = os.path.join(cls.runtime_dir(), f"borg-display-{display_id}")
        backend.wayland_display = f"borg-private-{display_id}"
        control = os.path.join(backend.directory, "control")
        if not os.path.exists(control):
            raise ValueError("no running private display has that display_id")
        backend.connect(control)
        return backend

    def connect(self, control):
        import socket
        self.closed = False
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(20)
        self.socket.connect(control)
        self.reader = self.socket.makefile("rb")

    @staticmethod
    def sweep(runtime):
        """Remove directories of displays whose helper was killed: the compositor
        deletes its control socket on exit, so only logs remain."""
        import glob
        for directory in glob.glob(os.path.join(runtime, "borg-display-*")):
            try:
                if not os.path.exists(os.path.join(directory, "control")) and \
                        time.time() - os.stat(directory).st_mtime > 60:
                    shutil.rmtree(directory, ignore_errors=True)
            except OSError:
                pass

    def running(self):
        return not self.closed and (self.process is None or self.process.poll() is None)

    def call(self, request):
        if not self.running():
            raise ValueError("the private display exited; call start_display again")
        try:
            self.socket.sendall(json.dumps(request).encode() + b"\n")
            line = self.reader.readline(8 * 1024 * 1024)
        except OSError as error:
            self.closed = True
            raise ValueError(f"private display control failed: {error}")
        if not line:
            self.closed = True
            raise ValueError("the private display exited; call start_display again")
        response = json.loads(line)
        if not response.get("ok"):
            raise ValueError(response.get("error") or "private display request failed")
        return response["result"]

    def info(self):
        return self.call({"op": "info"})

    def windows(self):
        return self.call({"op": "windows"})["windows"]

    def focus(self, window):
        self.call({"op": "focus", "window": window})

    def pointer_move(self, x, y):
        self.call({"op": "pointer_move", "x": x, "y": y})

    def pointer_relative(self, dx, dy):
        self.call({"op": "pointer_move", "dx": dx, "dy": dy})

    def button(self, code, pressed):
        self.call({"op": "button", "code": code, "pressed": pressed})

    def axis(self, dx, dy):
        self.call({"op": "axis", "dx": dx, "dy": dy})

    def key(self, code, pressed):
        self.call({"op": "key", "code": code, "pressed": pressed})

    def type_text(self, text):
        self.call({"op": "type", "text": text})

    def screenshot(self, path, window=None):
        return self.call({"op": "screenshot", "path": path, **({"window": window} if window is not None else {})})

    def stop(self):
        for closeable in (self.reader, self.socket):
            try:
                closeable.close()
            except OSError:
                pass
        self.closed = True
        if not self.owned:
            return
        try:
            self.process.wait(timeout=5)  # closing the owner socket makes it exit
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        shutil.rmtree(self.directory, ignore_errors=True)


def private_running():
    return PRIVATE is not None and PRIVATE.running()


def require_private():
    if not private_running():
        raise ValueError("the private display is not running; call start_display or launch")
    return PRIVATE


def display_status(info=None):
    if not private_running():
        return {"display": "private", "running": False}
    info = info or PRIVATE.info()
    x11 = getattr(PRIVATE, "x11", None)
    return {"display": "private", "running": True, "backend": PRIVATE.name,
            "display_id": PRIVATE.display_id, "owner": PRIVATE.owned, "wayland_display": PRIVATE.wayland_display,
            "x11_display": x11[1] if x11 and x11[0].poll() is None else None,
            "width": info["width"], "height": info["height"], "gpu_accelerated": info["hardware"],
            "gl_renderer": info["gl_renderer"], "render_node": info["render_node"], "dmabuf": info["dmabuf"],
            "session_apps": sorted(pid for pid, app in PRIVATE_APPS.items() if app.poll() is None),
            "coordinate_space": "private display pixels, top-left origin, scale 1"}


def display_size(args):
    size = []
    for key, default in (("width", 1920), ("height", 1080)):
        value = args.get(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or not 64 <= value <= 8192:
            raise ValueError(f"{key} must be an integer between 64 and 8192")
        size.append(value)
    return tuple(size)


def start_display(args):
    global PRIVATE
    width, height = display_size(args)
    if private_running():
        info = PRIVATE.info()
        if ("width" in args or "height" in args) and (info["width"], info["height"]) != (width, height):
            raise ValueError(f"the private display is already running at {info['width']}x{info['height']}; "
                             "stop_display first to change its size")
        return display_status(info)
    if PRIVATE is not None:
        stop_display()  # the compositor died: reap what it left behind
    PRIVATE = BorgDisplay(width, height, args.get("render_node") or os.environ.get("BORG_DISPLAY_RENDER_NODE"))
    PRIVATE.x11 = None
    return display_status()


def attach_display(args):
    global PRIVATE
    if private_running():
        raise ValueError("this session already uses a private display; stop_display before attaching to another")
    if PRIVATE is not None:
        stop_display()
    PRIVATE = BorgDisplay.attach(args.get("display_id"))
    PRIVATE.x11 = None
    return display_status()


def stop_display(_args=None):
    global PRIVATE
    terminated = sorted(PRIVATE_APPS)
    terminate_groups(list(PRIVATE_APPS.values()))
    PRIVATE_APPS.clear()
    if PRIVATE is None:
        return {"display": "private", "running": False, "stopped": False, "terminated_pids": terminated}
    backend, PRIVATE = PRIVATE, None
    if backend.x11 is not None:
        stop_x11(backend.x11)
    backend.stop()
    return {"display": "private", "running": False, "stopped": backend.owned, "detached": not backend.owned,
            "terminated_pids": terminated}


def stop_x11(x11):
    """Stop xwayland-satellite and remove the X socket and lock it leaves behind."""
    terminate_groups([x11[0]], grace=1.0)
    number = x11[1].lstrip(":")
    for path in (f"/tmp/.X11-unix/X{number}", f"/tmp/.X{number}-lock"):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def ensure_x11():
    if PRIVATE.x11 is not None and PRIVATE.x11[0].poll() is None:
        return PRIVATE.x11[1]
    satellite = shutil.which("xwayland-satellite")
    if not satellite:
        raise ValueError("X11 apps on the private display need xwayland-satellite (package xwayland-satellite) "
                         "and Xwayland; launch a Wayland-native app or install it")
    number = next(n for n in range(100, 1000)
                  if not os.path.exists(f"/tmp/.X11-unix/X{n}") and not os.path.exists(f"/tmp/.X{n}-lock"))
    env = display_env()
    env["WAYLAND_DISPLAY"] = PRIVATE.wayland_display
    log_path = os.path.join(PRIVATE.directory, "xwayland.log")
    with open(log_path, "wb") as log:
        process = subprocess.Popen([satellite, f":{number}"], env=env, stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=die_with_helper)
    deadline = time.monotonic() + 5
    while not os.path.exists(f"/tmp/.X11-unix/X{number}"):
        if process.poll() is not None or time.monotonic() > deadline:
            stop_x11((process, f":{number}"))
            raise ValueError("xwayland-satellite did not start; see " + log_path)
        time.sleep(0.05)
    PRIVATE.x11 = (process, f":{number}")
    return PRIVATE.x11[1]


def descends_from(pid, ancestor):
    for _ in range(64):
        if pid == ancestor:
            return True
        try:
            with open(f"/proc/{pid}/stat") as stat:
                pid = int(stat.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            return False
        if pid <= 1:
            return False
    return False


def launch(args):
    argv = args.get("argv")
    if not isinstance(argv, list) or not argv or len(argv) > 256 or not all(isinstance(a, str) for a in argv):
        raise ValueError("argv must be a non-empty list of strings")
    overrides = args.get("env") or {}
    if not isinstance(overrides, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in overrides.items()):
        raise ValueError("env must map strings to strings")
    redirected = sorted(set(overrides) & set(SCRUBBED_DISPLAY_ENV))
    if redirected:
        raise ValueError(f"env may not set {', '.join(redirected)}: launched apps always run on the private display")
    cwd = args.get("cwd")
    if cwd is not None and (not isinstance(cwd, str) or not os.path.isdir(cwd)):
        raise ValueError("cwd must be an existing directory")
    wait = args.get("wait", 5)
    if not isinstance(wait, (int, float)) or isinstance(wait, bool) or not 0 <= wait <= 10:
        raise ValueError("wait must be between 0 and 10 seconds")
    detached = args.get("detached") is True
    start_display(args)
    env = display_env()
    env.update({"WAYLAND_DISPLAY": PRIVATE.wayland_display, "XDG_SESSION_TYPE": "wayland"})
    if args.get("x11") is True:
        env["DISPLAY"] = ensure_x11()
    env.update(overrides)
    log_path = os.path.join(PRIVATE.directory, f"app-{uuid.uuid4().hex[:8]}.log")
    with open(log_path, "wb") as log:
        try:
            app = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True,
                                   preexec_fn=None if detached else die_with_helper)
        except OSError as error:
            raise ValueError(f"cannot launch {argv[0]}: {error}")
    if not detached:
        PRIVATE_APPS[app.pid] = app
    result = {"pid": app.pid, "detached": detached, "log": log_path}
    deadline = time.monotonic() + wait
    while True:
        found = [w for w in private_windows(accessibility=False)
                 if w["pid"] and w["bounds"]["width"] > 0 and descends_from(w["pid"], app.pid)]
        if found or app.poll() is not None or time.monotonic() >= deadline:
            break
        time.sleep(0.1)
    result.update({"windows": found, "display": display_status()})
    if app.poll() is not None:
        PRIVATE_APPS.pop(app.pid, None)
        with open(log_path, errors="replace") as output:
            result.update({"exited": app.returncode, "output_tail": output.read()[-2048:]})
    return result


def private_accessible(info):
    """The AT-SPI window of a private-display app (same accessibility bus, matched by pid and title)."""
    if not info["pid"]:
        return None
    candidates = []
    desktop = Atspi.get_desktop(0)
    for ai in range(min(desktop.get_child_count(), 256)):
        app = desktop.get_child_at_index(ai)
        if app_pid(app) != info["pid"]:
            continue
        for wi in range(min(app.get_child_count(), 256)):
            win = app.get_child_at_index(wi)
            if alive(win):
                candidates.append(win)
    titled = [w for w in candidates if (w.get_name() or "") == info["title"]]
    chosen = titled or candidates
    return chosen[0] if len(chosen) == 1 else None


def x11_pids():
    """Title -> _NET_WM_PID on the private X server; X11 windows otherwise report xwayland-satellite's pid."""
    x11 = PRIVATE.x11
    if not x11 or x11[0].poll() is not None or not shutil.which("xdotool"):
        return {}
    env = {**display_env(), "DISPLAY": x11[1]}
    found = subprocess.run(["xdotool", "search", "--onlyvisible", "--name", ""], env=env, capture_output=True,
                           text=True, timeout=5)
    pids = {}
    for xid in found.stdout.split()[:256]:
        name = subprocess.run(["xdotool", "getwindowname", xid], env=env, capture_output=True, text=True, timeout=5)
        pid = subprocess.run(["xdotool", "getwindowpid", xid], env=env, capture_output=True, text=True, timeout=5)
        title = name.stdout.rstrip("\n")
        if pid.returncode == 0 and pid.stdout.strip().isdigit():
            pids[title] = None if title in pids else int(pid.stdout)
    return pids


def private_windows(accessibility=True):
    if not private_running():
        return []
    listed = []
    satellite = PRIVATE.x11[0].pid if PRIVATE.x11 else None
    windows_ = PRIVATE.windows()
    x11 = x11_pids() if satellite and any(w["pid"] == satellite for w in windows_) else {}
    for w in windows_:
        if satellite and w["pid"] == satellite:
            w = {**w, "pid": x11.get(w["title"]), "x11": True}
        entry = {"id": f"{PRIVATE_PREFIX}{w['id']}", "title": w["title"], "application": w["app_id"],
                 "app_id": w["app_id"], "pid": w["pid"], "bounds": w["bounds"], "active": w["focused"],
                 "focused": w["focused"], "display": "private", "x11": w.get("x11", False),
                 "compositor": {"backend": PRIVATE.name, "id": w["id"], "app_id": w["app_id"], "pid": w["pid"],
                                "workspace": None, "output": "BORG-1", "focused": w["focused"], "visible": True,
                                "geometry": w["bounds"]}}
        if accessibility:
            entry["accessible"] = private_accessible(w) is not None
        listed.append(entry)
    return listed


def private_window(window_id):
    require_private()
    for w in private_windows(accessibility=False):
        if w["id"] == window_id:
            return {"id": w["compositor"]["id"], "title": w["title"], "pid": w["pid"], "bounds": w["bounds"]}
    raise ValueError("stale or unknown private window_id; list_windows with display=private again")


def private_screenshot(args):
    backend = require_private()
    window_id = args.get("window_id") if args.get("scope") == "window" else None
    if window_id is not None and not is_private(window_id):
        raise ValueError("window capture on the private display needs a pd: window_id")
    path = os.path.join(backend.directory, f"shot-{uuid.uuid4().hex[:8]}.png")
    shot = backend.screenshot(path, private_window(window_id)["id"] if window_id else None)
    try:
        with open(path, "rb") as image:
            data = image.read()
    finally:
        os.unlink(path)
    if len(data) > 4 * 1024 * 1024:
        raise ValueError("screenshot exceeds 4 MiB; use a smaller private display or window capture")
    result = {"scope": "window" if window_id else "desktop", "display": "private", "width": shot["width"],
              "height": shot["height"],
              "coordinate_space": ("window pixels; pass coordinate_space=window to pointer ops" if window_id
                                   else "private display pixels; use these x,y for pointer ops on pd: windows"),
              "borg_attachments": [{"media_type": "image/png", "data_base64": base64.b64encode(data).decode()}]}
    if window_id:
        result["window_id"] = window_id
    return result


def private_point(args, info, accessible, keys=("x", "y")):
    """A private display pixel: an observed element's centre or x,y (display or window pixels)."""
    if args.get("element_id") is not None and keys == ("x", "y"):
        if accessible is None:
            raise ValueError("this private window has no accessibility tree; use x,y from a private screenshot")
        _, obj = target(args)
        extents = obj.get_component_iface().get_extents(Atspi.CoordType.WINDOW)
        if extents.width <= 0 or extents.height <= 0 or not states(obj).contains(Atspi.StateType.SHOWING):
            raise ValueError("element has no on-screen bounds")
        return (info["bounds"]["x"] + extents.x + extents.width / 2,
                info["bounds"]["y"] + extents.y + extents.height / 2), False
    x, y = number(args.get(keys[0])), number(args.get(keys[1]))
    if x is None or y is None:
        raise ValueError(f"pointer ops need element_id + observation_id or {keys[0]} + {keys[1]}")
    space = args.get("coordinate_space", "desktop")
    if space not in ("desktop", "window"):
        raise ValueError('coordinate_space must be "desktop" or "window"')
    if space == "window":
        x, y = x + info["bounds"]["x"], y + info["bounds"]["y"]
    return (x, y), True


def private_inject(args):
    op, wid = args["op"], args["window_id"]
    backend = require_private()
    info = private_window(wid)
    accessible = private_accessible(info)
    extra = {"display": "private"}
    observations.pop(wid, None)
    if op == "type_text":
        text = args.get("text")
        if not isinstance(text, str) or len(text) > 16384:
            raise ValueError("text must be a string of at most 16384 characters")
        backend.focus(info["id"])
        backend.type_text(text)
    elif op == "key":
        modifiers, key = parse_keys(args.get("keys"))
        hold = args.get("hold_ms", 0)
        if not isinstance(hold, int) or isinstance(hold, bool) or not 0 <= hold <= 10000:
            raise ValueError("hold_ms must be an integer between 0 and 10000")
        backend.focus(info["id"])
        codes = [EVDEV_CODES[name] for name in modifiers]
        for code in codes:
            backend.key(code, True)
        backend.key(EVDEV_CODES[key], True)
        time.sleep(hold / 1000)
        backend.key(EVDEV_CODES[key], False)
        for code in reversed(codes):
            backend.key(code, False)
        extra["keys"] = args["keys"]
    elif op == "pointer_move":
        dx, dy = number(args.get("dx", 0)), number(args.get("dy", 0))
        steps = args.get("steps", 1)
        duration = args.get("duration_ms", 0)
        if dx is None or dy is None or abs(dx) > 10000 or abs(dy) > 10000:
            raise ValueError("pointer_move dx/dy are limited to 10000 pixels")
        if not isinstance(steps, int) or isinstance(steps, bool) or not 1 <= steps <= 1000:
            raise ValueError("steps must be an integer between 1 and 1000")
        if not isinstance(duration, int) or isinstance(duration, bool) or not 0 <= duration <= 10000:
            raise ValueError("duration_ms must be an integer between 0 and 10000")
        backend.focus(info["id"])
        for _ in range(steps):
            backend.pointer_relative(dx / steps, dy / steps)
            time.sleep(duration / 1000 / steps)
        extra.update({"relative": {"dx": dx, "dy": dy}, "steps": steps})
    elif op in ("pointer_click", "scroll"):
        (x, y), coordinate = private_point(args, info, accessible)
        backend.focus(info["id"])
        backend.pointer_move(x, y)
        extra.update({"coordinate_click": coordinate, "point": {"x": x, "y": y}})
        if op == "pointer_click":
            count = args.get("count", 1)
            if count not in (1, 2) or isinstance(count, bool):
                raise ValueError("count must be 1 or 2")
            code = EVDEV_CODES[button_code(args.get("button"))]
            for _ in range(count):
                backend.button(code, True)
                backend.button(code, False)
                time.sleep(0.03)
        else:
            dx, dy = number(args.get("dx", 0)), number(args.get("dy", 0))
            if dx is None or dy is None or abs(dx) > 10000 or abs(dy) > 10000:
                raise ValueError("scroll distance is limited to 10000 pixels")
            backend.axis(notches(dx), notches(dy))  # positive dy scrolls content down
            extra.update({"units": "wheel notches of about 120 pixels",
                          "notches": {"dx": notches(dx), "dy": notches(dy)}})
    elif op == "drag":
        (fx, fy), _ = private_point(args, info, accessible, ("from_x", "from_y"))
        (tx, ty), _ = private_point(args, info, accessible, ("to_x", "to_y"))
        code = EVDEV_CODES[button_code(args.get("button"))]
        backend.focus(info["id"])
        backend.pointer_move(fx, fy)
        backend.button(code, True)
        for step in range(1, 13):
            backend.pointer_move(fx + (tx - fx) * step / 12, fy + (ty - fy) * step / 12)
            time.sleep(0.02)
        backend.button(code, False)
        extra.update({"from": {"x": fx, "y": fy}, "to": {"x": tx, "y": ty}})
    else:
        raise ValueError(f"unsupported operation: {op}")
    time.sleep(0.1)
    if not any(f"{PRIVATE_PREFIX}{w['id']}" == wid for w in backend.windows()):
        return {"window_id": wid, "action": op, "dispatched": True, "window_closed": True, **extra,
                "verification": "The window closed after the action; list_windows with display=private."}
    if accessible is not None:
        return settle_and_snapshot(accessible, wid, op, extra)
    return {"window_id": wid, "action": op, "dispatched": True, "accessible": False, **extra,
            "verification": "This window exposes no accessibility tree; take a private screenshot to verify."}


def private_capabilities():
    import glob
    binary = BorgDisplay.binary()
    status = {"available": bool(binary) and bool(os.environ.get("XDG_RUNTIME_DIR")),
              "backend": "borg-display (Borg-owned headless Wayland compositor)", "binary": binary,
              "render_nodes": sorted(glob.glob("/dev/dri/renderD*")),
              "x11": "xwayland-satellite" if shutil.which("xwayland-satellite") else None,
              "input_backend": "private seat through the display's control socket (never uinput or the user's focus)",
              "operations": ["start_display", "stop_display", "attach_display", "launch", "list_windows", "observe", "screenshot",
                             "click", "set_value", "type_text", "key", "pointer_click", "pointer_move", "scroll", "drag"],
              "gpu_accelerated": None,
              "gpu_note": "Measured when the display starts: true means hardware EGL on a render node plus dmabuf "
                          "for Vulkan/GL clients; false means Mesa software rendering.",
              "detected_alternative_compositors": [name for name in ALTERNATIVE_BACKENDS if shutil.which(name)],
              "limitations": [
                  "Private apps share the user's session D-Bus and accessibility bus; single-instance apps that are "
                  "already running on the desktop (browsers, some terminals) may open their window there instead, so "
                  "launch a separate instance or profile.",
                  "type_text covers the characters of the default keyboard layout; use set_value for others.",
                  "GTK4 reports element extents as 0,0; prefer semantic click/set_value or x,y from a private screenshot.",
                  "Screenshots contain no cursor. X11 apps need launch x11=true and xwayland-satellite.",
                  "Detached apps are not killed at teardown but lose their display when it stops.",
              ]}
    if not binary:
        status["reason"] = "borg-display is not installed next to borg or on PATH"
    if private_running():
        status.update(display_status())
    return status


def module_available(name):
    import importlib.util
    return importlib.util.find_spec(name) is not None


def window_capabilities():
    """Window backend, whether window capture works, and honest limitations."""
    backend = compositor_backend()
    limitations = []
    capture = False
    if backend == "niri":
        capture = True
        limitations.append("niri: scope=window uses niri's own window screenshot, which renders only that window's surfaces even when it is on another workspace or scrolled off-screen, without changing focus. niri also copies each capture to the clipboard and shows a transient 'Screenshot captured' notification; Borg restores the previous clipboard contents in one format" + ("" if shutil.which("wl-copy") else " once wl-clipboard is installed (it is missing)") + ".")
        locate = module_available("numpy") and module_available("PIL")
        limitations.append("niri: coordinate_space=window and element-targeted pointer ops use niri's IPC geometry for floating windows (no capture). niri exposes no position for tiled windows, so for those the helper falls back to locating the window by matching a fresh window capture on the desktop (another capture + notification per op), requires the position to hold for 300 ms, re-checks it right before pressing, and refuses covered, off-screen, featureless or duplicate-looking windows." if locate else
                           "niri: tiled-window localisation needs python-numpy and python-pillow (missing), so coordinate_space=window works only for floating windows.")
    elif backend in ("sway", "hyprland"):
        capture = bool(shutil.which("grim"))
        limitations.append(f"{backend}: scope=window crops the composited desktop to the compositor's window geometry (grim -g; grim -T when the compositor exposes an ext-foreign-toplevel identifier); overlapping windows appear in the crop and hidden windows are briefly brought into view, then prior focus is restored.")
    elif backend == "x11":
        capture = bool(shutil.which("import"))
        limitations.append("X11: windows come from EWMH _NET_CLIENT_LIST; scope=window uses ImageMagick import -window" + ("" if capture else " (not installed: install imagemagick)") + ", where overlapping windows can show through without a compositor.")
    elif backend == "foreign-toplevel":
        capture = bool(shutil.which("grim"))
        limitations.append("wlr/ext foreign-toplevel via lswt: windows are listed without geometry or pid; scope=window needs grim -T support in the compositor.")
    else:
        limitations.append("No compositor window backend detected (niri, sway, Hyprland, lswt or X11 EWMH): list_windows shows only AT-SPI windows and scope=window is unavailable.")
    return backend, capture, limitations


def dispatch(args):
    op = args["op"]
    display = args.get("display", "desktop")
    if display not in ("desktop", "private"):
        raise ValueError('display must be "desktop" or "private"')
    if op == "start_display":
        return start_display(args)
    if op == "stop_display":
        return stop_display()
    if op == "attach_display":
        return attach_display(args)
    if op == "launch":
        return launch(args)
    if display == "private" and op == "list_windows":
        return {"windows": private_windows(), "display": display_status()}
    if op == "screenshot" and (display == "private" or is_private(args.get("window_id"))):
        return private_screenshot(args)
    if op == "pointer_move" and is_private(args.get("window_id")):
        return private_inject(args)
    if op == "capabilities":
        missing = input_requirements()
        backend, window_capture, limitations = window_capabilities()
        operations = ["capabilities", "list_windows", "observe", "screenshot", "click", "set_value"]
        if not (shutil.which("grim") and os.environ.get("WAYLAND_DISPLAY")):
            limitations.append("Desktop screenshots need grim on a Wayland compositor with wlr-screencopy.")
        if missing:
            limitations.append("Input injection unavailable: " + "; ".join(missing) + ".")
        else:
            operations += ["type_text", "key", "pointer_click", "scroll", "drag", "pointer_move"]
            limitations.append("Input injection focuses the target window through the compositor when it lists the window (pass restore_focus=true to hand focus back afterwards) and refuses if it did not become focused; events reach the focused window.")
            limitations.append("pointer_move emits raw relative motion (REL_X/REL_Y) from a separate Borg virtual mouse; games with pointer lock receive unaccelerated deltas, the visible cursor follows compositor acceleration. key hold_ms (up to 10 s) and pointer_move hold_keys hold keys for games.")
            if session_type() == "wayland":
                limitations.append("On Wayland, element-targeted pointer_click/scroll map window-relative AT-SPI extents through the compositor's window position; they are refused when the compositor does not list the window.")
        private = private_capabilities()
        if private["available"]:
            operations += ["start_display", "stop_display", "attach_display", "launch"]
            operations += [o for o in ("type_text", "key", "pointer_click", "scroll", "drag") if o not in operations]
            limitations.append("Prefer the private display (launch, then display=private / pd: window ids) for app testing: "
                               "its input and capture never touch the user's desktop. Desktop input goes to the user's focused window.")
        return {"platform": "linux", "backend": "AT-SPI2", "desktop_available": Atspi.get_desktop_count() > 0,
                "session_type": session_type(), "operations": operations,
                "window_backend": backend,
                "capture_scopes": (["desktop"] if shutil.which("grim") and os.environ.get("WAYLAND_DISPLAY") else []) + (["window"] if window_capture else []),
                "input_backend": None if missing else f"evdev uinput + {typing_tool()}",
                "input_coordinate_space": "desktop screenshot pixels (top-left origin); coordinate_space=window uses window screenshot pixels",
                "limitations": limitations, "private_display": private}
    if op == "list_windows":
        listed = windows()
        result = {"windows": listed, "window_backend": compositor_backend()}
        if COMPOSITOR_STATE["error"]:
            result["window_backend_error"] = COMPOSITOR_STATE["error"]
        return result
    if op == "screenshot":
        return screenshot(args.get("scope"), args.get("window_id"))
    if op == "observe":
        return snapshot(args)
    if op in ("click", "set_value"):
        return mutate(args)
    if op in ("type_text", "key", "pointer_click", "scroll", "drag", "pointer_move"):
        return inject(args)
    raise ValueError(f"unsupported operation: {op}")


if __name__ == "__main__":
    import atexit
    import signal
    atexit.register(stop_display)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    for line in sys.stdin:
        try:
            request = json.loads(line)
            result = {"ok": True, "result": dispatch(request)}
        except Exception as error:
            result = {"ok": False, "error": str(error)}
        print(json.dumps(result, ensure_ascii=False), flush=True)
