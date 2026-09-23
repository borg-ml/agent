"""Borg-owned AT-SPI worker. JSONL on stdin/stdout; diagnostics on stderr."""
import base64
import json
import math
import os
import shutil
import struct
import subprocess
import sys
import time
import uuid

import gi

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # pyright: ignore[reportAttributeAccessIssue]

Atspi.set_timeout(1500, 3000)
EPOCH = uuid.uuid4().hex[:12]
objects = {}
object_ids = {}
next_id = 0
observations = {}


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
    result = []
    private_pids = {w["pid"] for w in private_windows(accessibility=False)}
    desktop = Atspi.get_desktop(0)
    for ai in range(min(desktop.get_child_count(), 256)):
        app = desktop.get_child_at_index(ai)
        if private_pids and app_pid(app) in private_pids:
            continue  # listed under display=private
        for wi in range(min(app.get_child_count(), 256)):
            win = app.get_child_at_index(wi)
            if alive(win):
                result.append({"id": identify(win), "title": win.get_name(),
                               "application": app.get_name(),
                               "active": states(win).contains(Atspi.StateType.ACTIVE)})
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
    return objects[key]


def describe(obj, parent):
    state = states(obj)
    node = {"id": identify(obj), "parent": parent, "role": obj.get_role_name(),
            "name": (obj.get_name() or "")[:1024],
            "enabled": state.contains(Atspi.StateType.ENABLED),
            "focused": state.contains(Atspi.StateType.FOCUSED),
            "showing": state.contains(Atspi.StateType.SHOWING)}
    try:
        r = obj.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
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


def screenshot(scope):
    if scope != "desktop":
        raise ValueError("only explicit desktop capture is available; isolated window capture is unsupported")
    capture = subprocess.run(["grim", "-"], capture_output=True, timeout=5)
    if capture.returncode:
        raise ValueError("desktop capture failed: " + capture.stderr.decode(errors="replace")[:1024])
    data = capture.stdout
    if not data.startswith(bytes.fromhex("89504e470d0a1a0a")) or len(data) < 24:
        raise ValueError("capture did not return a PNG")
    if len(data) > 4 * 1024 * 1024:
        raise ValueError("screenshot exceeds 4 MiB")
    width, height = struct.unpack_from(">II", data, 16)
    global SCREEN
    SCREEN = (width, height)
    return {"scope": "desktop", "width": width, "height": height,
            "coordinate_space": "screenshot pixels, not AT-SPI screen coordinates",
            "borg_attachments": [{"media_type": "image/png", "data_base64": base64.b64encode(data).decode()}]}


def snapshot(args):
    wid = args["window_id"]
    win = window(wid)
    limit = args.get("max_nodes", 300)
    if not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("max_nodes must be between 1 and 1000")
    nodes, truncated = tree(win, limit)
    token = uuid.uuid4().hex
    previous = observations.get(wid)
    requested = args.get("since")
    if requested and (not previous or previous["observation_id"] != requested):
        raise ValueError("unknown diff baseline; observe without since")
    result = {"window_id": wid, "observation_id": token, "truncated": truncated,
              "coordinate_space": ("window-relative AT-SPI coordinates; add the window bounds origin for "
                                   "private display pixels") if is_private(wid) else "AT-SPI screen logical coordinates"}
    if requested:
        assert previous is not None
        old = previous["nodes"]
        result.update({"changed": [n for k, n in nodes.items() if old.get(k) != n],
                       "removed": [k for k in old if k not in nodes]})
    else:
        result["nodes"] = list(nodes.values())
    observations[wid] = {"observation_id": token, "nodes": nodes}
    if args.get("screenshot"):
        if is_private(wid):
            result.update(private_screenshot({"scope": args.get("screenshot_scope") or "window", "window_id": wid}))
        else:
            result.update(screenshot(args.get("screenshot_scope")))
    return result


def target(args):
    wid = args["window_id"]
    win = window(wid)
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
    if not states(obj).contains(Atspi.StateType.ENABLED):
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
    return settle_and_snapshot(win, args["window_id"], op)


def settle_and_snapshot(win, wid, op, extra=None):
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


def ensure_active(win):
    """Injected events go to the focused window, so refuse unless the target is active."""
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
    """'ctrl+shift+t' -> ([modifier codes], key code); one non-modifier key per call."""
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
        raise ValueError("keys must name one non-modifier key")
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


def pointer_target(args):
    """The centre of an observed element (validated like click) or an explicit desktop pixel."""
    wid = args["window_id"]
    if args.get("element_id") is not None:
        if session_type() == "wayland":
            raise ValueError("element-targeted pointer ops are unavailable on Wayland: AT-SPI extents are window-relative and the window origin is unknown; use click/set_value or x,y read from a desktop screenshot")
        win, obj = target(args)
        observations.pop(wid, None)
        extents = obj.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
        if extents.width <= 0 or extents.height <= 0 or not states(obj).contains(Atspi.StateType.SHOWING):
            raise ValueError("element has no on-screen bounds")
        return win, (extents.x + extents.width / 2, extents.y + extents.height / 2), False
    win = window(wid)
    x, y = number(args.get("x")), number(args.get("y"))
    if x is None or y is None:
        raise ValueError("pointer ops need element_id + observation_id or x + y")
    observations.pop(wid, None)
    return win, (x, y), True


def click_button(code, count):
    for _ in range(count):
        emit("EV_KEY", code, 1)
        time.sleep(0.03)
        emit("EV_KEY", code, 0)
        time.sleep(0.05)


def notches(pixels):
    """Wheel notches for a pixel distance: at least one for any non-zero request (~120 px per notch)."""
    return 0 if pixels == 0 else int(math.copysign(max(1, round(abs(pixels) / 120)), pixels))


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
        ensure_active(win)
        type_text(text)
        return settle_and_snapshot(win, wid, op)
    if op == "key":
        modifiers, key = parse_keys(args.get("keys"))
        win = window(wid)
        input_device()
        observations.pop(wid, None)
        ensure_active(win)
        for code in modifiers:
            emit("EV_KEY", code, 1)
        emit("EV_KEY", key, 1)
        time.sleep(0.02)
        emit("EV_KEY", key, 0)
        for code in reversed(modifiers):
            emit("EV_KEY", code, 0)
        return settle_and_snapshot(win, wid, op, {"keys": args["keys"]})
    if op == "pointer_click":
        code = button_code(args.get("button"))
        count = args.get("count", 1)
        if count not in (1, 2) or isinstance(count, bool):
            raise ValueError("count must be 1 or 2")
        win, (x, y), coordinate = pointer_target(args)
        input_device()
        ensure_active(win)
        move_pointer(x, y)
        click_button(code, count)
        return settle_and_snapshot(win, wid, op, {"coordinate_click": coordinate, "point": {"x": x, "y": y}})
    if op == "scroll":
        dx, dy = number(args.get("dx", 0)), number(args.get("dy", 0))
        if dx is None or dy is None or abs(dx) > 10000 or abs(dy) > 10000:
            raise ValueError("scroll distance is limited to 10000 pixels")
        win, (x, y), coordinate = pointer_target(args)
        input_device()
        ensure_active(win)
        move_pointer(x, y)
        # Positive dy scrolls content down; REL_WHEEL is positive for scrolling up.
        # Positive dx scrolls content right; REL_HWHEEL is positive for scrolling right.
        vertical, horizontal = -notches(dy), notches(dx)
        for _ in range(abs(vertical)):
            emit("EV_REL", "REL_WHEEL", int(math.copysign(1, vertical)))
            time.sleep(0.01)
        for _ in range(abs(horizontal)):
            emit("EV_REL", "REL_HWHEEL", int(math.copysign(1, horizontal)))
            time.sleep(0.01)
        return settle_and_snapshot(win, wid, op, {"coordinate_click": coordinate, "point": {"x": x, "y": y},
                                                  "units": "wheel notches of about 120 pixels",
                                                  "notches": {"dx": horizontal, "dy": -vertical}})
    if op == "drag":
        points = [number(args.get(k)) for k in ("from_x", "from_y", "to_x", "to_y")]
        if any(p is None for p in points):
            raise ValueError("drag needs from_x, from_y, to_x, to_y")
        fx, fy, tx, ty = points
        code = button_code(args.get("button"))
        win = window(wid)
        width, height = screen_size()
        for x, y in ((fx, fy), (tx, ty)):
            if not (0 <= x < width and 0 <= y < height):
                raise ValueError(f"point ({x:g}, {y:g}) is outside the {width}x{height} desktop")
        input_device()
        observations.pop(wid, None)
        ensure_active(win)
        move_pointer(fx, fy)
        emit("EV_KEY", code, 1)
        steps = 12
        for step in range(1, steps + 1):
            t = step / steps
            move_pointer(fx + (tx - fx) * t, fy + (ty - fy) * t)
            time.sleep(0.02)
        emit("EV_KEY", code, 0)
        return settle_and_snapshot(win, wid, op, {"from": {"x": fx, "y": fy}, "to": {"x": tx, "y": ty}})
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

    def __init__(self, width, height, render_node=None):
        import select
        import socket
        import tempfile
        binary, runtime = self.binary(), os.environ.get("XDG_RUNTIME_DIR")
        if not binary:
            raise ValueError("borg-display is not installed; build it with `cargo install --path crates/borg-display` "
                             "or set BORG_DISPLAY_BIN")
        if not runtime or not os.path.isdir(runtime):
            raise ValueError("XDG_RUNTIME_DIR is required for the private display sockets")
        self.sweep(runtime)
        self.directory = tempfile.mkdtemp(prefix="borg-display-", dir=runtime)
        control = os.path.join(self.directory, "control")
        command = [binary, "--socket", "borg-private-" + uuid.uuid4().hex[:12], "--control", control,
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
        return self.process.poll() is None

    def call(self, request):
        if not self.running():
            raise ValueError("the private display exited; call start_display again")
        try:
            self.socket.sendall(json.dumps(request).encode() + b"\n")
            line = self.reader.readline(8 * 1024 * 1024)
        except OSError as error:
            raise ValueError(f"private display control failed: {error}")
        if not line:
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
            "wayland_display": PRIVATE.wayland_display,
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
    return {"display": "private", "running": False, "stopped": True, "terminated_pids": terminated}


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
              "operations": ["start_display", "stop_display", "launch", "list_windows", "observe", "screenshot",
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


def dispatch(args):
    op = args["op"]
    display = args.get("display", "desktop")
    if display not in ("desktop", "private"):
        raise ValueError('display must be "desktop" or "private"')
    if op == "start_display":
        return start_display(args)
    if op == "stop_display":
        return stop_display()
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
        operations = ["capabilities", "list_windows", "observe", "screenshot", "click", "set_value"]
        limitations = ["Desktop screenshots require explicit scope=desktop and grim on a supported Wayland compositor; no isolated window capture."]
        if missing:
            limitations.append("Input injection unavailable: " + "; ".join(missing) + ".")
        else:
            operations += ["type_text", "key", "pointer_click", "scroll", "drag"]
            limitations.append("Input injection requires the target window to be active (it is raised when the compositor allows); events reach the focused window.")
            if session_type() == "wayland":
                limitations.append("On Wayland, element-targeted pointer_click/scroll are refused because AT-SPI extents are window-relative; use x,y from a desktop screenshot.")
        private = private_capabilities()
        if private["available"]:
            operations += ["start_display", "stop_display", "launch"]
            operations += [o for o in ("type_text", "key", "pointer_click", "scroll", "drag") if o not in operations]
            limitations.append("Prefer the private display (launch, then display=private / pd: window ids) for app testing: "
                               "its input and capture never touch the user's desktop. Desktop input goes to the user's focused window.")
        return {"platform": "linux", "backend": "AT-SPI2", "desktop_available": Atspi.get_desktop_count() > 0,
                "session_type": session_type(), "operations": operations,
                "capture_scopes": ["desktop"] if shutil.which("grim") and os.environ.get("WAYLAND_DISPLAY") else [],
                "input_backend": None if missing else f"evdev uinput + {typing_tool()}",
                "input_coordinate_space": "desktop screenshot pixels (top-left origin)",
                "limitations": limitations, "private_display": private}
    if op == "list_windows":
        return {"windows": windows()}
    if op == "screenshot":
        return screenshot(args.get("scope"))
    if op == "observe":
        return snapshot(args)
    if op in ("click", "set_value"):
        return mutate(args)
    if op in ("type_text", "key", "pointer_click", "scroll", "drag"):
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
