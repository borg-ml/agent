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
    desktop = Atspi.get_desktop(0)
    for ai in range(min(desktop.get_child_count(), 256)):
        app = desktop.get_child_at_index(ai)
        for wi in range(min(app.get_child_count(), 256)):
            win = app.get_child_at_index(wi)
            if alive(win):
                result.append({"id": identify(win), "title": win.get_name(),
                               "application": app.get_name(),
                               "active": states(win).contains(Atspi.StateType.ACTIVE)})
    return result


def window(key):
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
              "coordinate_space": "AT-SPI screen logical coordinates"}
    if requested:
        assert previous is not None
        old = previous["nodes"]
        result.update({"changed": [n for k, n in nodes.items() if old.get(k) != n],
                       "removed": [k for k in old if k not in nodes]})
    else:
        result["nodes"] = list(nodes.values())
    observations[wid] = {"observation_id": token, "nodes": nodes}
    if args.get("screenshot"):
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


def dispatch(args):
    op = args["op"]
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
        return {"platform": "linux", "backend": "AT-SPI2", "desktop_available": Atspi.get_desktop_count() > 0,
                "session_type": session_type(), "operations": operations,
                "capture_scopes": ["desktop"] if shutil.which("grim") and os.environ.get("WAYLAND_DISPLAY") else [],
                "input_backend": None if missing else f"evdev uinput + {typing_tool()}",
                "input_coordinate_space": "desktop screenshot pixels (top-left origin)",
                "limitations": limitations}
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
    for line in sys.stdin:
        try:
            request = json.loads(line)
            result = {"ok": True, "result": dispatch(request)}
        except Exception as error:
            result = {"ok": False, "error": str(error)}
        print(json.dumps(result, ensure_ascii=False), flush=True)
