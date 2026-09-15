"""Borg-owned AT-SPI worker. JSONL on stdin/stdout; diagnostics on stderr."""
import base64
import json
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
    result = snapshot({"window_id": args["window_id"]})
    result.update({"action": op, "dispatched": True, "tree_settled": settled,
                   "verification": "Inspect the returned tree for the requested application effect."})
    return result


def dispatch(args):
    op = args["op"]
    if op == "capabilities":
        return {"platform": "linux", "backend": "AT-SPI2", "desktop_available": Atspi.get_desktop_count() > 0,
                "operations": ["capabilities", "list_windows", "observe", "screenshot", "click", "set_value"],
                "capture_scopes": ["desktop"] if shutil.which("grim") and os.environ.get("WAYLAND_DISPLAY") else [],
                "limitations": ["No keyboard, pointer injection, drag, or scroll backend yet.",
                                "Desktop screenshots require explicit scope=desktop and grim on a supported Wayland compositor; no isolated window capture."]}
    if op == "list_windows":
        return {"windows": windows()}
    if op == "screenshot":
        return screenshot(args.get("scope"))
    if op == "observe":
        return snapshot(args)
    if op in ("click", "set_value"):
        return mutate(args)
    raise ValueError(f"unsupported operation: {op}")


for line in sys.stdin:
    try:
        request = json.loads(line)
        result = {"ok": True, "result": dispatch(request)}
    except Exception as error:
        result = {"ok": False, "error": str(error)}
    print(json.dumps(result, ensure_ascii=False), flush=True)
