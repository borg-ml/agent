"""Compositor window enumeration and window-to-desktop coordinate mapping.

Pure parsing/mapping functions plus thin IPC wrappers; no AT-SPI imports, so the
parsers are unit-testable without a desktop. Prepended to linux.py at runtime.
"""
import collections
import json
import math
import os
import shutil
import socket
import subprocess
import unicodedata

MAX_COMPOSITOR_WINDOWS = 256


def rect(x, y, width, height):
    return {"x": round(x), "y": round(y), "width": round(width), "height": round(height)}


def desktop_scale(scales):
    """grim renders the whole desktop at the greatest output scale."""
    return max([s for s in scales if isinstance(s, (int, float)) and s > 0] or [1.0])


def intersects(geometry, area):
    return (geometry["x"] < area["x"] + area["width"] and area["x"] < geometry["x"] + geometry["width"]
            and geometry["y"] < area["y"] + area["height"] and area["y"] < geometry["y"] + geometry["height"])


# ---- niri: JSON IPC on $NIRI_SOCKET ---------------------------------------

def niri_request(request, timeout=3.0):
    path = os.environ.get("NIRI_SOCKET")
    if not path:
        raise ValueError("NIRI_SOCKET is not set")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(path)
        sock.sendall((json.dumps(request) + "\n").encode())
        sock.shutdown(socket.SHUT_WR)
        data = b""
        while len(data) < 16 * 1024 * 1024:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
    reply = json.loads(data)
    if "Err" in reply:
        raise ValueError(f"niri refused the request: {reply['Err']}")
    return reply["Ok"]


def niri_windows(windows, workspaces, outputs):
    """Normalise niri Windows/Workspaces/Outputs replies.

    niri reports absolute placement only for floating windows
    (tile_pos_in_workspace_view); tiled windows scroll with a view offset that
    IPC does not expose, so their geometry is null and visibility is unknown
    unless their workspace is inactive.
    """
    scale = desktop_scale([(o.get("logical") or {}).get("scale") for o in outputs.values()])
    spaces = {w["id"]: w for w in workspaces}
    result = []
    for w in windows[:MAX_COMPOSITOR_WINDOWS]:
        space = spaces.get(w.get("workspace_id")) or {}
        output = outputs.get(space.get("output")) or {}
        logical = output.get("logical") or {}
        layout = w.get("layout") or {}
        size = layout.get("window_size") or [0, 0]
        active = bool(space.get("is_active"))
        geometry = None
        tile = layout.get("tile_pos_in_workspace_view")
        if tile is not None and logical and active:
            offset = layout.get("window_offset_in_tile") or [0, 0]
            geometry = rect((logical["x"] + tile[0] + offset[0]) * scale, (logical["y"] + tile[1] + offset[1]) * scale,
                            size[0] * scale, size[1] * scale)
        output_area = (rect(logical["x"] * scale, logical["y"] * scale, logical["width"] * scale, logical["height"] * scale)
                       if logical else None)
        if not space:
            visible = False  # not on any workspace (e.g. unmapped)
        elif not active:
            visible = False
        elif geometry is not None and output_area is not None:
            visible = intersects(geometry, output_area)
        else:
            visible = None
        result.append({
            "backend": "niri", "native_id": w["id"], "id": f"niri:{w['id']}",
            "title": w.get("title") or "", "app_id": w.get("app_id"), "pid": w.get("pid"),
            "workspace": space.get("name") or space.get("idx"), "workspace_id": w.get("workspace_id"),
            "output": space.get("output"), "focused": bool(w.get("is_focused")),
            "floating": bool(w.get("is_floating")), "visible": visible, "geometry": geometry,
            "size": {"width": round(size[0] * scale), "height": round(size[1] * scale)},
            "output_geometry": output_area, "scale": logical.get("scale", 1.0),
        })
    return result


def niri_list():
    outputs = niri_request("Outputs")["Outputs"]
    workspaces = niri_request("Workspaces")["Workspaces"]
    windows = niri_request("Windows")["Windows"]
    return niri_windows(windows, workspaces, outputs)


# ---- sway: swaymsg -t get_tree ---------------------------------------------

def sway_windows(tree):
    """Leaf containers of a sway tree. Rects are absolute logical coordinates."""
    scale = desktop_scale([o.get("scale") for o in tree.get("nodes", []) if o.get("type") == "output"])
    result = []

    def walk(node, output, workspace):
        if node.get("type") == "output":
            output = node.get("name")
        elif node.get("type") == "workspace":
            workspace = node.get("name")
        children = node.get("nodes", []) + node.get("floating_nodes", [])
        if not children and node.get("type") in ("con", "floating_con") and (node.get("pid") or node.get("app_id") or node.get("window")):
            outer, inner = node.get("rect") or {}, node.get("window_rect") or {}
            geometry = rect((outer.get("x", 0) + inner.get("x", 0)) * scale, (outer.get("y", 0) + inner.get("y", 0)) * scale,
                            inner.get("width", outer.get("width", 0)) * scale, inner.get("height", outer.get("height", 0)) * scale)
            props = node.get("window_properties") or {}
            result.append({
                "backend": "sway", "native_id": node["id"], "id": f"sway:{node['id']}",
                "title": node.get("name") or "", "app_id": node.get("app_id") or props.get("class"), "pid": node.get("pid"),
                "workspace": workspace, "output": output, "focused": bool(node.get("focused")),
                "floating": node.get("type") == "floating_con", "visible": bool(node.get("visible")),
                "geometry": geometry if geometry["width"] > 0 and geometry["height"] > 0 else None,
                "size": {"width": geometry["width"], "height": geometry["height"]},
                "toplevel_identifier": node.get("foreign_toplevel_identifier"), "scale": scale,
            })
        for child in children:
            if len(result) >= MAX_COMPOSITOR_WINDOWS:
                return
            walk(child, output, workspace)

    walk(tree, None, None)
    return result


# ---- Hyprland: hyprctl -j clients / monitors --------------------------------

def hyprland_windows(clients, monitors):
    scale = desktop_scale([m.get("scale") for m in monitors])
    shown = {m.get("activeWorkspace", {}).get("id") for m in monitors} | {m.get("specialWorkspace", {}).get("id") for m in monitors}
    names = {m.get("id"): m.get("name") for m in monitors}
    result = []
    for c in clients[:MAX_COMPOSITOR_WINDOWS]:
        if not c.get("mapped", True):
            continue
        at, size = c.get("at") or [0, 0], c.get("size") or [0, 0]
        workspace = c.get("workspace") or {}
        result.append({
            "backend": "hyprland", "native_id": c["address"], "id": f"hyprland:{c['address']}",
            "title": c.get("title") or "", "app_id": c.get("class"), "pid": c.get("pid"),
            "workspace": workspace.get("name") or workspace.get("id"), "output": names.get(c.get("monitor")),
            "focused": c.get("focusHistoryID") == 0, "floating": bool(c.get("floating")),
            "visible": workspace.get("id") in shown and not c.get("hidden"),
            "geometry": rect(at[0] * scale, at[1] * scale, size[0] * scale, size[1] * scale),
            "size": {"width": round(size[0] * scale), "height": round(size[1] * scale)}, "scale": scale,
        })
    return result


# ---- X11 EWMH (X sessions): xprop + xdotool ---------------------------------

def xprop_values(text):
    """Parse `xprop` output lines into {atom: [values]} (strings unquoted, ints decoded)."""
    values = {}
    for line in text.splitlines():
        head, sep, rest = line.partition(" = ")
        if not sep:
            head, sep, rest = line.partition(": ")
            if not sep or "#" not in rest:
                continue
            rest = rest.split("#", 1)[1]
        atom = head.split("(", 1)[0].strip()
        items, current, quoted, escaped, raw = [], "", False, False, ""
        for ch in rest.strip():
            if quoted:
                if escaped:
                    current += ch
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    quoted = False
                    items.append(current)
                    current = ""
                else:
                    current += ch
            elif ch == '"':
                quoted = True
            elif ch == ",":
                if raw.strip():
                    items.append(raw.strip())
                raw = ""
            else:
                raw += ch
        if raw.strip():
            items.append(raw.strip())
        parsed = []
        for item in items:
            try:
                parsed.append(int(item, 0))
            except (TypeError, ValueError):
                parsed.append(item)
        values[atom] = parsed
    return values


def x11_window(xid, props, geometry, active):
    name = (props.get("_NET_WM_NAME") or props.get("WM_NAME") or [""])[0]
    wm_class = props.get("WM_CLASS") or []
    state = props.get("_NET_WM_STATE") or []
    desktop = (props.get("_NET_WM_DESKTOP") or [None])[0]
    pid = (props.get("_NET_WM_PID") or [None])[0]
    return {
        "backend": "x11", "native_id": xid, "id": f"x11:{xid:#x}", "title": str(name),
        "app_id": str(wm_class[-1]) if wm_class else None, "pid": pid if isinstance(pid, int) else None,
        "workspace": desktop if isinstance(desktop, int) else None, "output": None, "focused": xid == active,
        "floating": None, "visible": "_NET_WM_STATE_HIDDEN" not in state, "geometry": geometry,
        "size": {"width": geometry["width"], "height": geometry["height"]} if geometry else None, "scale": 1.0,
    }


def xdotool_geometry(text):
    fields = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    try:
        return rect(int(fields["X"]), int(fields["Y"]), int(fields["WIDTH"]), int(fields["HEIGHT"]))
    except (KeyError, ValueError):
        return None


def x11_list():
    root = xprop_values(run_text(["xprop", "-root", "_NET_CLIENT_LIST", "_NET_ACTIVE_WINDOW"]))
    active = (root.get("_NET_ACTIVE_WINDOW") or [None])[0]
    result = []
    for xid in [x for x in root.get("_NET_CLIENT_LIST", []) if isinstance(x, int)][:MAX_COMPOSITOR_WINDOWS]:
        props = xprop_values(run_text(["xprop", "-id", str(xid), "_NET_WM_NAME", "WM_NAME", "WM_CLASS", "_NET_WM_PID",
                                       "_NET_WM_DESKTOP", "_NET_WM_STATE"]))
        geometry = xdotool_geometry(run_text(["xdotool", "getwindowgeometry", "--shell", str(xid)])) if shutil.which("xdotool") else None
        result.append(x11_window(xid, props, geometry, active))
    return result


# ---- wlr/ext foreign-toplevel via lswt (other wlroots compositors) ----------

def lswt_windows(data):
    result = []
    for index, t in enumerate((data.get("toplevels") or [])[:MAX_COMPOSITOR_WINDOWS]):
        ident = t.get("identifier")
        native = ident or str(index)
        result.append({
            "backend": "foreign-toplevel", "native_id": native, "id": f"toplevel:{native}",
            "title": t.get("title") or "", "app_id": t.get("app-id") or t.get("app_id"), "pid": None,
            "workspace": None, "output": None, "focused": bool(t.get("activated")), "floating": None,
            "visible": False if t.get("minimized") else None, "geometry": None, "size": None,
            "toplevel_identifier": ident, "scale": 1.0,
        })
    return result


# ---- backend selection ------------------------------------------------------

def run_text(command, timeout=3):
    run = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if run.returncode:
        raise ValueError(f"{command[0]} failed: {run.stderr.strip()[:512]}")
    return run.stdout


def compositor_backend():
    """The window backend for this session, chosen at call time (env may change)."""
    if os.environ.get("NIRI_SOCKET"):
        return "niri"
    if os.environ.get("SWAYSOCK") and shutil.which("swaymsg"):
        return "sway"
    if os.environ.get("HYPRLAND_INSTANCE_SIGNATURE") and shutil.which("hyprctl"):
        return "hyprland"
    if os.environ.get("WAYLAND_DISPLAY"):
        return "foreign-toplevel" if shutil.which("lswt") else None
    if os.environ.get("DISPLAY") and shutil.which("xprop"):
        return "x11"
    return None


def compositor_list(backend=None):
    backend = backend or compositor_backend()
    if backend == "niri":
        return niri_list()
    if backend == "sway":
        return sway_windows(json.loads(run_text(["swaymsg", "-r", "-t", "get_tree"])))
    if backend == "hyprland":
        return hyprland_windows(json.loads(run_text(["hyprctl", "-j", "clients"])), json.loads(run_text(["hyprctl", "-j", "monitors"])))
    if backend == "x11":
        return x11_list()
    if backend == "foreign-toplevel":
        return lswt_windows(json.loads(run_text(["lswt", "-j"])))
    return []


def title_key(title):
    """Title without symbol decorations (spinners, bells) that apps animate between reads."""
    return " ".join("".join(c for c in title or "" if unicodedata.category(c) != "So").split())


def correlate(accessible, compositor):
    """Map AT-SPI window keys to compositor entries by pid, then (normalised) title.

    `accessible` is [(key, pid, title, active)]. X11 clients under XWayland
    report the bridge's pid to the compositor, so a globally unique title also
    matches; focus state breaks ties between same-titled windows. Ambiguous
    candidates stay unmatched rather than guessed.
    """
    matches, used = {}, set()
    pids = collections.Counter(item[1] for item in accessible if item[1])
    for normalise in (lambda t: t, title_key):
        for key, pid, title, active in accessible:
            if key in matches:
                continue
            free = [c for c in compositor if c["id"] not in used]
            same_pid = [c for c in free if pid and c.get("pid") == pid]
            if len(same_pid) == 1 and pids[pid] == 1:
                pool = same_pid
            else:
                pool = [c for c in same_pid if normalise(c["title"]) == normalise(title)]
                if not pool and normalise(title):
                    pool = [c for c in free if normalise(c["title"]) == normalise(title)]
            if len(pool) > 1:
                pool = [c for c in pool if c["focused"] == bool(active)]
            if len(pool) == 1:
                matches[key] = pool[0]
                used.add(pool[0]["id"])
    return matches


# ---- coordinate mapping -----------------------------------------------------

def element_point(element, window_extents, origin, scale=1.0):
    """Desktop pixel at the centre of an AT-SPI element.

    On Wayland AT-SPI reports extents relative to the toplevel surface (the
    window's own extents are then at 0,0); on X11 both are screen coordinates.
    Either way the element's offset from the window's own extents, in logical
    pixels, plus the window's desktop origin gives the desktop position.
    """
    return (origin[0] + (element["x"] - window_extents["x"] + element["width"] / 2) * scale,
            origin[1] + (element["y"] - window_extents["y"] + element["height"] / 2) * scale)


def window_point(x, y, origin, image_scale=1.0):
    """Desktop pixel for a point read from a window screenshot (possibly downscaled)."""
    return origin[0] + x / image_scale, origin[1] + y / image_scale


def split_motion(dx, dy, steps):
    """Integer relative-motion events whose running sums track dx, dy exactly."""
    events, sent_x, sent_y = [], 0, 0
    for step in range(1, steps + 1):
        target_x, target_y = round(dx * step / steps), round(dy * step / steps)
        events.append((target_x - sent_x, target_y - sent_y))
        sent_x, sent_y = target_x, target_y
    return events


def distinctive_strips(window, alpha, strip, limit=48):
    """(row, col) of the most textured horizontal strips, spread over the window.

    Texture is the number of colour changes inside the strip; flat regions and
    semi-transparent pixels (shadows, rounded corners) are skipped because the
    desktop composites them differently.
    """
    import numpy as np
    h, w = window.shape[:2]
    changes = (np.abs(np.diff(window[:, :, :3].astype(np.int16), axis=1)).sum(axis=2) > 0).astype(np.int32)
    csum = np.concatenate([np.zeros((h, 1), np.int32), np.cumsum(changes, axis=1)], axis=1)
    rows = np.arange(0, h, 4)
    cols = np.arange(0, w - strip + 1, 8)
    score = csum[rows][:, cols + strip - 1] - csum[rows][:, cols]
    if alpha is not None:
        clear = np.concatenate([np.zeros((h, 1), np.int32), np.cumsum(alpha < 255, axis=1, dtype=np.int32)], axis=1)
        score[(clear[rows][:, cols + strip] - clear[rows][:, cols]) > 0] = 0
    best = {}
    for ri, ci in zip(*np.nonzero(score >= 6)):
        r, c = int(rows[ri]), int(cols[ci])
        key = (r // 48, c // 96)
        if key not in best or score[ri, ci] > best[key][0]:
            best[key] = (int(score[ri, ci]), r, c)
    ranked = sorted(best.values(), reverse=True)
    return [(r, c) for _, r, c in ranked[:limit]]


def locate_window(window, desktop, alpha=None, strip=48):
    """Find where a window capture sits inside a desktop capture.

    `window` and `desktop` are HxWx3 uint8 numpy arrays of the same pixel
    scale. The most textured strips of the window are searched exactly in the
    desktop and vote for an offset (weighted by how unique each match is); the
    winner must clearly beat the runner-up (repetitive content is refused) and
    is verified by sampled pixel agreement, so occlusion or animation in parts
    of the window still localises. Returns {"x", "y", "agreement", "votes"} or None.
    """
    import numpy as np
    wh, ww = window.shape[:2]
    dh, dw = desktop.shape[:2]
    strip = min(strip, ww)
    if strip < 8 or wh < 1:
        return None
    haystack = np.ascontiguousarray(desktop[:, :, :3]).tobytes()
    row_bytes = dw * 3
    votes = collections.defaultdict(float)
    for r, c in distinctive_strips(window, alpha, strip):
        needle = np.ascontiguousarray(window[r, c:c + strip, :3]).tobytes()
        hits, start = set(), 0
        while len(hits) <= 8:
            index = haystack.find(needle, start)
            if index < 0:
                break
            start = index + 1
            y, rem = divmod(index, row_bytes)
            if rem % 3 == 0 and rem // 3 + strip <= dw:
                hits.add((rem // 3 - c, y - r))
        if 0 < len(hits) <= 8:
            for hit in hits:
                votes[hit] += 1 / len(hits)
    ranked = sorted(votes.items(), key=lambda item: item[1], reverse=True)[:2]
    if not ranked or ranked[0][1] < 2 or (len(ranked) > 1 and ranked[1][1] * 1.5 >= ranked[0][1]):
        return None
    (x, y), weight = ranked[0]
    x0, y0 = max(0, x), max(0, y)
    x1, y1 = min(dw, x + ww), min(dh, y + wh)
    if x1 - x0 < 8 or y1 - y0 < 8:
        return None
    ys = np.linspace(y0, y1 - 1, num=min(64, y1 - y0)).astype(int)
    xs = np.linspace(x0, x1 - 1, num=min(64, x1 - x0)).astype(int)
    desk = desktop[np.ix_(ys, xs)][:, :, :3].astype(int)
    win = window[np.ix_(ys - y, xs - x)][:, :, :3].astype(int)
    agreement = float((np.abs(desk - win).max(axis=2) <= 2).mean())
    if agreement < 0.5:
        return None
    return {"x": int(x), "y": int(y), "agreement": round(agreement, 3), "votes": round(weight, 1)}


def clipboard_restore_type(types):
    """The single MIME type worth restoring after a compositor clobbered the clipboard."""
    for preferred in ("text/plain;charset=utf-8", "UTF8_STRING", "text/plain", "STRING", "TEXT"):
        if preferred in types:
            return preferred
    images = [t for t in types if t.startswith("image/")]
    if "image/png" in images:
        return "image/png"
    return images[0] if images else (types[0] if types else None)


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
