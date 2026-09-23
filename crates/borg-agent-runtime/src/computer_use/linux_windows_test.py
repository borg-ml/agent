
# Unit tests for linux_windows.py; run by computer_use.rs with the module prepended.
import unittest
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linux_windows import *  # noqa: F403


class CompositorParsing(unittest.TestCase):
    def test_niri_geometry_only_for_floating_windows_on_active_workspaces(self):
        outputs = {"DP-1": {"logical": {"x": 100, "y": 0, "width": 1280, "height": 720, "scale": 2.0}}}
        workspaces = [{"id": 1, "idx": 1, "name": None, "output": "DP-1", "is_active": True},
                      {"id": 2, "idx": 2, "name": "games", "output": "DP-1", "is_active": False}]
        layout = {"window_size": [400, 300], "window_offset_in_tile": [4.0, 4.0], "tile_pos_in_workspace_view": None}
        windows = [
            {"id": 7, "title": "Tiled", "app_id": "a", "pid": 1, "workspace_id": 1, "is_focused": True, "is_floating": False, "layout": layout},
            {"id": 8, "title": "Float", "app_id": "b", "pid": 2, "workspace_id": 1, "is_focused": False, "is_floating": True,
             "layout": {**layout, "tile_pos_in_workspace_view": [10.0, 20.0]}},
            {"id": 9, "title": "Hidden", "app_id": "UnrealEditor", "pid": 3, "workspace_id": 2, "is_focused": False, "is_floating": True,
             "layout": {**layout, "tile_pos_in_workspace_view": [10.0, 20.0]}},
        ]
        tiled, floating, hidden = niri_windows(windows, workspaces, outputs)
        self.assertEqual((tiled["id"], tiled["geometry"], tiled["visible"], tiled["focused"]), ("niri:7", None, None, True))
        # (output x + tile x + border) * desktop scale
        self.assertEqual(floating["geometry"], {"x": 228, "y": 48, "width": 800, "height": 600})
        self.assertTrue(floating["visible"])
        self.assertEqual((hidden["visible"], hidden["geometry"], hidden["workspace"]), (False, None, "games"))

    def test_sway_leaf_geometry_is_absolute_content_rect(self):
        tree = {"type": "root", "nodes": [{"type": "output", "name": "HDMI-A-1", "scale": 2.0, "nodes": [
            {"type": "workspace", "name": "3", "nodes": [
                {"type": "con", "id": 41, "name": "Game", "app_id": None, "pid": 77, "focused": True, "visible": True,
                 "window_properties": {"class": "UnrealEditor"}, "window": 123,
                 "rect": {"x": 10, "y": 30, "width": 500, "height": 400},
                 "window_rect": {"x": 2, "y": 20, "width": 496, "height": 378}, "nodes": [], "floating_nodes": []}],
             "floating_nodes": []}]}]}
        (game,) = sway_windows(tree)
        self.assertEqual(game["id"], "sway:41")
        self.assertEqual(game["app_id"], "UnrealEditor")
        self.assertEqual((game["workspace"], game["output"]), ("3", "HDMI-A-1"))
        self.assertEqual(game["geometry"], {"x": 24, "y": 100, "width": 992, "height": 756})

    def test_hyprland_visibility_follows_monitor_workspaces(self):
        monitors = [{"id": 0, "name": "DP-2", "scale": 1.0, "activeWorkspace": {"id": 1}, "specialWorkspace": {"id": 0}}]
        clients = [{"address": "0xabc", "mapped": True, "hidden": False, "at": [5, 6], "size": [700, 500],
                    "workspace": {"id": 1, "name": "1"}, "monitor": 0, "class": "steam_app_1", "title": "Game",
                    "pid": 9, "focusHistoryID": 0},
                   {"address": "0xdef", "mapped": True, "at": [0, 0], "size": [1, 1], "workspace": {"id": 4, "name": "4"},
                    "monitor": 0, "class": "x", "title": "Other", "pid": 10, "focusHistoryID": 3}]
        game, other = hyprland_windows(clients, monitors)
        self.assertEqual((game["focused"], game["visible"], game["geometry"]["x"]), (True, True, 5))
        self.assertEqual((other["focused"], other["visible"]), (False, False))

    def test_xprop_values_and_ewmh_window(self):
        root = xprop_values("_NET_CLIENT_LIST(WINDOW): window id # 0x1a00003, 0x2c00007\n"
                            "_NET_ACTIVE_WINDOW(WINDOW): window id # 0x2c00007\n")
        self.assertEqual(root["_NET_CLIENT_LIST"], [0x1a00003, 0x2c00007])
        props = xprop_values('_NET_WM_NAME(UTF8_STRING) = "Unreal \\"Editor\\", 5.4"\n'
                             'WM_CLASS(STRING) = "UnrealEditor", "UnrealEditor"\n'
                             "_NET_WM_PID(CARDINAL) = 4242\n"
                             "_NET_WM_STATE(ATOM) = _NET_WM_STATE_FOCUSED\n")
        geometry = xdotool_geometry("WINDOW=46137351\nX=12\nY=34\nWIDTH=800\nHEIGHT=600\nSCREEN=0\n")
        entry = x11_window(0x2c00007, props, geometry, root["_NET_ACTIVE_WINDOW"][0])
        self.assertEqual((entry["id"], entry["title"], entry["app_id"], entry["pid"], entry["focused"]),
                         ("x11:0x2c00007", 'Unreal "Editor", 5.4', "UnrealEditor", 4242, True))
        self.assertEqual(entry["geometry"], {"x": 12, "y": 34, "width": 800, "height": 600})

    def test_lswt_toplevels(self):
        (item,) = lswt_windows({"toplevels": [{"title": "T", "app-id": "foot", "identifier": "abc", "activated": True}]})
        self.assertEqual((item["id"], item["app_id"], item["focused"], item["toplevel_identifier"]), ("toplevel:abc", "foot", True, "abc"))


class Correlation(unittest.TestCase):
    def window(self, native, pid, title, focused=False):
        return {"id": f"niri:{native}", "pid": pid, "title": title, "focused": focused}

    def test_pid_title_and_decorated_titles(self):
        compositor = [self.window(1, 10, "⠋ Borg Agent • ~/x"), self.window(2, 10, "🔔 Borg Agent • ~"),
                      self.window(3, 20, "Solo"), self.window(4, 99, "gedit X11 via bridge")]
        matches = correlate([("a", 10, "⠙ Borg Agent • ~/x", False), ("b", 10, "Borg Agent • ~", False),
                             ("c", 20, "renamed", False), ("d", 5, "gedit X11 via bridge", False)], compositor)
        self.assertEqual({k: v["id"] for k, v in matches.items()},
                         {"a": "niri:1", "b": "niri:2", "c": "niri:3", "d": "niri:4"})

    def test_ambiguous_titles_are_not_guessed_unless_focus_decides(self):
        compositor = [self.window(1, 10, "Same"), self.window(2, 10, "Same")]
        self.assertEqual(correlate([("a", 10, "Same", False), ("b", 10, "Same", False)], compositor), {})
        compositor[1]["focused"] = True
        matches = correlate([("a", 10, "Same", False), ("b", 10, "Same", True)], compositor)
        self.assertEqual({k: v["id"] for k, v in matches.items()}, {"a": "niri:1", "b": "niri:2"})


class Mapping(unittest.TestCase):
    def test_element_point_wayland_relative_and_x11_absolute(self):
        element = {"x": 40, "y": 10, "width": 20, "height": 10}
        # Wayland: AT-SPI extents relative to the toplevel (window extents at 0,0), 2x scale.
        self.assertEqual(element_point(element, {"x": 0, "y": 0}, (1000, 200), 2.0), (1100.0, 230.0))
        # X11-style absolute extents: only the offset from the window's own extents matters.
        absolute = {"x": 540, "y": 310, "width": 20, "height": 10}
        self.assertEqual(element_point(absolute, {"x": 500, "y": 300}, (500, 300)), (550.0, 315.0))

    def test_window_point_undoes_screenshot_downscale(self):
        self.assertEqual(window_point(50, 25, (100, 200), 0.5), (200.0, 250.0))

    def test_split_motion_is_exact_and_smooth(self):
        for dx, dy, steps in ((100, -37, 7), (3, 0, 10), (-500, 250.4, 50), (0, 1, 1)):
            events = split_motion(dx, dy, steps)
            self.assertEqual(len(events), steps)
            self.assertEqual((sum(e[0] for e in events), sum(e[1] for e in events)), (round(dx), round(dy)))
            self.assertLessEqual(max(abs(e[0]) for e in events), math.ceil(abs(dx) / steps) + 1)

    def test_clipboard_restore_prefers_text(self):
        self.assertEqual(clipboard_restore_type(["image/png", "text/plain", "text/plain;charset=utf-8"]), "text/plain;charset=utf-8")
        self.assertEqual(clipboard_restore_type(["image/jpeg", "image/png"]), "image/png")
        self.assertIsNone(clipboard_restore_type([]))


class Localisation(unittest.TestCase):
    def setUp(self):
        try:
            import numpy
        except ImportError:
            self.skipTest("numpy unavailable")
        self.np = numpy

    def test_finds_window_despite_occlusion_and_offscreen_part(self):
        np = self.np
        rng = np.random.default_rng(7)
        desktop = rng.integers(0, 255, (300, 400, 3), dtype=np.uint8)
        window = rng.integers(0, 255, (120, 160, 3), dtype=np.uint8)
        desktop[50:170, 230:390] = window
        desktop[60:100, 240:300] = 0  # a popup covering part of the window
        self.assertEqual({k: v for k, v in locate_window(window, desktop).items() if k in ("x", "y")}, {"x": 230, "y": 50})
        # Scrolled partly off the left edge.
        clipped = rng.integers(0, 255, (300, 400, 3), dtype=np.uint8)
        clipped[100:220, 0:100] = window[:, 60:]
        self.assertEqual({k: v for k, v in locate_window(window, clipped).items() if k in ("x", "y")}, {"x": -60, "y": 100})

    def test_repetitive_content_localises_only_by_its_unique_parts(self):
        np = self.np
        tile = np.zeros((40, 40, 3), dtype=np.uint8)
        tile[2:38, 2:38] = (40, 90, 160)  # flat cells with dark borders, like a grid or tile map
        window = np.tile(tile, (4, 6, 1))  # 160x240 of a repeating pattern
        desktop = np.zeros((400, 600, 3), dtype=np.uint8)
        desktop[100:260, 200:440] = window
        # Without unique content every period is an equally good answer: refuse.
        self.assertIsNone(locate_window(window, desktop))
        labels = np.random.default_rng(2)
        for y, x in ((5, 60), (70, 150), (120, 20)):  # a few unique labels, like cell captions
            window[y:y + 8, x:x + 60] = labels.integers(0, 255, (8, 60, 3), dtype=np.uint8)
        desktop[100:260, 200:440] = window
        self.assertEqual({k: v for k, v in locate_window(window, desktop).items() if k in ("x", "y")}, {"x": 200, "y": 100})

    def test_refuses_absent_or_featureless_windows(self):
        np = self.np
        rng = np.random.default_rng(3)
        desktop = rng.integers(0, 255, (200, 200, 3), dtype=np.uint8)
        self.assertIsNone(locate_window(rng.integers(0, 255, (50, 60, 3), dtype=np.uint8), desktop))
        flat = np.full((50, 60, 3), 30, dtype=np.uint8)
        desktop[10:60, 10:70] = flat
        self.assertIsNone(locate_window(flat, desktop))


result = unittest.main(argv=["linux_windows_test"], exit=False, verbosity=1).result
raise SystemExit(0 if result.wasSuccessful() else 1)
