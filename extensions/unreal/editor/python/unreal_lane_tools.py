"""UnrealLaneTools: the pieces the stock MCP toolsets lack for a visual loop.

EditorToolset.EditorAppToolset can move the level-viewport camera and capture
the level viewport, but it cannot set a console variable, run a console command,
or capture the Play-In-Editor viewport (CaptureViewport reads the level editor
viewport, which is not what renders during PIE). These tools fill those gaps.

Loaded only inside the Borg-managed editor service (UE_PYTHONPATH). Every tool runs on the game thread, so each call is atomic with
respect to other MCP clients. Multi-step work (camera + cvars + capture, or a
PIE session) holds the lease; while it is held, the mutating tools here refuse
callers that pass a different owner. This is not a security boundary:
stock MCP tools and direct backend connections bypass this in-tool lease. Cvars are remembered per owner and are
restored automatically when the owner's lease expires.

Idle throttle: after UE_EDITOR_LANE_IDLE_SECONDS (default 60) without a call to
these tools and with no lease held, the editor drops to t.MaxFPS
UE_EDITOR_LANE_IDLE_FPS (default 5) and the level viewport stops rendering in
realtime; any lane tool call restores UE_EDITOR_LANE_ACTIVE_FPS (default 60)
before it does its work.

State lives in the unreal_lane_state module, so `reload_tools` (importlib
reload) keeps leases, cvar originals and the throttle.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
import types

import toolset_registry
import unreal
from toolset_registry.registration import Registration

_LANE_CAMERA_TAG = 'UnrealLaneCamera'
IDLE_SECONDS = float(os.environ.get('UE_EDITOR_LANE_IDLE_SECONDS', '60'))
IDLE_FPS = int(os.environ.get('UE_EDITOR_LANE_IDLE_FPS', '5'))
ACTIVE_FPS = int(os.environ.get('UE_EDITOR_LANE_ACTIVE_FPS', '60'))


def _state() -> types.ModuleType:
    state = sys.modules.get('unreal_lane_state')
    if state is None:
        state = types.ModuleType('unreal_lane_state')
        state.cvars = {}          # name -> {'original': str, 'owner': str}
        state.lease = {'owner': '', 'expires': 0.0, 'purpose': ''}
        state.activity = time.monotonic()
        state.mode = ''           # 'active' | 'idle' | '' (not yet applied)
        state.tick_handle = None
        state.ticks = 0
        state.window_start = time.monotonic()
        state.fps = 0.0
        sys.modules['unreal_lane_state'] = state
    return state


S = _state()


def _editor_subsystem() -> unreal.UnrealEditorSubsystem:
    return unreal.get_editor_subsystem(unreal.UnrealEditorSubsystem)


def _pie_world() -> unreal.World | None:
    return _editor_subsystem().get_game_world()


def _active_world() -> unreal.World:
    world = _pie_world() or _editor_subsystem().get_editor_world()
    if world is None:
        raise RuntimeError('No editor or PIE world is available.')
    return world


def _pie_player_controller() -> unreal.PlayerController | None:
    world = _pie_world()
    if world is None:
        return None
    return unreal.GameplayStatics.get_player_controller(world, 0)


def _exec(command: str) -> str:
    """Run a console command where it will be understood.

    During PIE the command is routed through the PIE player controller, so it
    reaches the game viewport, the game's own exec handlers and the PIE world.
    Otherwise it goes to the editor engine against the editor world.
    """
    controller = _pie_player_controller()
    if controller is not None:
        unreal.SystemLibrary.execute_console_command(_pie_world(), command, controller)
        return 'pie'
    unreal.SystemLibrary.execute_console_command(_active_world(), command)
    return 'editor'


def _restore_owner_cvars(owner: str | None) -> dict[str, str]:
    """Restore cvars set by owner (every owner when None)."""
    restored = {}
    for name, entry in list(S.cvars.items()):
        if owner is None or entry['owner'] == owner:
            _exec(f'{name} {entry["original"]}')
            restored[name] = unreal.SystemLibrary.get_console_variable_string_value(name)
            S.cvars.pop(name, None)
    return restored


def _lease_state() -> dict[str, object]:
    remaining = float(S.lease['expires']) - time.monotonic()
    if remaining <= 0.0 and S.lease['owner']:
        expired = str(S.lease['owner'])
        S.lease.update(owner='', expires=0.0, purpose='')
        restored = _restore_owner_cvars(expired)
        unreal.log_warning(f'UnrealLane: lease of {expired!r} expired; restored cvars {restored}')
    remaining = max(0.0, float(S.lease['expires']) - time.monotonic())
    return {'holder': S.lease['owner'], 'purpose': S.lease['purpose'],
            'expires_in_seconds': round(remaining, 1)}


SUPERVISOR_OWNER = 'lane-supervisor'  # Scripts/editor_lane.sh only: stop/yield must never be refused


def _check_owner(owner: str) -> None:
    if owner == SUPERVISOR_OWNER:
        return
    state = _lease_state()
    if state['holder'] and owner != state['holder']:
        raise RuntimeError(
            f'lane lease is held by {state["holder"]!r} ({state["purpose"] or "no purpose"}) for '
            f'{state["expires_in_seconds"]}s more; pass owner={state["holder"]!r} if that is you, '
            'or take the lease when it is free')


# ------------------------------------------------------------------ throttle

def _apply_mode(mode: str) -> None:
    if S.mode == mode:
        return
    fps = ACTIVE_FPS if mode == 'active' else IDLE_FPS
    unreal.SystemLibrary.execute_console_command(None, f't.MaxFPS {fps}')
    try:
        # Realtime off stops the level viewport redrawing every frame; PIE keeps
        # rendering its own viewport regardless.
        unreal.get_editor_subsystem(unreal.LevelEditorSubsystem).editor_set_viewport_realtime(mode == 'active')
    except Exception as error:  # noqa: BLE001 - older API or no level viewport; t.MaxFPS still applies
        unreal.log_warning(f'UnrealLane: viewport realtime toggle unavailable: {error}')
    S.mode = mode
    unreal.log(f'UnrealLane: throttle -> {mode} (t.MaxFPS {fps})')


def _touch() -> None:
    """Mark lane activity and make sure the editor renders at full rate."""
    S.activity = time.monotonic()
    _apply_mode('active')


def _on_tick(delta_seconds: float) -> None:
    now = time.monotonic()
    S.ticks += 1
    if now - S.window_start >= 5.0:
        S.fps = S.ticks / (now - S.window_start)
        S.ticks, S.window_start = 0, now
    if S.mode != 'idle' and now - S.activity >= IDLE_SECONDS and not _lease_state()['holder']:
        _apply_mode('idle')


def install_throttle() -> None:
    if S.tick_handle is not None:
        unreal.unregister_slate_post_tick_callback(S.tick_handle)
    # Look the callback up through the module so a reload swaps the code too.
    S.tick_handle = unreal.register_slate_post_tick_callback(
        lambda delta: sys.modules[__name__]._on_tick(delta))
    _touch()


# --------------------------------------------------------------------- tools

@unreal.uclass()
class UnrealLaneTools(unreal.ToolsetDefinition):
    """Unreal project editor-lane helpers: console variables and commands, PIE view
    control, file-based high-resolution capture of the live view (PIE or level
    viewport), and a lease that serialises multi-step agent work on the one
    shared editor. Pass the same owner string to every call of a sequence."""

    @toolset_registry.tool_call
    @staticmethod
    def lane_status() -> str:
        """Returns a JSON summary of the lane: PIE state, worlds, lease, cvars
        not yet restored (with their owners), throttle mode and measured tick
        rate. Does not count as activity, so polling it never wakes the lane.

        Returns:
            JSON object as a string.
        """
        pie = _pie_world()
        editor_world = _editor_subsystem().get_editor_world()
        controller = _pie_player_controller()
        view = {}
        if controller is not None:
            manager = controller.player_camera_manager
            pawn = controller.get_controlled_pawn()
            if manager is not None:
                loc, rot = manager.get_camera_location(), manager.get_camera_rotation()
                view['camera'] = {'x': loc.x, 'y': loc.y, 'z': loc.z,
                                  'pitch': rot.pitch, 'yaw': rot.yaw, 'roll': rot.roll}
            if pawn is not None:
                loc = pawn.get_actor_location()
                view['pawn'] = {'x': loc.x, 'y': loc.y, 'z': loc.z}
        return json.dumps({
            'pid': os.getpid(),
            'pie_running': pie is not None,
            'pie_has_player': controller is not None,
            'editor_world': editor_world.get_path_name() if editor_world else '',
            'pie_world': pie.get_path_name() if pie else '',
            'pie_view': view,
            'lease': _lease_state(),
            'unrestored_cvars': {name: dict(entry) for name, entry in S.cvars.items()},
            'throttle': {'mode': S.mode, 'tick_fps': round(S.fps, 1),
                         'idle_after_seconds': IDLE_SECONDS,
                         'idle_in_seconds': round(max(0.0, IDLE_SECONDS - (time.monotonic() - S.activity)), 1)},
        })

    @toolset_registry.tool_call
    @staticmethod
    def wake(owner: str) -> str:
        """Restores full frame rate without changing anything else. Call it
        before using stock EditorToolset tools directly after the lane idled.

        Args:
            owner: Caller identity (informational).

        Returns:
            The throttle mode, 'active'.
        """
        _touch()
        return S.mode

    @toolset_registry.tool_call
    @staticmethod
    def get_cvar(name: str) -> str:
        """Reads a console variable's current value as a string. An unknown
        variable also reads as an empty string; use
        EditorToolset.EditorAppToolset.SearchCVars to confirm a name exists.

        Args:
            name: Console variable name, e.g. 'r.Tonemapper.Sharpen'.

        Returns:
            The current value.
        """
        return unreal.SystemLibrary.get_console_variable_string_value(name)

    @toolset_registry.tool_call
    @staticmethod
    def set_cvar(name: str, value: str, owner: str) -> str:
        """Sets a console variable and returns its previous value. The first
        original value is remembered under owner, so restore_cvars (or the
        owner's lease expiring) puts the shared editor back. Refused while
        another owner holds the lease.

        Args:
            name: Console variable name, e.g. 'r.Tonemapper.Sharpen'.
            value: New value as a string.
            owner: Caller identity; the lease holder if a lease is held.

        Returns:
            JSON {"name", "previous", "current", "target"}.
        """
        _check_owner(owner)
        _touch()
        previous = unreal.SystemLibrary.get_console_variable_string_value(name)
        entry = S.cvars.get(name)
        if entry is not None and entry['owner'] != owner:
            raise RuntimeError(f'{name} was changed by {entry["owner"]!r} and not restored yet')
        S.cvars.setdefault(name, {'original': previous, 'owner': owner})
        target = _exec(f'{name} {value}')
        current = unreal.SystemLibrary.get_console_variable_string_value(name)
        if current == previous and str(value) != previous:
            # Commands such as a read-only cvar silently ignore the set.
            unreal.log_warning(f'UnrealLane: {name} unchanged after set ({previous!r})')
        if current == S.cvars[name]['original']:
            S.cvars.pop(name, None)
        return json.dumps({'name': name, 'previous': previous, 'current': current, 'target': target})

    @toolset_registry.tool_call
    @staticmethod
    def restore_cvars(owner: str) -> str:
        """Restores the console variables owner changed through set_cvar to the
        value they had before the first change. owner '*' restores every
        owner's changes (cleanup after a crashed client).

        Args:
            owner: Caller identity, or '*' for all.

        Returns:
            JSON map of name -> restored value.
        """
        _touch()
        return json.dumps(_restore_owner_cvars(None if owner == '*' else owner))

    @toolset_registry.tool_call
    @staticmethod
    def exec_console(command: str, owner: str) -> str:
        """Runs a console command in the PIE session if one is running (through
        the player controller, so game exec commands work), otherwise in the
        editor world. Output is not returned; read the log with
        EditorToolset.LogsToolset. Changes made this way are NOT tracked for
        restore: use set_cvar for console variables.

        Args:
            command: The console command line.
            owner: Caller identity; the lease holder if a lease is held.

        Returns:
            'pie' or 'editor': where the command was executed.
        """
        _check_owner(owner)
        _touch()
        return _exec(command)

    @toolset_registry.tool_call
    @staticmethod
    def set_pie_view(x: float, y: float, z: float, pitch: float, yaw: float,
                     roll: float, fov: float, owner: str) -> str:
        """Points the PIE player's view through a lane camera at an explicit pose
        (world space, centimetres and degrees), independent of the pawn. Use
        clear_pie_view to hand the view back to the pawn.

        Args:
            x: World X (cm).
            y: World Y (cm).
            z: World Z (cm).
            pitch: Pitch in degrees.
            yaw: Yaw in degrees.
            roll: Roll in degrees.
            fov: Horizontal field of view in degrees.
            owner: Caller identity; the lease holder if a lease is held.

        Returns:
            Path name of the lane camera actor.
        """
        _check_owner(owner)
        _touch()
        world = _pie_world()
        controller = _pie_player_controller()
        if world is None or controller is None:
            raise RuntimeError('set_pie_view needs a running PIE session with a player.')
        location = unreal.Vector(x, y, z)
        rotation = unreal.Rotator(roll=roll, pitch=pitch, yaw=yaw)
        camera = UnrealLaneTools._find_lane_camera(world)
        if camera is None:
            # Editor scripting refuses to spawn while PIE runs, and deferred
            # spawning is Blueprint-internal, so spawn through the PIE cheat
            # manager, which runs synchronously inside the PIE world.
            before = set(unreal.GameplayStatics.get_all_actors_of_class(world, unreal.CameraActor))
            if controller.cheat_manager is None:
                unreal.SystemLibrary.execute_console_command(world, 'EnableCheats', controller)
            unreal.SystemLibrary.execute_console_command(
                world, 'summon /Script/Engine.CameraActor', controller)
            spawned = [a for a in unreal.GameplayStatics.get_all_actors_of_class(world, unreal.CameraActor)
                       if a not in before]
            if not spawned:
                raise RuntimeError('Could not spawn the lane camera in the PIE world.')
            camera = spawned[0]
            camera.tags = [_LANE_CAMERA_TAG]
        camera.set_actor_location_and_rotation(location, rotation, False, True)
        component = camera.get_editor_property('camera_component')
        component.set_editor_property('field_of_view', fov)
        component.set_editor_property('constrain_aspect_ratio', False)
        controller.set_view_target_with_blend(camera, 0.0)
        return camera.get_path_name()

    @staticmethod
    def _find_lane_camera(world: unreal.World) -> unreal.CameraActor | None:
        for actor in unreal.GameplayStatics.get_all_actors_of_class_with_tag(
                world, unreal.CameraActor, _LANE_CAMERA_TAG):
            return actor
        return None

    @toolset_registry.tool_call
    @staticmethod
    def clear_pie_view(owner: str) -> str:
        """Returns the PIE view to the player's pawn and removes the lane camera.

        Args:
            owner: Caller identity; the lease holder if a lease is held.

        Returns:
            'cleared' or 'no-pie'.
        """
        _check_owner(owner)
        _touch()
        world = _pie_world()
        controller = _pie_player_controller()
        if world is None or controller is None:
            return 'no-pie'
        pawn = controller.get_controlled_pawn()
        if pawn is not None:
            controller.set_view_target_with_blend(pawn, 0.0)
        camera = UnrealLaneTools._find_lane_camera(world)
        if camera is not None:
            camera.destroy_actor()
        return 'cleared'

    @toolset_registry.tool_call
    @staticmethod
    def set_hud_visible(visible: bool, owner: str) -> bool:
        """Shows or hides the PIE player's HUD (AHUD.bShowHUD) and returns the
        previous state so the caller can restore it.

        Args:
            visible: True to draw the HUD, False to hide it.
            owner: Caller identity; the lease holder if a lease is held.

        Returns:
            The previous visibility.
        """
        _check_owner(owner)
        _touch()
        controller = _pie_player_controller()
        hud = controller.get_hud() if controller is not None else None
        if hud is None:
            raise RuntimeError('set_hud_visible needs a running PIE session with a HUD.')
        previous = bool(hud.get_editor_property('show_hud'))
        hud.set_editor_property('show_hud', bool(visible))
        return previous

    @toolset_registry.tool_call
    @staticmethod
    def request_capture(path: str, width: int, height: int, owner: str) -> str:
        """Requests a high-resolution PNG of the live view: the PIE game viewport
        while PIE runs, otherwise the level editor viewport. The file is written
        a frame or two after this call returns; poll for it. Any existing file at
        the path is deleted first so its appearance means a fresh frame.

        Args:
            path: Absolute output path ending in .png.
            width: Output width in pixels (1..7680).
            height: Output height in pixels (1..4320).
            owner: Caller identity; the lease holder if a lease is held.

        Returns:
            JSON {"path", "target"} where target is 'pie' or 'editor'.
        """
        _check_owner(owner)
        _touch()
        if not os.path.isabs(path) or not path.lower().endswith('.png'):
            raise RuntimeError('path must be absolute and end in .png')
        if width <= 0 or height <= 0 or width > 7680 or height > 4320:
            raise RuntimeError('width/height must be within 1..7680 x 1..4320')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            os.remove(path)
        target = _exec(f'HighResShot {int(width)}x{int(height)} filename={path}')
        return json.dumps({'path': path, 'target': target})

    @toolset_registry.tool_call
    @staticmethod
    def lease(owner: str, seconds: float, purpose: str) -> str:
        """Takes or renews the lane lease for a multi-step sequence (camera +
        cvars + capture, or PIE). Granted when free, expired, or already held
        by the same owner. While held, the mutating lane tools refuse other
        owners; the holder's cvars are restored when it expires.

        Args:
            owner: A stable identifier for the calling agent.
            seconds: Lease duration, 1..3600 seconds.
            purpose: Short description shown in lane status, e.g. 'pie'.

        Returns:
            JSON {"granted", "holder", "purpose", "expires_in_seconds"}.
        """
        if not owner or owner == '*':
            raise RuntimeError('owner must be a non-empty identifier other than "*"')
        _touch()
        seconds = max(1.0, min(float(seconds), 3600.0))
        state = _lease_state()
        granted = state['holder'] in ('', owner)
        if granted:
            S.lease.update(owner=owner, expires=time.monotonic() + seconds, purpose=purpose or '')
            state = _lease_state()
        return json.dumps({'granted': granted, **state})

    @toolset_registry.tool_call
    @staticmethod
    def release(owner: str) -> str:
        """Releases the lane lease if held by owner. Cvars the owner changed stay
        tracked until restore_cvars; release does not restore them.

        Args:
            owner: The identifier used with lease.

        Returns:
            JSON {"released", "holder", "purpose", "expires_in_seconds"}.
        """
        state = _lease_state()
        released = state['holder'] == owner
        if released:
            S.lease.update(owner='', expires=0.0, purpose='')
        return json.dumps({'released': released, **_lease_state()})

    @toolset_registry.tool_call
    @staticmethod
    def reload_tools() -> str:
        """Reloads this module from disk and re-registers the toolset, keeping
        leases, tracked cvars and the throttle. Use after editing
        Scripts/editor_lane/python/unreal_lane_tools.py.

        Returns:
            'reloaded'.
        """
        sys.modules[__name__]._registration.unregister()
        module = importlib.reload(sys.modules[__name__])
        if not module.register():
            raise RuntimeError('toolset registry unavailable after reload')
        return 'reloaded'


_registration = Registration([UnrealLaneTools])


def register() -> bool:
    ok = _registration.register()
    install_throttle()
    return ok
