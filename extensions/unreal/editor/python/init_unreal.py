# Loaded by the Borg service spec via UE_PYTHONPATH. Only private editor
# backends should load these helpers; tool-local leases are not a security
# boundary against direct stock MCP clients.
import unreal

try:
    import unreal_lane_tools

    if unreal_lane_tools.register():
        unreal.log('UnrealLane: registered UnrealLaneTools toolset')
    else:
        unreal.log_warning('UnrealLane: toolset registry unavailable; lane tools not registered')
except Exception as error:  # noqa: BLE001 - never take the editor down over a helper toolset
    unreal.log_error(f'UnrealLane: failed to register lane tools: {error}')
