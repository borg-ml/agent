# Unreal editor service preview

`python3 /path/to/extensions/unreal/bin/unreal.py --project /path/to/Game.uproject --engine-root /path/to/UE editor spec`
shows the core service template. An integrated `borg lane service` CLI is
required for `editor start|status|restart|yield|resume|stop|lease|release`.
Do not start a second editor against someone else's active project; coordinate
with its owner first. Do not bind a port already in use (especially a shared
editor's port). The service is declared with project-run ownership and
loopback ports, but no real editor has been validated here.

`mcp ...` is intentionally unavailable: stock Unreal MCP tools do not enforce
owner leases. Do not connect directly to its backend or promise safe PIE,
cvar, capture or camera restoration until a complete owner-checking adapter
proxy is implemented and tested.
