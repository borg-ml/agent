#!/usr/bin/env python3
"""End PIE, request editor quit, then wait for its backend to exit."""
from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path

from lane_mcp import Client, EDITOR_APP, LANE_TOOLS


def process_state(pid: int) -> tuple[str, str] | None:
    """Return state and Linux start identity, or None if the process exited."""
    if sys.platform == 'linux':
        try:
            fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        except FileNotFoundError:
            return None
        return fields[0], fields[19]  # state, starttime (field 22)
    # Borg cannot reap its child until the hook exits; zombies count as exited.
    proc = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True,
                          check=False)
    return (proc.stdout.strip()[:1], '') if proc.returncode == 0 else None


def port_closed(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(('127.0.0.1', port)) != 0


def exited(pid: int, identity: tuple[str, str] | None) -> bool:
    state = process_state(pid)
    return state is None or state[0] == 'Z' or (identity is not None and state[1] != identity[1])


def wait_for_exit(pid: int, port: int, identity: tuple[str, str] | None, deadline: float) -> bool:
    while time.monotonic() < deadline:
        if exited(pid, identity) and port_closed(port):
            return True
        time.sleep(min(0.2, max(0, deadline - time.monotonic())))
    return exited(pid, identity) and port_closed(port)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--pid', type=int, required=True)
    parser.add_argument('--timeout-seconds', type=float, default=85)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.pid <= 0 or args.timeout_seconds <= 0:
        parser.error('invalid port, pid or timeout')
    deadline = time.monotonic() + args.timeout_seconds  # Within the core's 90 s hook bound.
    identity = process_state(args.pid)
    if exited(args.pid, identity) and port_closed(args.port):
        return 0
    client = Client(f'http://127.0.0.1:{args.port}/mcp', timeout=10)
    try:
        client.initialize()
        if client.call(EDITOR_APP, 'IsPIERunning'):
            client.timeout = min(60, max(0.1, deadline - time.monotonic() - 15))
            client.call(EDITOR_APP, 'StopPIE')
        client.timeout = min(10, max(0.1, deadline - time.monotonic() - 5))
        client.call(LANE_TOOLS, 'exec_console', {'command': 'QUIT_EDITOR', 'owner': 'lane-supervisor'})
    finally:
        client.close()
    if not wait_for_exit(args.pid, args.port, identity, deadline):
        print(f'editor {args.pid} did not exit and close MCP port {args.port} before timeout',
              file=sys.stderr)
        return 1  # Core will terminate only its tracked backend cgroup.
    return 0


if __name__ == '__main__':
    sys.exit(main())
