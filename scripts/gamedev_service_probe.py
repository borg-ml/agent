#!/usr/bin/env python3
"""Public JSON-CLI smoke test for service health, endpoint, restart and yield."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
from urllib.request import urlopen


def available_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def command(binary: Path, root: Path, *args: str) -> dict:
    cmd = [str(binary), "lane", "--json", "service", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15,
                            env={**os.environ, "BORG_LANES_ROOT": str(root),
                                 "BORG_LANE_SCOPE": "0"})
    if result.returncode:
        raise RuntimeError(f"{cmd!r}: exit {result.returncode}: {result.stderr.strip()}")
    return json.loads(result.stdout)


def health_at(port: int) -> str:
    with urlopen(f"http://127.0.0.1:{port}/", timeout=2) as response:
        if response.status != 200:
            raise RuntimeError(f"front proxy returned {response.status}")
        return response.read().decode()


def run(binary: Path) -> dict:
    binary = binary.resolve(strict=True)
    backend = Path(__file__).with_name("gamedev_fake_service.py").resolve()
    with tempfile.TemporaryDirectory(prefix="borg-service-bench-") as tmp:
        root = Path(tmp)
        ports = []
        while len(ports) < 3:
            port = available_port()
            if port not in ports:
                ports.append(port)
        spec = {"id": "bench-editor", "argv": [sys.executable, str(backend), "{port}"],
                "cwd": str(root), "env": [], "resources": [],
                "memory_max_bytes": 128 * 1024 * 1024,
                "admission": {"min_available_ram_bytes": 0, "reserve_ram_bytes": 0,
                              "min_free_disk_bytes": 0, "reserve_disk_bytes": 0,
                              "disk_path": str(root)},
                "health": {"argv": ["/health"], "kind": "http", "interval_ms": 100,
                           "timeout_ms": 1000},
                "restart": {"max_restarts": 3, "backoff_ms": 100, "debounce_ms": 100},
                "endpoint": {"listen": f"127.0.0.1:{ports[0]}",
                             "backend_ports": ports[1:]}, "restore": None}
        definition = root / "service.json"
        definition.write_text(json.dumps(spec))
        started = False
        try:
            status = command(binary, root, "start", "bench-editor", "--definition", str(definition))
            started = True
            before = health_at(ports[0])
            leased = command(binary, root, "lease", "bench-editor", "--owner", "bench",
                             "--ttl-seconds", "10", "--purpose", "capture")
            command(binary, root, "release", "bench-editor", "--owner", "bench")
            command(binary, root, "restart", "bench-editor")
            after = health_at(ports[0])
            command(binary, root, "yield", "bench-editor", "--by", "bench-import", "--for-seconds", "3")
            yielded = command(binary, root, "status", "bench-editor")
            command(binary, root, "resume", "bench-editor", "--by", "bench-import")
            restored = health_at(ports[0])
            return {"mode": "service-cli", "started": status.get("state"),
                    "lease_id": leased.get("id"), "before": before.strip(),
                    "after_restart": after.strip(), "yielded": yielded.get("state"),
                    "after_resume": restored.strip(), "front_port_stable": ports[0]}
        finally:
            if started:
                # Only the service created in this isolated root can be stopped.
                command(binary, root, "stop", "bench-editor")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--borg", required=True, type=Path)
    args = ap.parse_args()
    print(json.dumps(run(args.borg), indent=2))


if __name__ == "__main__":
    main()
