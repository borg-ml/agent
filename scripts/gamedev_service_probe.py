#!/usr/bin/env python3
"""Public JSON-CLI smoke test for service health, endpoint, restart and yield."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
import uuid
import tempfile
from urllib.request import urlopen


@contextmanager
def isolated_root():
    root = Path(tempfile.mkdtemp(prefix="borg-service-bench-"))
    try:
        yield root
    except BaseException:
        print(f"Failed service probe retained for diagnosis: {root}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(root)


def available_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def command(binary: Path, root: Path, *args: str) -> dict:
    cmd = [str(binary), "lane", "--json", "service", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15,
                            env={**os.environ, "BORG_LANES_ROOT": str(root),
                                 "BORG_LANE_DIR": str(root), "BORG_LANE_SCOPE": "0",
                                 "BORG_LANE_EXECUTABLE": str(binary)})
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
    with isolated_root() as root:
        ports = []
        while len(ports) < 3:
            port = available_port()
            if port not in ports:
                ports.append(port)
        spec = {"id": "bench-editor", "argv": [sys.executable, str(backend), "{port}"],
                "cwd": str(root), "env": [], "resources": [],
                "adapter_enforces_leases": False, "read_only_paths": ["/", "/health"],
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
                stopped = command(binary, root, "stop", "bench-editor")
                if stopped.get("state") not in ("Stopped", {"Stopped": None}):
                    status = command(binary, root, "status", "bench-editor")
                    if status.get("state") not in ("Stopped", {"Stopped": None}):
                        raise RuntimeError(f"own test service did not stop; preserve state: {status!r}")



def lane(binary: Path, root: Path, *args: str, input_data: dict | None = None,
         timeout: float = 10, allow_failure: bool = False) -> dict:
    cmd = [str(binary), "lane", "--json", *args]
    result = subprocess.run(cmd, input=json.dumps(input_data) if input_data is not None else None,
                            capture_output=True, text=True, timeout=timeout,
                            env={**os.environ, "BORG_LANES_ROOT": str(root),
                                 "BORG_LANE_DIR": str(root), "BORG_LANE_SCOPE": "0",
                                 "BORG_LANE_EXECUTABLE": str(binary)})
    if result.returncode:
        if not allow_failure:
            raise RuntimeError(f"{cmd!r}: exit {result.returncode}: {result.stderr.strip()}")
        return {"exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    return json.loads(result.stdout) if result.stdout.strip() else {}


def atomic_worker(binary: Path, root: Path, ports: list[int], marker: Path) -> None:
    """Run only inside the granted exclusive job; inspect both services through CLI."""
    checks = {}
    for i, service_id in enumerate(("bench-editor-a", "bench-editor-b")):
        status = command(binary, root, "status", service_id)
        if status.get("backend_pid") is not None or status.get("state") != "Yielded":
            raise RuntimeError(f"exclusive started before {service_id} yielded: {status!r}")
        if status.get("clients"):
            raise RuntimeError(f"unrestored client lease before exclusive grant: {status!r}")
        try:
            health_at(ports[i])
        except HTTPError as exc:
            if exc.code != 503:
                raise RuntimeError(f"{service_id} proxy returned {exc.code}, expected 503") from exc
        else:
            raise RuntimeError(f"{service_id} proxy still forwarded during exclusive job")
        # A restart may error or defer; it must not launch while exclusive R is held.
        lane(binary, root, "service", "restart", service_id, allow_failure=True)
        if command(binary, root, "status", service_id).get("backend_pid") is not None:
            raise RuntimeError(f"{service_id} restarted during exclusive lease")
        checks[service_id] = {"yielded_before_grant": True, "client_restored": True,
                              "proxy_503": True, "restart_fenced": True}
    end = time.monotonic() + .3
    while time.monotonic() < end:
        _ = sum(range(50))
    marker.write_text(json.dumps(checks))


def resumed(binary: Path, root: Path, service_id: str) -> dict:
    """Wait on state-change notifications, never a sleep/status polling loop."""
    deadline = time.monotonic() + 5
    events_dir = root / "services" / service_id
    while True:
        status = command(binary, root, "status", service_id)
        if status.get("backend_pid") is not None and isinstance(status.get("state"), dict):
            if "Healthy" in status["state"]:
                return status
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{service_id} did not auto-resume after exclusive job: {status!r}")
        # Atomic state replacement emits MOVED_TO; timeout is a final bounded check.
        subprocess.run(["inotifywait", "-q", "-t", "2", "-e", "moved_to", str(events_dir)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)


def run_atomic(binary: Path) -> dict:
    binary = binary.resolve(strict=True)
    backend = Path(__file__).with_name("gamedev_fake_service.py").resolve()
    with isolated_root() as root:
        ports = []
        while len(ports) < 6:
            port = available_port()
            if port not in ports:
                ports.append(port)
        project = root / "project"
        project.mkdir()
        # Host scope is isolated by a unique name; both services share precisely R.
        name = "bench-exclusive-" + uuid.uuid4().hex
        resource_key = {"scope": "Host", "name": name}
        lane(binary, root, "resource", "set-capacity", "--name", name, "--slots", "2")
        resource = {"key": resource_key, "access": {"Shared": {"slots": 1}}}
        admission = {"min_available_ram_bytes": 0, "reserve_ram_bytes": 0,
                     "min_free_disk_bytes": 0, "reserve_disk_bytes": 0,
                     "disk_path": str(project)}
        started: list[str] = []
        job_id = None
        try:
            before = {}
            for i, service_id in enumerate(("bench-editor-a", "bench-editor-b")):
                spec = {"id": service_id, "argv": [sys.executable, str(backend), "{port}"],
                        "cwd": str(project), "env": [], "resources": [resource],
                        "adapter_enforces_leases": False, "read_only_paths": ["/", "/health"],
                        "memory_max_bytes": 128 * 1024 * 1024,
                        "admission": admission,
                        "health": {"argv": ["/health"], "kind": "http", "interval_ms": 100,
                                   "timeout_ms": 1000},
                        "restart": {"max_restarts": 3, "backoff_ms": 100, "debounce_ms": 100},
                        "endpoint": {"listen": f"127.0.0.1:{ports[i]}",
                                     "backend_ports": ports[2+i*2:4+i*2]}, "restore": None}
                definition = root / f"{service_id}.json"
                definition.write_text(json.dumps(spec))
                command(binary, root, "start", service_id, "--definition", str(definition))
                started.append(service_id)
                status = command(binary, root, "status", service_id)
                if status.get("backend_pid") is None:
                    raise RuntimeError(f"{service_id} never became healthy")
                before[service_id] = status["backend_pid"]
            command(binary, root, "lease", "bench-editor-a", "--owner", "bench-client",
                    "--ttl-seconds", "10", "--purpose", "capture")
            marker = root / "exclusive.json"
            job_spec = {"fingerprint": "bench-D11-exclusive", "lease": {
                "resources": [{"key": resource_key, "access": "Exclusive"}],
                "holder": {"participant_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
                           "host_pid": None, "purpose": "atomic editor handoff probe"},
                "queue_timeout_ms": 10000},
                "argv": [sys.executable, str(Path(__file__).resolve()), "--borg", str(binary),
                         "--atomic-worker", str(root), f"{ports[0]},{ports[1]}", str(marker)],
                "cwd": str(project), "env": [], "memory_max_bytes": 128 * 1024 * 1024,
                "admission": admission,
                # This is essential: no adapter-provided yield/resume hook can mask
                # failure to auto-discover ALL services bound to R.
                "pre_hook": None, "post_hook": None,
                "timeout_ms": 10000, "stall_timeout_ms": None, "coalesce": False}
            submitted = lane(binary, root, "job", "submit", "--spec", "-", input_data=job_spec)
            job_id = submitted["job_id"]
            done = lane(binary, root, "job", "wait", job_id, timeout=20, allow_failure=True)
            state = done.get("state", {})
            if not isinstance(state, dict) or state.get("Finished", {}).get("exit_code") != 0 or not marker.exists():
                raise RuntimeError(f"D11 no-hook exclusive did not complete: {done!r}")
            after = {service_id: resumed(binary, root, service_id)["backend_pid"]
                     for service_id in started}
            for i in range(2):
                health_at(ports[i])
            return {"mode": "D11-atomic-cli", "job_id": job_id, "backend_before": before,
                    "checks": json.loads(marker.read_text()), "backend_after": after,
                    "auto_resumed": True, "job_hooks": False}
        finally:
            if job_id is not None:
                try:
                    status = lane(binary, root, "job", "status", job_id)
                    job = status.get("job") or {}
                    state = job.get("state") or {}
                    if not isinstance(state, dict) or not any(k in state for k in ("Finished", "Cancelled")):
                        lane(binary, root, "job", "cancel", job_id)
                        lane(binary, root, "job", "wait", job_id, allow_failure=True)
                except Exception:
                    pass  # Preserve this test-owned state for diagnosis.
            for service_id in reversed(started):
                stopped = command(binary, root, "stop", service_id)
                if stopped.get("state") != "Stopped":
                    raise RuntimeError(f"synthetic {service_id} did not stop: {stopped!r}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--borg", required=True, type=Path)
    ap.add_argument("--atomic", action="store_true", help="run D11 exclusive project handoff")
    ap.add_argument("--atomic-worker", nargs=3, metavar=("ROOT", "PORT", "MARKER"))
    args = ap.parse_args()
    if args.atomic_worker:
        root, port, marker = args.atomic_worker
        atomic_worker(args.borg, Path(root), [int(p) for p in port.split(",")], Path(marker))
    else:
        print(json.dumps(run_atomic(args.borg) if args.atomic else run(args.borg), indent=2))


if __name__ == "__main__":
    main()
