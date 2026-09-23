#!/usr/bin/env python3
"""Public JSON-CLI smoke test for service health, endpoint, restart and yield."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
import signal
import select
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
                                 "BORG_LANE_DIR": str(root), "BORG_LANE_SCOPE": "1" if os.environ.get("BORG_BENCH_REQUIRE_SCOPE") else "0",
                                 "BORG_LANE_DEGRADED": "0" if os.environ.get("BORG_BENCH_REQUIRE_SCOPE") else "1",
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
        name = "bench-service-" + uuid.uuid4().hex
        lane(binary, root, "resource", "set-capacity", "--name", name, "--slots", "1")
        resource = {"key": {"scope": "Host", "name": name},
                    "access": {"Shared": {"slots": 1}}}
        spec = {"id": "bench-editor", "argv": [sys.executable, str(backend), "{port}"],
                "cwd": str(root), "env": [], "resources": [resource],
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
        attempted = False
        try:
            attempted = True
            status = command(binary, root, "start", "bench-editor", "--definition", str(definition))
            before = health_at(ports[0])
            leased = command(binary, root, "lease", "bench-editor", "--owner", "bench",
                             "--ttl-seconds", "10", "--purpose", "capture")
            command(binary, root, "release", "bench-editor", "--owner", "bench")
            command(binary, root, "restart", "bench-editor")
            after = health_at(ports[0])
            command(binary, root, "yield", "bench-editor", "--by", "bench-import", "--for-seconds", "3")
            yielded = command(binary, root, "status", "bench-editor")
            command(binary, root, "resume", "bench-editor", "--by", "bench-import")
            resumed(binary, root, "bench-editor")
            restored = health_at(ports[0])
            return {"mode": "service-cli", "started": status.get("state"),
                    "lease_id": leased.get("id"), "before": before.strip(),
                    "after_restart": after.strip(), "yielded": yielded.get("state"),
                    "after_resume": restored.strip(), "front_port_stable": ports[0]}
        finally:
            if attempted:
                # Only this isolated service is eligible for cleanup.
                current = command(binary, root, "status", "bench-editor")
                stopped = command(binary, root, "stop", "bench-editor") if current.get("supervisor_pid") else current
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
                                 "BORG_LANE_DIR": str(root), "BORG_LANE_SCOPE": "1" if os.environ.get("BORG_BENCH_REQUIRE_SCOPE") else "0",
                                 "BORG_LANE_DEGRADED": "0" if os.environ.get("BORG_BENCH_REQUIRE_SCOPE") else "1",
                                 "BORG_LANE_EXECUTABLE": str(binary)})
    if result.returncode:
        if not allow_failure:
            raise RuntimeError(f"{cmd!r}: exit {result.returncode}: {result.stderr.strip()}")
        return {"exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    return json.loads(result.stdout) if result.stdout.strip() else {}


def atomic_worker(binary: Path, root: Path, ports: list[int], marker: Path,
                  child_markers: Path | None = None, stop_owned_service: bool = False,
                  fail_health: bool = False) -> None:
    """Run only inside the granted exclusive job; inspect both services through CLI."""
    checks = {}
    for i, service_id in enumerate(("bench-editor-a", "bench-editor-b")[:len(ports)]):
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
    if child_markers is not None:
        descendants = [json.loads(p.read_text()) for p in child_markers.glob("*.json")]
        if len(descendants) < 2:
            raise RuntimeError(f"expected two detached child records, got {descendants!r}")
        for record in descendants:
            if live_test_child(record):
                raise RuntimeError(f"detached child survived backend yield: {record!r}")
            group = record["cgroup"]
            if "borg-" not in group:
                raise RuntimeError(f"backend child had no dedicated Borg cgroup: {group!r}")
            procs = Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.procs"
            if procs.exists() and procs.read_text().strip():
                raise RuntimeError(f"backend generation cgroup not empty after yield: {procs}")
        checks["detached_scope"] = {"descendants": len(descendants), "empty_before_grant": True}
    if fail_health:
        (root / "health-disabled").touch()
        checks["bench-editor-a"]["health_disabled_for_resume"] = True
    if stop_owned_service:
        # Only this temp-root fixture service, after its backend yielded; no OS signals.
        command(binary, root, "stop", "bench-editor-a")
        checks["bench-editor-a"]["stopped_for_failed_resume"] = True
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


def live_test_child(record: dict) -> bool:
    stat = Path(f"/proc/{record['pid']}/stat")
    try:
        fields = stat.read_text().split()
    except FileNotFoundError:
        return False
    # A zombie cannot hold resources, and PID reuse must never identify another process.
    return fields[21] == record["start_ticks"] and fields[2] != "Z"


def cleanup_test_children(child_markers: Path) -> bool:
    """Terminate only our verified fake descendants if a buggy scope left them alive."""
    leaked = False
    for marker in child_markers.glob("*.json"):
        record = json.loads(marker.read_text())
        if live_test_child(record):
            try:
                cmdline = Path(f"/proc/{record['pid']}/cmdline").read_bytes()
            except FileNotFoundError:
                continue
            if b"gamedev_fake_service.py" not in cmdline or b"--detached-child" not in cmdline:
                raise RuntimeError(f"refusing to signal ambiguous PID {record['pid']}")
            try:
                os.kill(record["pid"], signal.SIGTERM)
                leaked = True
            except ProcessLookupError:
                pass
    return leaked


def post_hook_barrier(started: Path, fifo: Path, done: Path, fail: bool = False) -> None:
    """Mark hook start, then block on kernel FIFO readiness until test releases it."""
    fd = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)
    try:
        started.write_text("hook-blocked")
        ready, _, _ = select.select([fd], [], [], 15)
        if not ready or os.read(fd, 1) != b"x":
            raise RuntimeError("post-hook release marker missing")
        done.write_text("hook-completed")
        if fail:
            raise SystemExit(42)  # Workload succeeded; bound post-hook must quarantine.
    finally:
        os.close(fd)


def wait_marker(marker: Path, root: Path, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not marker.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"test-owned hook never reached marker {marker}")
        subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "create,close_write,moved_to",
                        str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)


def run_atomic(binary: Path, descendant: bool = False, post_barrier: bool = False,
               project_alias: bool = False, canonical_project: bool = False,
               fail_post_hook: bool = False, fail_resume: bool = False,
               fail_health: bool = False, foreign_lease: bool = False,
               late_lease: bool = False, own_lease: bool = False,
               foreign_grace: bool = False, foreign_indefinite: bool = False,
               fail_active_hook: bool = False, per_resource_grace: bool = False) -> dict:
    foreign_lease = foreign_lease or late_lease or foreign_grace or foreign_indefinite or per_resource_grace
    binary = binary.resolve(strict=True)
    backend = Path(__file__).with_name("gamedev_fake_service.py").resolve()
    with isolated_root() as root:
        ports = []
        while len(ports) < (3 if project_alias or canonical_project else 6):
            port = available_port()
            if port not in ports:
                ports.append(port)
        project = root / "project"
        project.mkdir()
        child_markers = root / "detached-children"
        hook_started = root / "post-hook-started"
        hook_done = root / "post-hook-done"
        hook_fifo = root / "post-hook-release"
        if post_barrier:
            os.mkfifo(hook_fifo)
        if descendant:
            child_markers.mkdir()
        # Two-service capacity uses a unique Host key. The one-service alias
        # variant instead exercises canonical Project(path) without capacity setup.
        name = "bench-exclusive-" + uuid.uuid4().hex
        resource_key = {"scope": {"Project": str(project)}, "name": name} if project_alias or canonical_project else {"scope": "Host", "name": name}
        second_name = name + "-second"
        second_key = {"scope": "Host", "name": second_name}
        if not (project_alias or canonical_project):
            lane(binary, root, "resource", "set-capacity", "--name", name,
                 "--slots", "1" if per_resource_grace else "2")
            if per_resource_grace:
                lane(binary, root, "resource", "set-capacity", "--name", second_name, "--slots", "1")
        resource = {"key": resource_key, "access": {"Shared": {"slots": 1}}}
        admission = {"min_available_ram_bytes": 0, "reserve_ram_bytes": 0,
                     "min_free_disk_bytes": 0, "reserve_disk_bytes": 0,
                     "disk_path": str(project)}
        started: list[str] = []
        job_id = None
        idle_marker = root / "bench-editor-b.idle"
        active_marker = root / "bench-editor-b.active"
        try:
            before = {}
            services = ("bench-editor-a",) if project_alias or canonical_project else ("bench-editor-a", "bench-editor-b")
            for i, service_id in enumerate(services):
                spec = {"id": service_id, "argv": [sys.executable, str(backend), "{port}"],
                        "cwd": str(project),
                        "env": ([["BENCH_CHILD_MARKER_DIR", str(child_markers)]] if descendant else [])
                               + ([["BENCH_HEALTH_FAIL_FILE", str(root / "health-disabled")]]
                                  if fail_health and i == 0 else []),
                        "resources": [{"key": second_key, "access": {"Shared": {"slots": 1}}}]
                                     if per_resource_grace and i == 1 else [resource],
                        "adapter_enforces_leases": False, "read_only_paths": ["/", "/health"],
                        "memory_max_bytes": 128 * 1024 * 1024,
                        "admission": admission,
                        "health": {"argv": ["/health"], "kind": "http", "interval_ms": 100,
                                   "timeout_ms": 1000},
                        "restart": {"max_restarts": 3, "backoff_ms": 100, "debounce_ms": 100},
                        "endpoint": {"listen": f"127.0.0.1:{ports[i]}",
                                     "backend_ports": ports[(1 if project_alias or canonical_project else 2)+i*2:
                                                             (3 if project_alias or canonical_project else 4)+i*2]}, "restore": None}
                if i == 1 and (late_lease or fail_active_hook):
                    spec["idle_after_ms"] = 100
                    spec["idle"] = {"argv": [sys.executable, str(Path(__file__).resolve()),
                                               "--borg", str(binary), "--service-hook", str(idle_marker)],
                                    "timeout_ms": 5000}
                    spec["active"] = {"argv": [sys.executable, str(Path(__file__).resolve()),
                                                 "--borg", str(binary), "--service-hook", str(active_marker)]
                                                + (["--fail-service-hook"] if fail_active_hook else []),
                                      "timeout_ms": 5000}
                definition = root / f"{service_id}.json"
                definition.write_text(json.dumps(spec))
                started.append(service_id)
                command(binary, root, "start", service_id, "--definition", str(definition))
                status = command(binary, root, "status", service_id)
                if status.get("backend_pid") is None:
                    raise RuntimeError(f"{service_id} never became healthy")
                before[service_id] = status["backend_pid"]
            if late_lease or fail_active_hook:
                wait_marker(idle_marker, root)
                if active_marker.exists():
                    raise RuntimeError("service active hook ran before a client lease")
            if fail_active_hook:
                failing_owner = str(uuid.uuid4())
                failed = lane(binary, root, "service", "lease", "bench-editor-b",
                              "--owner", failing_owner, "--ttl-seconds", "20",
                              "--purpose", "failing-active-hook", timeout=8, allow_failure=True)
                if "exit_code" not in failed:
                    command(binary, root, "release", "bench-editor-b", "--owner", failing_owner)
                    raise RuntimeError(f"failing active hook accepted a client: {failed!r}")
                failure_text = failed.get("stderr", "") + failed.get("stdout", "")
                if ("service hook failed: exit status: 42" not in failure_text
                        or not active_marker.exists()):
                    raise RuntimeError(f"active hook failure not surfaced by CLI: {failed!r}")
                recovered = command(binary, root, "status", "bench-editor-b")
                if (recovered.get("clients") or recovered.get("backend_pid") != before["bench-editor-b"]
                        or "Healthy" not in recovered.get("state", {}) or not health_at(ports[1])):
                    raise RuntimeError(f"failing active hook left a ghost client or fenced backend: {recovered!r}")
            # Ordinary handoffs use the exclusive holder for the client lease;
            # foreign-client cases deliberately use a different owner.
            holder_id = str(uuid.uuid4())
            client_owner = str(uuid.uuid4()) if foreign_lease and not per_resource_grace else holder_id
            command(binary, root, "lease", "bench-editor-a", "--owner", client_owner,
                    "--ttl-seconds", "20", "--purpose", "capture")
            if per_resource_grace:
                client_owner = str(uuid.uuid4())
                command(binary, root, "lease", "bench-editor-b", "--owner", client_owner,
                        "--ttl-seconds", "20", "--purpose", "different-resource-foreign-client")
            marker = root / "exclusive.json"
            job_key = {"scope": {"Project": str(project / ".." / "project")}, "name": name} if project_alias else resource_key
            worker_ports = ports[:1] if project_alias or canonical_project else ports[:2]
            job_spec = {"fingerprint": "bench-D11-exclusive", "lease": {
                "resources": [{"key": job_key, "access": "Exclusive"}]
                             + ([{"key": second_key, "access": "Exclusive"}] if per_resource_grace else []),
                "holder": {"participant_id": holder_id, "session_id": holder_id,
                           "host_pid": None, "purpose": "atomic editor handoff probe"},
                "queue_timeout_ms": 40000 if late_lease or per_resource_grace else (20000 if foreign_lease else 10000)},
                "argv": [sys.executable, str(Path(__file__).resolve()), "--borg", str(binary),
                         "--atomic-worker", str(root), ",".join(str(p) for p in worker_ports), str(marker),
                         str(child_markers) if descendant else "-"]
                         + (["--stop-owned-service"] if fail_resume else [])
                         + (["--fail-owned-health"] if fail_health else []),
                "cwd": str(project), "env": [], "memory_max_bytes": 128 * 1024 * 1024,
                "admission": admission,
                # This is essential: no adapter-provided yield/resume hook can mask
                # failure to auto-discover ALL services bound to R.
                "pre_hook": None,
                "post_hook": {"argv": [sys.executable, str(Path(__file__).resolve()),
                                        "--borg", str(binary), "--post-hook",
                                        str(hook_started), str(hook_fifo), str(hook_done)]
                                        + (["--post-hook-fail"] if fail_post_hook else []),
                              "timeout_ms": 20000} if post_barrier else None,
                "timeout_ms": 20000 if post_barrier else 10000, "stall_timeout_ms": None, "coalesce": False}
            if per_resource_grace:
                job_spec["foreign_client_grace_ms"] = 5000
                job_spec["foreign_client_grace_by_resource"] = [
                    {"resource": second_key, "grace_ms": 0}]
            alias_rejected = False
            alias_reason = ""
            if project_alias:
                symlink = root / "alias-project"
                symlink.symlink_to(project, target_is_directory=True)
                # Reject symlinks and dot-dot aliases in both filesystem scopes
                # before they can allocate a distinct lock inode.
                for scope, value in (("Project", str(symlink)),
                                     ("Worktree", str(project / ".." / "project")),
                                     ("Worktree", str(symlink))):
                    attempt = json.loads(json.dumps(job_spec))
                    attempt["lease"]["resources"][0]["key"]["scope"] = {scope: value}
                    refused = lane(binary, root, "job", "submit", "--spec", "-",
                                   input_data=attempt, allow_failure=True)
                    if "exit_code" not in refused:
                        raise RuntimeError(f"untrusted {scope} path was accepted: {value}")
                submitted = lane(binary, root, "job", "submit", "--spec", "-",
                                 input_data=job_spec, allow_failure=True)
                if "exit_code" in submitted:
                    alias_rejected = True
                    alias_reason = submitted.get("stderr", "")
                    job_spec["fingerprint"] = "bench-D11-canonical-project"
                    job_spec["lease"]["resources"][0]["key"] = resource_key
                    submitted = lane(binary, root, "job", "submit", "--spec", "-",
                                     input_data=job_spec)
            else:
                submit_args = ["job", "submit", "--spec", "-"]
                if foreign_lease and not per_resource_grace:
                    if foreign_indefinite:
                        grace_seconds = 0
                    elif foreign_grace:
                        grace_seconds = 5
                    elif late_lease:
                        grace_seconds = 30
                    else:
                        grace_seconds = 15
                    submit_args += ["--foreign-lease-grace-seconds", str(grace_seconds)]
                submitted = lane(binary, root, *submit_args, input_data=job_spec)
            job_id = submitted["job_id"]
            foreign_wait_reason = ""
            service_notice = ""
            if foreign_lease:
                # A foreign client stays active: Preparing must hold the exclusive
                # before any fake workload or destructive service yield is allowed.
                deadline = time.monotonic() + 5
                while True:
                    row = lane(binary, root, "job", "status", job_id)
                    if (row.get("state") == "Preparing"
                            and (row.get("wait_reason") or "").startswith("foreign client lease:")):
                        break
                    if row.get("started_ms") is not None or marker.exists():
                        raise RuntimeError(f"foreign client lease did not block grant: {row!r}")
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"foreign client never reached Preparing: {row!r}")
                    subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                    str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                waiting_id = "bench-editor-b" if per_resource_grace else "bench-editor-a"
                waiting_port = ports[1] if per_resource_grace else ports[0]
                before_release = command(binary, root, "status", waiting_id)
                foreign_wait_reason = str(row["wait_reason"])
                service_notice = str(before_release.get("reason") or "")
                if foreign_wait_reason not in service_notice:
                    raise RuntimeError(f"service status omitted foreign client notice: {service_notice!r}")
                if (before_release.get("backend_pid") != before[waiting_id]
                        or not before_release.get("clients") or not health_at(waiting_port)):
                    raise RuntimeError(f"foreign lease was yielded before release: {before_release!r}")
                other_id = "bench-editor-a" if per_resource_grace else "bench-editor-b"
                other_port = ports[0] if per_resource_grace else ports[1]
                if len(started) > 1:
                    other_before = command(binary, root, "status", other_id)
                    if (other_before.get("backend_pid") != before[other_id]
                            or "Healthy" not in other_before.get("state", {})
                            or not health_at(other_port)):
                        raise RuntimeError(f"other bound service yielded before foreign lease released: {other_before!r}")
                if per_resource_grace:
                    if "grace indefinite" not in foreign_wait_reason:
                        raise RuntimeError(f"second-resource zero grace not applied: {foreign_wait_reason!r}")
                    deadline = time.monotonic() + 7
                    while time.monotonic() < deadline:
                        subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                        str(root)], stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, timeout=2)
                        held = lane(binary, root, "job", "status", job_id)
                        waiting = command(binary, root, "status", waiting_id)
                        other = command(binary, root, "status", other_id)
                        if (held.get("state") != "Preparing" or held.get("started_ms") is not None
                                or waiting.get("backend_pid") != before[waiting_id]
                                or not waiting.get("clients") or other.get("backend_pid") != before[other_id]
                                or not health_at(ports[0]) or not health_at(ports[1])):
                            raise RuntimeError(f"two-key grace0 released before foreign client end: {held!r}, {waiting!r}, {other!r}")
                if foreign_indefinite:
                    # A zero grace must remain Preparing across a bounded
                    # observation window, not merely for one status snapshot.
                    deadline = time.monotonic() + 2
                    while time.monotonic() < deadline:
                        subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                        str(root)], stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, timeout=2)
                        held = lane(binary, root, "job", "status", job_id)
                        active = command(binary, root, "status", "bench-editor-a")
                        if (held.get("state") != "Preparing" or held.get("started_ms") is not None
                                or active.get("backend_pid") != before["bench-editor-a"]
                                or not active.get("clients") or not health_at(ports[0])):
                            raise RuntimeError(f"grace=0 released foreign client before lease end: {held!r}, {active!r}")
                if late_lease:
                    # New clients for another bound service must not sneak in
                    # after the exclusive enters Preparing under the same R.
                    late_owner = str(uuid.uuid4())
                    late = lane(binary, root, "service", "lease", "bench-editor-b",
                                "--owner", late_owner, "--ttl-seconds", "10",
                                "--purpose", "late-client", timeout=5, allow_failure=True)
                    if "exit_code" not in late:
                        command(binary, root, "release", "bench-editor-b", "--owner", late_owner)
                        raise RuntimeError(f"late client lease accepted during Preparing: {late!r}")
                    if command(binary, root, "status", "bench-editor-b").get("clients"):
                        raise RuntimeError("late client left a lease after refusal")
                    if active_marker.exists():
                        raise RuntimeError("denied late lease invoked the idle service active hook")
                    # Reusing the active client's owner renews its expiration;
                    # this too must be denied after Preparing, not silently extend.
                    expires_before = before_release["clients"][0]["expires_at_unix_ms"]
                    renewal = lane(binary, root, "service", "lease", "bench-editor-a",
                                   "--owner", client_owner, "--ttl-seconds", "25",
                                   "--purpose", "late-renew", timeout=5, allow_failure=True)
                    if "exit_code" not in renewal:
                        command(binary, root, "release", waiting_id, "--owner", client_owner)
                        raise RuntimeError(f"late client renewal accepted during Preparing: {renewal!r}")
                    renewed = command(binary, root, "status", "bench-editor-a")["clients"]
                    if len(renewed) != 1 or renewed[0]["expires_at_unix_ms"] != expires_before:
                        raise RuntimeError(f"late renewal changed client expiry: {renewed!r}")
                if not foreign_grace:
                    command(binary, root, "release", waiting_id, "--owner", client_owner)
            if post_barrier:
                wait_env = {**os.environ, "BORG_LANES_ROOT": str(root), "BORG_LANE_DIR": str(root),
                            "BORG_LANE_SCOPE": "1", "BORG_LANE_DEGRADED": "0",
                            "BORG_LANE_EXECUTABLE": str(binary)}
                waiter = subprocess.Popen([str(binary), "lane", "--json", "job", "wait", job_id],
                                          env=wait_env, text=True, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
                deadline = time.monotonic() + 10
                while not hook_started.exists() and time.monotonic() < deadline:
                    # Multiple own files may be created before the hook marker;
                    # wait on filesystem notifications, never a sleep/status loop.
                    subprocess.run(["inotifywait", "-q", "-t", "2", "-e", "create,moved_to", str(root)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
                if not hook_started.exists():
                    raise RuntimeError(f"post-hook never reached barrier for job {job_id}")
                for i, service_id in enumerate(started):
                    current = command(binary, root, "status", service_id)
                    if current.get("backend_pid") is not None or job_id not in (current.get("yields") or {}):
                        raise RuntimeError(f"{service_id} lost fencing before post-hook: {current!r}")
                    try:
                        health_at(ports[i])
                    except HTTPError as exc:
                        if exc.code != 503:
                            raise RuntimeError(f"post-hook proxy expected 503, got {exc.code}") from exc
                    else:
                        raise RuntimeError(f"{service_id} proxy routed during post-hook")
                with hook_fifo.open("wb", buffering=0) as release:
                    release.write(b"x")
                out, err = waiter.communicate(timeout=15)
                if waiter.returncode:
                    raise RuntimeError(f"job wait failed after post-hook: {waiter.returncode} {err} {out}")
                if not hook_done.exists():
                    raise RuntimeError("job completed before post-hook reported completion")
                done = json.loads(out)
            else:
                done = lane(binary, root, "job", "wait", job_id, timeout=20, allow_failure=True)
            state = done.get("state", {})
            if not isinstance(state, dict) or state.get("Finished", {}).get("exit_code") != 0 or not marker.exists():
                raise RuntimeError(f"D11 no-hook exclusive did not complete: {done!r}")
            if foreign_grace:
                waited = lane(binary, root, "job", "status", job_id)
                if (waited.get("started_ms") or 0) - (waited.get("created_ms") or 0) < 3500:
                    raise RuntimeError(f"foreign lease did not wait for grace: {waited!r}")
            if fail_post_hook:
                row = lane(binary, root, "job", "status", job_id)
                if not row.get("quarantined") or "post hook failed" not in (row.get("evidence") or ""):
                    raise RuntimeError(f"failed post-hook not quarantined: {row!r}")
                for i, service_id in enumerate(started):
                    current = command(binary, root, "status", service_id)
                    if current.get("backend_pid") is not None:
                        raise RuntimeError(f"{service_id} restarted after failed post-hook")
                    try:
                        health_at(ports[i])
                    except HTTPError as exc:
                        if exc.code != 503:
                            raise RuntimeError(f"quarantined service proxy returned {exc.code}") from exc
                    else:
                        raise RuntimeError(f"{service_id} routed after failed post-hook")
                return {"mode": "D11-post-hook-failure-cli", "job_id": job_id,
                        "workload_exit": state["Finished"]["exit_code"],
                        "quarantined": True, "evidence": row["evidence"],
                        "services_fenced": True}
            if fail_health:
                deadline = time.monotonic() + 36
                while True:
                    row = lane(binary, root, "job", "status", job_id)
                    status = command(binary, root, "status", "bench-editor-a")
                    if row.get("resume_error"):
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"ACK then unhealthy did not journal error: {row!r}, {status!r}")
                    subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                    str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                if ("bench-editor-a" not in row.get("resume_pending", [])
                        or job_id in status.get("yields", {})
                        or isinstance(status.get("state"), dict) and "Healthy" in status["state"]):
                    raise RuntimeError(f"unhealthy Resume ACK cleared pending or kept yield: {row!r}, {status!r}")
                failed_reason = row["resume_error"]
                (root / "health-disabled").unlink()
                # The RPC has already removed the token. Re-enable the fake
                # health endpoint, wait for genuine Healthy, then retry journalling;
                # no second Resume request is sent by the fixture.
                while True:
                    status = command(binary, root, "status", "bench-editor-a")
                    if isinstance(status.get("state"), dict) and "Healthy" in status["state"]:
                        break
                    if status.get("state") == "Stopped":
                        command(binary, root, "start", "bench-editor-a", "--definition",
                                str(root / "bench-editor-a.json"))
                        continue
                    if time.monotonic() >= deadline + 12:
                        raise RuntimeError(f"fake backend did not become Healthy: {status!r}")
                    subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                    str(root / "services" / "bench-editor-a")],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                lane(binary, root, "job", "recover")
                while True:
                    row = lane(binary, root, "job", "status", job_id)
                    if not row.get("resume_pending") and not row.get("resume_error"):
                        break
                    if time.monotonic() >= deadline + 12:
                        raise RuntimeError(f"healthy recovery did not clear pending: {row!r}")
                    subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                    str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                for i, service_id in enumerate(started):
                    resumed(binary, root, service_id)
                    health_at(ports[i])
                return {"mode": "D11-ack-unhealthy-recover-cli", "job_id": job_id,
                        "yield_acknowledged": True, "pending_before": ["bench-editor-a"],
                        "failure": failed_reason, "pending_after": row["resume_pending"],
                        "healthy_after_recover": True}
            if fail_resume:
                deadline = time.monotonic() + 6
                while True:
                    row = lane(binary, root, "job", "status", job_id)
                    if row.get("resume_error"):
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"failed resume did not journal error: {row!r}")
                    subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                    str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                if "bench-editor-a" not in row.get("resume_pending", []) or row.get("quarantined"):
                    raise RuntimeError(f"failed resume row lacks retry: {row!r}")
                failed_reason = row["resume_error"]
                # A yielded service's foreground start blocks until Resume, so launch
                # the public CLI asynchronously and use its status as the readiness signal.
                starter = subprocess.Popen([str(binary), "lane", "--json", "service", "start",
                                            "bench-editor-a", "--definition",
                                            str(root / "bench-editor-a.json")],
                                           env={**os.environ, "BORG_LANES_ROOT": str(root),
                                                "BORG_LANE_DIR": str(root), "BORG_LANE_SCOPE": "1",
                                                "BORG_LANE_DEGRADED": "0",
                                                "BORG_LANE_EXECUTABLE": str(binary)},
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    while True:
                        status = command(binary, root, "status", "bench-editor-a")
                        if status.get("state") != "Stopped" and status.get("supervisor_pid"):
                            break
                        if starter.poll() is not None:
                            raise RuntimeError(f"owned service start exited {starter.returncode}: {status!r}")
                        if time.monotonic() >= deadline + 6:
                            raise RuntimeError(f"owned service did not start: {status!r}")
                        subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                        str(root / "services" / "bench-editor-a")],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                    lane(binary, root, "job", "recover")
                finally:
                    # If start is still waiting on the yield, recover will release it;
                    # if the gate fails, terminate only this test-owned CLI child.
                    if starter.poll() is None:
                        try:
                            starter.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            starter.terminate()
                            starter.wait(timeout=2)
                while True:
                    row = lane(binary, root, "job", "status", job_id)
                    if not row.get("resume_pending") and not row.get("resume_error"):
                        break
                    if time.monotonic() >= deadline + 6:
                        raise RuntimeError(f"recover did not clear failed resume: {row!r}")
                    subprocess.run(["inotifywait", "-q", "-t", "1", "-e", "modify,close_write,moved_to",
                                    str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                after = {service_id: resumed(binary, root, service_id)["backend_pid"]
                         for service_id in started}
                for i in range(len(started)):
                    health_at(ports[i])
                return {"mode": "D11-failed-resume-recover-cli", "job_id": job_id,
                        "failed_resume_error": failed_reason, "pending_before": ["bench-editor-a"],
                        "pending_after": row["resume_pending"], "backend_after": after}
            after = {service_id: resumed(binary, root, service_id)["backend_pid"]
                     for service_id in started}
            for i in range(len(started)):
                health_at(ports[i])
            return {"mode": "D11-atomic-cli", "job_id": job_id, "backend_before": before,
                    "checks": json.loads(marker.read_text()), "backend_after": after,
                    "auto_resumed": True, "job_hooks": post_barrier,
                    "post_hook_before_resume": post_barrier, "scoped_descendants": descendant,
                    "project_alias": project_alias, "canonical_project": canonical_project or alias_rejected,
                    "alias_rejected": alias_rejected, "alias_refusal": alias_reason[:300],
                    "foreign_lease_waited": foreign_lease,
                    "foreign_wait_reason": foreign_wait_reason if foreign_lease else None,
                    "service_notice": service_notice if foreign_lease else None,
                    "both_healthy_while_waiting": foreign_lease and len(started) > 1,
                    "late_lease_refused": late_lease, "idle_active_hook_suppressed": late_lease,
                    "failing_active_hook_rolled_back": fail_active_hook,
                    "own_lease_unblocked": own_lease,
                    "foreign_grace_expired": foreign_grace,
                    "indefinite_wait_released": foreign_indefinite,
                    "indefinite_observation_seconds": 2 if foreign_indefinite else None,
                    "per_resource_grace_held_seconds": 7 if per_resource_grace else None}
        finally:
            if post_barrier and hook_started.exists() and not hook_done.exists():
                try:
                    release_fd = os.open(hook_fifo, os.O_WRONLY | os.O_NONBLOCK)
                    os.write(release_fd, b"x")
                    os.close(release_fd)
                except OSError:
                    pass
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
            cleanup_errors = []
            for service_id in reversed(started):
                try:
                    current = command(binary, root, "status", service_id)
                    stopped = command(binary, root, "stop", service_id) if current.get("supervisor_pid") else current
                    if stopped.get("state") != "Stopped":
                        cleanup_errors.append(f"{service_id} did not stop: {stopped!r}")
                except Exception as exc:
                    cleanup_errors.append(f"{service_id} stop failed: {exc}")
            if descendant:
                try:
                    if cleanup_test_children(child_markers):
                        cleanup_errors.append("backend scope leaked a detached test child after stop")
                except Exception as exc:
                    cleanup_errors.append(f"child cleanup failed: {exc}")
            if cleanup_errors:
                raise RuntimeError("; ".join(cleanup_errors))



def service_budget_gate(binary: Path, disk: bool = False) -> dict:
    """Disjoint services reserve more than one host/device can grant together."""
    binary = binary.resolve(strict=True)
    backend = Path(__file__).with_name("gamedev_fake_service.py").resolve()
    with isolated_root() as root:
        project = root / "project"
        project.mkdir()
        other_disk_path = root / "same-device-other-path"
        other_disk_path.mkdir()
        if project.stat().st_dev != other_disk_path.stat().st_dev:
            raise RuntimeError("budget fixture paths must share a filesystem")
        if disk:
            fs = os.statvfs(project)
            available = fs.f_bavail * fs.f_frsize
        else:
            available_kib = next(int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines()
                                 if line.startswith("MemAvailable:"))
            available = available_kib * 1024
        reserve = (available // 5) * 3
        if reserve < 1024 * 1024 * 1024:
            raise RuntimeError("shared host/device too short on free capacity for safe admission probe")
        ports = []
        while len(ports) < 6:
            port = available_port()
            if port not in ports:
                ports.append(port)
        started: list[str] = []
        try:
            for index, service_id in enumerate(("bench-budget-a", "bench-budget-b")):
                definition = root / f"{service_id}.json"
                spec = {"id": service_id,
                        "argv": [sys.executable, str(backend), "{port}"],
                        "cwd": str(project), "env": [],
                        "resources": [{"key": {"scope": "Host", "name": service_id + "-R"},
                                       "access": {"Shared": {"slots": 1}}}],
                        "adapter_enforces_leases": False, "read_only_paths": ["/", "/health"],
                        "memory_max_bytes": 128 * 1024 * 1024,
                        "admission": {"min_available_ram_bytes": 0,
                                      "reserve_ram_bytes": 0 if disk else reserve,
                                      "min_free_disk_bytes": 0,
                                      "reserve_disk_bytes": reserve if disk else 0,
                                      "disk_path": str(project if index == 0 else other_disk_path)},
                        "health": {"argv": ["/health"], "kind": "http", "interval_ms": 100,
                                   "timeout_ms": 1000},
                        "restart": {"max_restarts": 3, "backoff_ms": 100, "debounce_ms": 100},
                        "endpoint": {"listen": f"127.0.0.1:{ports[index]}",
                                     "backend_ports": ports[2+index*2:4+index*2]},
                        "restore": None}
                definition.write_text(json.dumps(spec))
                started.append(service_id)
                if index == 0:
                    command(binary, root, "start", service_id, "--definition", str(definition),
                            "--wait-ready", "5")
                    if command(binary, root, "status", service_id).get("backend_pid") is None:
                        raise RuntimeError("first budget service never admitted")
                else:
                    # CLI may return nonzero after a one-second readiness deadline;
                    # its supervisor must persist and expose the admission reason.
                    lane(binary, root, "service", "start", service_id,
                         "--definition", str(definition), "--wait-ready", "1",
                         allow_failure=True)
            second = command(binary, root, "status", "bench-budget-b")
            kind = "disk" if disk else "RAM"
            if second.get("backend_pid") is not None or f"{kind} admission queued" not in second.get("reason", ""):
                raise RuntimeError(f"combined service {kind} reservation not enforced: {second!r}")
            command(binary, root, "yield", "bench-budget-a", "--by", "bench-budget-test",
                    "--for-seconds", "10")
            admitted = resumed(binary, root, "bench-budget-b")
            health_at(ports[1])
            return {"mode": "service-disk-budget-cli" if disk else "service-RAM-budget-cli",
                    "reserved_bytes_each": reserve,
                    "total_reservation_bytes": reserve * 2,
                    "second_wait_reason": second["reason"],
                    "second_admitted_after_first_yield": admitted.get("backend_pid") is not None}
        finally:
            errors = []
            for service_id in reversed(started):
                try:
                    current = command(binary, root, "status", service_id)
                    stopped = command(binary, root, "stop", service_id) if current.get("supervisor_pid") else current
                    if stopped.get("state") != "Stopped":
                        errors.append(f"{service_id} did not stop")
                except Exception as exc:
                    errors.append(f"{service_id} stop failed: {exc}")
            if errors:
                raise RuntimeError("; ".join(errors))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--borg", required=True, type=Path)
    ap.add_argument("--atomic", action="store_true", help="run degraded-mode D11 coordination gate")
    ap.add_argument("--check-service-disk-budget", action="store_true",
                    help="disjoint service admissions share same-device disk reservation")
    ap.add_argument("--check-service-budget", action="store_true",
                    help="two disjoint services cannot overreserve host RAM")
    ap.add_argument("--atomic-project-alias", action="store_true",
                    help="systemd-only canonical Project(path) alias exclusive handoff")
    ap.add_argument("--atomic-foreign-indefinite", action="store_true",
                    help="systemd-only grace=0 holds foreign client until public release")
    ap.add_argument("--atomic-foreign-grace", action="store_true",
                    help="systemd-only foreign client yielded after bounded grace")
    ap.add_argument("--atomic-own-lease", action="store_true",
                    help="systemd-only own holder client does not block an exclusive")
    ap.add_argument("--atomic-per-resource-grace", action="store_true",
                    help="systemd-only second-key grace=0 outlasts job-wide five-second grace")
    ap.add_argument("--atomic-active-hook-rollback", action="store_true",
                    help="systemd-only failed active hook clears client before exclusive")
    ap.add_argument("--service-hook", type=Path)
    ap.add_argument("--fail-service-hook", action="store_true")
    ap.add_argument("--atomic-late-lease", action="store_true",
                    help="systemd-only refuse a late client while exclusive Preparing")
    ap.add_argument("--atomic-foreign-lease", action="store_true",
                    help="systemd-only wait on test-owned foreign service client then release")
    ap.add_argument("--atomic-unhealthy-resume", action="store_true",
                    help="systemd-only resumed backend fails health; retry without duplicate Resume")
    ap.add_argument("--fail-owned-health", action="store_true")
    ap.add_argument("--atomic-failed-resume", action="store_true",
                    help="systemd-only stopped test-service resume/recover gate")
    ap.add_argument("--stop-owned-service", action="store_true")
    ap.add_argument("--atomic-post-hook-fail", action="store_true",
                    help="systemd-only failed bound post-hook quarantine gate")
    ap.add_argument("--post-hook-fail", action="store_true")
    ap.add_argument("--atomic-post-hook", action="store_true",
                    help="systemd-only post-hook completion before service resume gate")
    ap.add_argument("--post-hook", nargs=3, metavar=("STARTED", "FIFO", "DONE"))
    ap.add_argument("--atomic-descendant", action="store_true",
                    help="systemd-only two-service gate with detached child cgroup assertions")
    ap.add_argument("--atomic-worker", nargs=4, metavar=("ROOT", "PORT", "MARKER", "CHILD_DIR"))
    args = ap.parse_args()
    if args.service_hook:
        args.service_hook.write_text("hook-called")
        if args.fail_service_hook:
            raise SystemExit(42)
    elif args.post_hook:
        post_hook_barrier(*(Path(value) for value in args.post_hook), fail=args.post_hook_fail)
    elif args.atomic_worker:
        root, port, marker, child_dir = args.atomic_worker
        atomic_worker(args.borg, Path(root), [int(p) for p in port.split(",")], Path(marker),
                      Path(child_dir) if child_dir != "-" else None, args.stop_owned_service,
                      args.fail_owned_health)
    else:
        if args.atomic_descendant or args.atomic_post_hook or args.atomic_post_hook_fail or args.atomic_failed_resume or args.atomic_unhealthy_resume or args.atomic_foreign_lease or args.atomic_foreign_grace or args.atomic_foreign_indefinite or args.atomic_late_lease or args.atomic_active_hook_rollback or args.atomic_per_resource_grace or args.atomic_own_lease or args.atomic_project_alias or args.check_service_budget or args.check_service_disk_budget:
            manager = subprocess.run(["systemctl", "--user", "show-environment"],
                                     capture_output=True, timeout=3)
            if manager.returncode:
                raise RuntimeError("systemd user manager unavailable; cannot test cgroup gate")
            os.environ["BORG_BENCH_REQUIRE_SCOPE"] = "1"
        if args.check_service_budget or args.check_service_disk_budget:
            result = service_budget_gate(args.borg, disk=args.check_service_disk_budget)
        elif args.atomic or args.atomic_descendant or args.atomic_post_hook or args.atomic_post_hook_fail or args.atomic_failed_resume or args.atomic_unhealthy_resume or args.atomic_foreign_lease or args.atomic_foreign_grace or args.atomic_foreign_indefinite or args.atomic_late_lease or args.atomic_active_hook_rollback or args.atomic_per_resource_grace or args.atomic_own_lease or args.atomic_project_alias:
            result = run_atomic(args.borg, descendant=args.atomic_descendant,
                                post_barrier=args.atomic_post_hook or args.atomic_post_hook_fail,
                                project_alias=args.atomic_project_alias,
                                fail_post_hook=args.atomic_post_hook_fail,
                                fail_resume=args.atomic_failed_resume,
                                fail_health=args.atomic_unhealthy_resume,
                                foreign_lease=args.atomic_foreign_lease,
                                late_lease=args.atomic_late_lease,
                                own_lease=args.atomic_own_lease,
                                foreign_grace=args.atomic_foreign_grace,
                                foreign_indefinite=args.atomic_foreign_indefinite,
                                fail_active_hook=args.atomic_active_hook_rollback,
                                per_resource_grace=args.atomic_per_resource_grace)
            if args.atomic_project_alias and not result["alias_rejected"]:
                result["canonical_handoff"] = run_atomic(args.borg, canonical_project=True)
        else:
            result = run(args.borg)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
