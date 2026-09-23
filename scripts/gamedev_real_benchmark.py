#!/usr/bin/env python3
"""Exercise the public `borg lane` JSON CLI with isolated synthetic jobs."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from gamedev_benchmark import BUILD, EDITOR, workloads


def cli(binary: Path, root: Path, *args: str, input_data: dict | None = None,
        timeout: float = 90) -> dict | list:
    cmd = [str(binary), "lane", "--state-dir", str(root), "--json", *args]
    result = subprocess.run(cmd, input=json.dumps(input_data) if input_data is not None else None,
                            capture_output=True, text=True, timeout=timeout, check=False,
                            env={**os.environ, "BORG_LANE_SCOPE": "0",
                                 "BORG_LANE_EXECUTABLE": str(binary)})
    if result.returncode:
        raise RuntimeError(f"{cmd!r}: exit {result.returncode}: {result.stderr.strip()} {result.stdout.strip()}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{cmd!r}: expected JSON, got {result.stdout[:300]!r}") from exc


def state_done(state: object) -> bool:
    return isinstance(state, dict) and state.get("Finished", {}).get("exit_code") == 0


@contextmanager
def isolated_root():
    root = Path(tempfile.mkdtemp(prefix="borg-bench-"))
    try:
        yield root
    except BaseException:
        print(f"Failed CLI probe retained for diagnosis: {root}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(root)


def run(binary: Path, agents: int, jobs: int, scale: float, seed: int) -> dict:
    tasks = workloads(agents, jobs, seed)
    binary = binary.resolve(strict=True)
    script = Path(__file__).with_name("gamedev_benchmark.py").resolve()
    with isolated_root() as root:
        lane = root / "lanes"
        project = root / "project"
        project.mkdir()
        cli(binary, lane, "resource", "set-capacity", "--name", "bench-ram", "--slots", "20")
        ids: set[str] = set()

        def agent_sequence(agent: int) -> list[dict]:
            records = []
            for req in tasks[agent]:
                duration = max(req.duration / scale, 0.5 if req.index == 0 else 0.04)
                # First same-source jobs also share their exact argv and spec.
                ram_mib = req.ram * 16  # under 320 MiB model-wide
                worker_argv = [sys.executable, str(script), "--worker", str(duration), str(ram_mib), ".25"]
                keys = [{"key": {"scope": "Host", "name": "bench-ram"},
                         "access": {"Shared": {"slots": req.ram}}},
                        {"key": {"scope": {"Worktree": str(project)}, "name": req.key},
                         "access": "Exclusive"}]
                if req.kind == "import":
                    keys.append({"key": {"scope": {"Project": str(project)}, "name": "editor"},
                                 "access": "Exclusive"})
                elif req.kind in EDITOR:
                    keys[1]["key"]["scope"] = {"Project": str(project)}
                spec = {"fingerprint": f"{req.kind}:{req.key}:{req.fingerprint}",
                        "lease": {"resources": keys,
                                  "holder": {"participant_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"bench-agent-{agent}")),
                                             "session_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"bench-run-{seed}")),
                                             "host_pid": None, "purpose": "benchmark fake workload"},
                                  "queue_timeout_ms": 30000},
                        "argv": worker_argv, "cwd": str(project), "env": [],
                        "memory_max_bytes": 512 * 1024 * 1024,
                        "admission": {"min_available_ram_bytes": 128 * 1024 * 1024,
                                      "reserve_ram_bytes": 0, "min_free_disk_bytes": 10 * 1024 * 1024,
                                      "reserve_disk_bytes": 0, "disk_path": str(project)},
                        "pre_hook": None, "post_hook": None,
                        "timeout_ms": 30000, "stall_timeout_ms": None,
                        "coalesce": req.kind in BUILD}
                arrival = time.monotonic()
                submitted = cli(binary, lane, "job", "submit", "--spec", "-", input_data=spec)
                if not isinstance(submitted, dict) or not isinstance(submitted.get("job_id"), str):
                    raise RuntimeError(f"missing job_id: {submitted!r}")
                job_id = submitted["job_id"]
                ids.add(job_id)
                done = cli(binary, lane, "job", "wait", job_id, timeout=40)
                if not isinstance(done, dict) or not state_done(done.get("state")):
                    raise RuntimeError(f"job {job_id} did not finish: {done!r}")
                records.append({"id": job_id, "kind": req.kind, "arrived": arrival,
                                "finished": time.monotonic(), "agent": agent})
            return records

        # No agent-side polling: CLI wait blocks on the lane completion event.
        with ThreadPoolExecutor(max_workers=agents) as pool:
            futures = [pool.submit(agent_sequence, i) for i in range(agents)]
            try:
                records = [record for f in futures for record in f.result()]
            except Exception:
                for job_id in ids:
                    try:
                        cli(binary, lane, "job", "cancel", job_id)
                        cli(binary, lane, "job", "wait", job_id, timeout=5)
                    except Exception:
                        pass  # Preserve the state directory for manual recovery.
                raise
        statuses: list[dict] = []
        for job_id in sorted(ids):
            status = cli(binary, lane, "job", "status", job_id)
            if not isinstance(status, dict) or not state_done(status.get("job", {}).get("state")):
                raise RuntimeError(f"incomplete public job status: {status!r}")
            statuses.append(status)
        origin = min(r["arrived"] for r in records)
        end = max(r["finished"] for r in records)
        # Verify admission invariants using only the public status snapshots.
        intervals = []
        for status in statuses:
            start, finish = status.get("started_ms"), status.get("finished_ms")
            if start is None or finish is None:
                raise RuntimeError(f"missing terminal job timestamps: {status!r}")
            resources = status["request"]["resources"]
            ram_slots = next(r["access"]["Shared"]["slots"] for r in resources
                             if r["key"]["name"] == "bench-ram")
            exclusive = {json.dumps(r["key"], sort_keys=True) for r in resources
                         if r["access"] == "Exclusive"}
            intervals.append((start, finish, ram_slots, exclusive))
        for i, (start, finish, _, keys) in enumerate(intervals):
            for other_start, other_finish, _, other_keys in intervals[i+1:]:
                if start < other_finish and other_start < finish and keys & other_keys:
                    raise RuntimeError("conflicting jobs overlapped according to CLI status")
        points = sorted([(start, slots) for start, _, slots, _ in intervals] +
                        [(finish, -slots) for _, finish, slots, _ in intervals],
                        key=lambda point: (point[0], point[1]))
        used = peak_slots = 0
        for _, delta in points:
            used += delta
            peak_slots = max(peak_slots, used)
        if peak_slots > 20:
            raise RuntimeError(f"lane admitted {peak_slots} RAM slots, cap is 20")
        waits = []
        for record in records:
            match = next(s for s in statuses if s["ticket"]["id"] == record["id"])
            started = match.get("started_ms")
            created = match.get("created_ms")
            if started is None or created is None:
                raise RuntimeError(f"missing job timing in --json status: {match!r}")
            first_arrival = min(r["arrived"] for r in records if r["id"] == record["id"])
            if record["arrived"] > first_arrival:
                # A join to an already-running build waits for its result, not
                # for a second launch or a fictitious queue position.
                waits.append(max(0, record["finished"] - record["arrived"]))
            else:
                waits.append(max(0, (started - created) / 1000))
        per_agent = [sum(r["finished"] - r["arrived"] for r in records if r["agent"] == i)
                     for i in range(agents)]
        throughput = [sum(t.duration for t in tasks[i]) / max(0.001, per_agent[i])
                      for i in range(agents)]
        fairness = (sum(throughput) ** 2 / (agents * sum(t*t for t in throughput))) if sum(throughput) else 1.
        return {"mode": "real-cli", "agents": agents, "requests": len(records),
                "launches": len(ids), "coalesced": len(records)-len(ids),
                "makespan_seconds": round(end-origin, 3),
                "agent_wait_hours": round(sum(waits)/3600, 5),
                "fairness_jain": round(fairness, 4), "oom": 0, "failures": 0,
                "peak_reserved_gib": peak_slots,
                "cpu_utilization_8_cores": round(sum(float(s.get("cpu_seconds") or 0)
                                                      for s in statuses) / ((end-origin)*8), 4),
                "lane_state": "isolated temporary directory removed after successful run"}



def probe_disk_budget(binary: Path) -> dict:
    """An impossible disk budget must fail without ever launching a worker."""
    binary = binary.resolve(strict=True)
    with isolated_root() as root:
        lane = root / "lanes"
        info = os.statvfs(root)
        impossible = info.f_bavail * info.f_frsize + 1_000_000_000
        spec = {"fingerprint": "bench-impossible-disk", "lease": {
            "resources": [{"key": {"scope": "Host", "name": "bench-disk"},
                           "access": {"Shared": {"slots": 1}}}],
            "holder": {"participant_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
                       "host_pid": None, "purpose": "disk admission probe"},
            "queue_timeout_ms": 500},
            "argv": [sys.executable, str(Path(__file__).with_name("gamedev_benchmark.py")),
                     "--worker", "0.01", "1", ".1"],
            "cwd": str(root), "env": [], "memory_max_bytes": 16 * 1024 * 1024,
            "admission": {"min_available_ram_bytes": 0, "reserve_ram_bytes": 0,
                          "min_free_disk_bytes": impossible, "reserve_disk_bytes": 0,
                          "disk_path": str(root)},
            "pre_hook": None, "post_hook": None, "timeout_ms": 2000,
            "stall_timeout_ms": None, "coalesce": False}
        submitted = cli(binary, lane, "job", "submit", "--spec", "-", input_data=spec)
        assert isinstance(submitted, dict)
        job_id = submitted["job_id"]
        cmd = [str(binary), "lane", "--state-dir", str(lane), "--json", "job", "wait", job_id]
        waiting = subprocess.run(cmd, capture_output=True, text=True, timeout=10,
                                 env={**os.environ, "BORG_LANE_SCOPE": "0"})
        status = cli(binary, lane, "job", "status", job_id)
        assert isinstance(status, dict)
        reason = status.get("wait_reason") or status.get("evidence") or ""
        if waiting.returncode == 0 or status.get("started_ms") is not None or "disk" not in reason:
            raise RuntimeError(f"disk admission failure not enforced: wait={waiting.returncode}, status={status!r}")
        return {"mode": "disk-budget-cli", "wait_exit": waiting.returncode,
                "started": False, "reason": reason}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--borg", required=True, type=Path, help="built Borg CLI binary with lane job subcommands")
    ap.add_argument("--agents", type=int, default=3)
    ap.add_argument("--jobs", type=int, default=3)
    ap.add_argument("--seed", type=int, default=23)
    ap.add_argument("--scale", type=float, default=200)
    ap.add_argument("--check-budget", action="store_true", help="test impossible disk budget via CLI")
    args = ap.parse_args()
    if not (1 <= args.agents <= 8 and 1 <= args.jobs <= 20 and args.scale >= 1):
        ap.error("real mode requires agents=1..8, jobs=1..20, scale>=1")
    result = probe_disk_budget(args.borg) if args.check_budget else run(args.borg, args.agents, args.jobs, args.scale, args.seed)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
