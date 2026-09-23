#!/usr/bin/env python3
"""Deterministic game-dev lane contention model and bounded process replay.

No Unreal installation or database required. Durations are simulated seconds;
--materialize replays the chosen schedule with capped fake CPU/RAM workers.
"""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass, field
import heapq
import json
import random
import subprocess
import sys
import time

# Baselines: Abundance docs/BUILD_LANE.md and AGENT_PRODUCTIVITY_2026-09-23.md.
# (seconds, GiB reserved, CPU cores); exclusive import yields the editor.
KINDS = {
    "leaf": (10, 2, 2), "header": (245, 8, 8), "clean": (340, 12, 8),
    "run": (90, 6, 3), "capture": (2, 2, 1), "editor": (4, 2, 1),
    "cargo": (25, 3, 3), "ctest": (7, 2, 2), "import": (75, 6, 3),
}
WEIGHTS = ("leaf",) * 24 + ("header",) * 6 + ("clean",) * 2 + ("run",) * 12 + ("capture",) * 18 + ("editor",) * 6 + ("cargo",) * 12 + ("ctest",) * 14 + ("import",) * 6
BUILD = {"leaf", "header", "clean", "cargo"}
EDITOR = {"capture", "editor"}


@dataclass(frozen=True)
class Request:
    agent: int
    index: int
    kind: str
    key: str
    fingerprint: str
    duration: float
    ram: int
    cpu: int


@dataclass
class Work:
    req: Request
    arrived: float
    members: list[tuple[Request, float]] = field(default_factory=list)
    started: float | None = None
    finish: float = 0


def workloads(agents: int, jobs: int, seed: int) -> list[list[Request]]:
    rng = random.Random(seed)
    result = []
    for agent in range(agents):
        tasks = []
        for i in range(jobs):
            kind = "leaf" if i == 0 else rng.choice(WEIGHTS)
            duration, ram, cpu = KINDS[kind]
            if kind == "header":
                duration = rng.uniform(190, 300)
            elif kind == "run":
                duration = rng.uniform(30, 220)
                ram = rng.randint(4, 8)
            elif kind == "leaf":
                duration = 10 if i == 0 else rng.uniform(9.8, 10.4)
            # First request in each 3-agent group intentionally has same source
            # fingerprint; later edits are distinct, so joins are safe only there.
            tree = agent // 3
            key = f"tree-{tree}" if kind in BUILD | {"run", "import"} else "editor"
            fingerprint = f"shared-{tree}" if i == 0 else f"{agent}-{i}"
            tasks.append(Request(agent, i, kind, key, fingerprint, duration, ram, cpu))
        result.append(tasks)
    return result


def simulate(tasks: list[list[Request]], policy: str, ram_limit: int = 20, cores: int = 24,
             poll_seconds: int = 10, scale: float = 10, coalesce_running: bool = False) -> dict:
    """Event-driven: agents issue their next request only when the previous completes."""
    if ram_limit < max(r.ram for row in tasks for r in row) or cores < 1:
        raise ValueError("budget cannot admit a single job")
    if policy not in ("naive", "fifo", "borg"):
        raise ValueError(policy)
    if scale <= 0 or poll_seconds <= 0:
        raise ValueError("scale and poll_seconds must be positive")
    agents = len(tasks)
    events: list[tuple[float, int, str, object]] = []
    order = 0
    def event(at: float, kind: str, data: object) -> None:
        nonlocal order
        heapq.heappush(events, (at, order, kind, data))
        order += 1

    for agent in range(agents):
        if tasks[agent]:
            event(agent % 3 * 0.2, "arrive", tasks[agent][0])
    pending: deque[Work] = deque()
    running: list[Work] = []
    finished_at = [0.0] * agents
    wait = [0.0] * agents
    solo = [sum(r.duration for r in row) for row in tasks]
    busy_cpu_seconds = 0.0
    last = 0.0
    oom = failures = joins = launches = refused = yields = 0
    spans: list[dict] = []
    editor_warm = False

    def effective(req: Request) -> float:
        if policy != "borg":
            if req.kind == "leaf":
                return 17.5  # pre-lane symbols on critical path
            if req.kind == "capture":
                return 38  # cold headless launch
            if req.kind == "editor":
                return 45  # cold editor session
        if req.kind in EDITOR and policy == "borg" and not editor_warm:
            return req.duration + 12  # one warm-up after exclusive yield
        return req.duration

    def can_run(w: Work) -> bool:
        if policy != "borg":
            return not running
        used = sum(x.req.ram for x in running)
        if used + w.req.ram > ram_limit:
            return False
        # Same-tree writes/builds and one serialized editor service.
        if any(x.req.key == w.req.key for x in running):
            return False
        if w.req.kind == "import" and any(x.req.kind in EDITOR and x.req.key == "editor" for x in running):
            return False
        if w.req.kind in EDITOR and any(x.req.kind == "import" for x in running):
            return False
        return True

    def dispatch(now: float) -> None:
        nonlocal editor_warm, launches, yields, oom
        for w in list(pending):
            if not can_run(w):
                if policy != "borg":
                    break
                continue
            if policy == "borg" and any(other.req.key == w.req.key for other in list(pending)[:list(pending).index(w)]):
                continue
            pending.remove(w)
            if w.req.kind == "import" and policy == "borg" and editor_warm:
                editor_warm = False
                yields += 1
            if w.req.kind in EDITOR and policy == "borg":
                # Warm state persists until yielded by an exclusive import.
                d = effective(w.req)
                editor_warm = True
            else:
                d = effective(w.req)
            w.started = now
            w.finish = now + d
            running.append(w)
            launches += 1
            if sum(x.req.ram for x in running) > ram_limit:
                oom += 1
            event(w.finish, "finish", w)

    arrival_time: dict[tuple[int, int], float] = {}
    while events:
        now, _, kind, data = heapq.heappop(events)
        busy_cpu_seconds += (now - last) * min(cores, sum(x.req.cpu for x in running))
        last = now
        if kind in {"arrive", "retry"}:
            req = data
            assert isinstance(req, Request)
            arrival_time.setdefault((req.agent, req.index), now)
            if kind == "retry" and (running or pending):
                refused += 1
                event(now + poll_seconds, "retry", req)
                continue
            # Join pending or running identical builds only if the same source
            # fingerprint; a later edit cannot join the in-flight build.
            match = next((w for w in [*pending, *(running if coalesce_running else [])]
                          if policy == "borg" and req.kind in BUILD and w.req.kind == req.kind
                          and w.req.key == req.key and w.req.fingerprint == req.fingerprint), None)
            if match:
                match.members.append((req, arrival_time[(req.agent, req.index)]))
                joins += 1
                continue
            if policy == "naive" and (running or pending):
                refused += 1
                event(now + poll_seconds, "retry", req)
                continue
            arrived = arrival_time[(req.agent, req.index)]
            pending.append(Work(req, arrived, [(req, arrived)]))
        elif kind == "finish":
            w = data
            assert isinstance(w, Work)
            running.remove(w)
            assert w.started is not None
            for member, arrival in w.members:
                wait[member.agent] += (w.started - arrival if member == w.req else now - arrival) / scale
                finished_at[member.agent] = now
                if member.index + 1 < len(tasks[member.agent]):
                    event(now, "arrive", tasks[member.agent][member.index + 1])
            spans.append({"kind": w.req.kind, "key": w.req.key, "start": round(w.started / scale, 4),
                          "end": round(now / scale, 4), "ram_gib": w.req.ram, "cpu": w.req.cpu})
        else:
            raise AssertionError(kind)
        dispatch(now)
    makespan = max(finished_at, default=0) / scale
    # Jain's index on per-agent productive throughput: solo work divided by
    # elapsed wall time since first arrival, not queue wait alone (zero-safe).
    throughput = [work / max(1e-9, end - (i % 3 * .2))
                  for i, (work, end) in enumerate(zip(solo, finished_at)) if work]
    fairness = sum(throughput) ** 2 / (len(throughput) * sum(x*x for x in throughput)) if throughput else 1.
    return {"policy": policy, "agents": agents, "requests": sum(map(len, tasks)),
            "scale": scale, "agent_wait_hours": round(sum(wait) / 3600, 5),
            "unscaled_wait_hours": round(sum(wait) * scale / 3600, 5),
            "makespan_seconds": round(makespan, 3),
            "cpu_utilization": round(busy_cpu_seconds / (max(finished_at, default=1) * cores), 4),
            "oom": oom, "failures": failures, "fairness_jain": round(fairness, 4),
            "launches": launches, "coalesced": joins, "refusals": refused,
            "service_yields": yields, "coalesce_running_verified": coalesce_running,
            "spans": sorted(spans, key=lambda s: (s["start"], s["key"]))}


def worker(seconds: float, ram_mib: int, cpu_fraction: float) -> None:
    # Memory is touched to ensure RSS, but always bounded by parent caps.
    memory = bytearray(ram_mib * 1024 * 1024)
    for i in range(0, len(memory), 4096):
        memory[i] = 1
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        start = time.monotonic()
        while time.monotonic() < min(end, start + .05 * cpu_fraction):
            _ = sum(range(30))
        time.sleep(min(.05 * (1-cpu_fraction), max(0, end-time.monotonic())))


def materialize(spans: list[dict], ram_mib_per_gib: int = 32) -> dict:
    """Replay concurrent fake processes; cap 8 cores and 8 GiB hard."""
    if not 1 <= ram_mib_per_gib <= 64:
        raise ValueError("ram_mib_per_gib must be 1..64")
    processes: list[tuple[subprocess.Popen, int]] = []
    all_processes: list[subprocess.Popen] = []
    origin = time.monotonic()
    peak_ram = 0
    try:
        for span in spans:
            wait = origin + span["start"] - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            processes = [(p, mib) for p, mib in processes if p.poll() is None]
            ram = span["ram_gib"] * ram_mib_per_gib
            live_ram = sum(mib for _, mib in processes)
            peak_ram = max(peak_ram, live_ram + ram)
            if live_ram + ram > 8192 or len(processes) >= 8:
                raise RuntimeError("materialized replay exceeded 8 GiB or 8 workers")
            p = subprocess.Popen([sys.executable, __file__, "--worker", str(span["end"]-span["start"]),
                                  str(ram), ".25"], stdout=subprocess.DEVNULL)
            processes.append((p, ram))
            all_processes.append(p)
        codes = [p.wait() for p in all_processes]
    finally:
        for p in all_processes:
            if p.poll() is None:
                p.terminate()
                p.wait()
    return {"wall_seconds": round(time.monotonic()-origin, 3),
            "peak_fake_ram_mib": peak_ram, "process_failures": sum(c != 0 for c in codes)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agents", type=int, default=6)
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--seed", type=int, default=23)
    parser.add_argument("--scale", type=float, default=10)
    parser.add_argument("--ram-gib", type=int, default=20)
    parser.add_argument("--cores", type=int, default=24)
    parser.add_argument("--policy", choices=("all", "naive", "fifo", "borg"), default="all")
    parser.add_argument("--materialize", action="store_true")
    parser.add_argument("--coalesce-running", action="store_true",
                        help="hypothetical: join running build only when input revision is independently verified")
    parser.add_argument("--ram-mib-per-gib", type=int, default=32)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--worker", nargs=3, metavar=("SECONDS", "RAM_MIB", "CPU_FRACTION"))
    args = parser.parse_args()
    if args.worker:
        worker(float(args.worker[0]), int(args.worker[1]), float(args.worker[2]))
        return
    if not (1 <= args.agents <= 64 and 1 <= args.jobs <= 100 and args.cores > 0):
        parser.error("agents=1..64, jobs=1..100, cores>0 required")
    if args.materialize and (args.agents > 8 or args.ram_gib > 20 or args.cores > 8):
        parser.error("materialized runs require <=8 agents, <=20 modeled GiB and <=8 cores")
    tasks = workloads(args.agents, args.jobs, args.seed)
    policies = ("naive", "fifo", "borg") if args.policy == "all" else (args.policy,)
    results = [simulate(tasks, p, args.ram_gib, args.cores, scale=args.scale,
                        coalesce_running=args.coalesce_running) for p in policies]
    if args.materialize:
        for result in results:
            result["replay"] = materialize(result["spans"], args.ram_mib_per_gib)
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print("policy  wait(h)  makespan(s)  cpu util  oom fail  fairness  launches joins polls yields")
        for r in results:
            print(f"{r['policy']:6} {r['agent_wait_hours']:8.3f} {r['makespan_seconds']:12.1f} "
                  f"{r['cpu_utilization']:9.1%} {r['oom']:4} {r['failures']:4} "
                  f"{r['fairness_jain']:9.3f} {r['launches']:9} {r['coalesced']:5} "
                  f"{r['refusals']:5} {r['service_yields']:6}")
            if "replay" in r:
                print("  replay:", r["replay"])


if __name__ == "__main__":
    main()
