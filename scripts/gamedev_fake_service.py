#!/usr/bin/env python3
"""Minimal loopback backend for the Borg service CLI regression harness."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return
        body = ("ok\n" if self.path == "/health" else f"pid={os.getpid()}\n").encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


def start_detached_child(marker_dir: Path) -> None:
    marker_dir.mkdir(parents=True, exist_ok=True)
    child = subprocess.Popen([sys.executable, __file__, "--detached-child"],
                             start_new_session=True, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        # The worker checks both process identity and its backend-generation cgroup.
        stat = Path(f"/proc/{child.pid}/stat").read_text().split()
        cgroup = Path("/proc/self/cgroup").read_text().strip().splitlines()[0].split("::", 1)[-1]
        record = {"pid": child.pid, "start_ticks": stat[21], "cgroup": cgroup,
                  "backend_pid": os.getpid()}
        (marker_dir / f"{os.getpid()}.json").write_text(json.dumps(record))
    except BaseException:
        child.terminate()  # Only this backend's newly-created child.
        child.wait()
        raise


if __name__ == "__main__":
    if sys.argv[1] == "--detached-child":
        signal.pause()  # No polling or CPU burn; survives leader exit without cgroup kill.
    else:
        if os.environ.get("BENCH_CHILD_MARKER_DIR"):
            start_detached_child(Path(os.environ["BENCH_CHILD_MARKER_DIR"]))
        ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
