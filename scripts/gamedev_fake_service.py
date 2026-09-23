#!/usr/bin/env python3
"""Minimal loopback backend for the Borg service CLI regression harness."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return
        marker = os.environ.get("BENCH_HEALTH_FAIL_FILE")
        if self.path == "/health" and marker and Path(marker).exists():
            self.send_error(503, "fake backend health disabled")
            return
        body = ("ok\n" if self.path == "/health" else f"pid={os.getpid()}\n").encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        # Opt-in MCP initialize shaped like the Unreal editor's: pretty JSON,
        # a session header, and a connection left open (Content-Length only).
        if self.path != "/mcp" or not os.environ.get("BENCH_MCP_PRETTY"):
            self.send_error(404)
            return
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        session = uuid.uuid4().hex
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"),
                           "result": {"protocolVersion": "2025-11-25",
                                      "capabilities": {"resources": {}, "tools": {"listChanged": True}},
                                      "serverInfo": {"name": "", "title": "", "version": ""}}},
                          indent="\t").encode()
        self.log_session("open", session)
        self.protocol_version = "HTTP/1.1"
        self.send_response(200)
        self.send_header("content-type", "application/json;charset=utf-8")
        self.send_header("Mcp-Session-Id", session)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = False

    def do_DELETE(self) -> None:
        session = self.headers.get("Mcp-Session-Id")
        if self.path != "/mcp" or not session or not os.environ.get("BENCH_MCP_PRETTY"):
            self.send_error(404)
            return
        self.log_session("delete", session)
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    @staticmethod
    def log_session(event: str, session: str) -> None:
        log = os.environ.get("BENCH_MCP_SESSION_LOG")
        if log:
            fd = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                os.write(fd, f"{event} {session}\n".encode())
            finally:
                os.close(fd)

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
        delay = os.environ.get("BENCH_START_DELAY_FILE")
        if delay and Path(delay).exists():
            # Like an editor still loading: the port is not listening yet.
            time.sleep(float(Path(delay).read_text() or 0))
        ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
