#!/usr/bin/env python3
"""Minimal loopback backend for the Borg service CLI regression harness."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
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


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
