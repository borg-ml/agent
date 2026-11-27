"""Borg's tools as Python functions, for code run through `exec`.

Import this shipped library from a command. In `runtime_exec`, use its
preloaded `borg` object instead; both surfaces reach the same host tools.

    import borg
    borg.send_message(target="/root/worker", message=report)
    plan = borg.get_plan()
    borg.tools("message")  # find capabilities
    borg.call("create_goal", objective="...")

Catalog capabilities, native tools, and configured MCP/extension tools are
reachable as attributes, or through `borg.call(name, arguments)` when the
name is computed. `borg.tools()` discovers all available names and schemas.
Calls retain host permission, approval, and human-confirmation checks.

Each call goes to the running session over its tool socket, exactly like
`borg call NAME JSON`, and shows in the transcript as a step of the command
that made it. Results are decoded JSON; failures raise `BorgError`.
"""

import json
import os
import socket
import threading

__all__ = ["BorgError", "call", "tool", "tools"]

_JSON_ENCODE = json.JSONEncoder(separators=(",", ":")).encode
_THREAD_CONN = threading.local()


class BorgError(Exception):
    """A Borg capability refused or failed the call."""


def _endpoint():
    path = os.environ.get("BORG_AGENT_TOOL_SOCKET")
    if path:
        return ("unix", path, None)
    address = os.environ.get("BORG_AGENT_TOOL_TCP")
    if not address:
        raise BorgError("not running inside a Borg session: BORG_AGENT_TOOL_SOCKET is unset")
    host, _, port = address.rpartition(":")
    return ("tcp", (host, int(port)), os.environ.get("BORG_AGENT_TOOL_TOKEN", ""))


def _close_cached():
    cached = getattr(_THREAD_CONN, "state", None)
    _THREAD_CONN.state = None
    if cached is not None:
        _, sock, reader = cached
        try:
            reader.close()
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass


def _socket_alive(sock):
    # Probe without consuming data so an idle server-closed socket (FIN) is
    # discarded before sending a request, avoiding any duplicate side effects.
    try:
        sock.setblocking(False)
        try:
            peeked = sock.recv(1, socket.MSG_PEEK)
            return len(peeked) > 0
        except BlockingIOError:
            return True
        except OSError:
            return False
        finally:
            sock.setblocking(True)
    except OSError:
        return False


def _acquire_connection(key):
    cached = getattr(_THREAD_CONN, "state", None)
    if cached is not None:
        cached_key, sock, reader = cached
        if cached_key == key and _socket_alive(sock):
            return sock, reader, True
        _close_cached()
    family = socket.AF_UNIX if key[0] == "unix" else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        if key[0] == "tcp":
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
        sock.connect(key[1])
        reader = sock.makefile("rb")
    except Exception:
        sock.close()
        raise
    _THREAD_CONN.state = (key, sock, reader)
    return sock, reader, False


def _request_unchecked(name, arguments):
    key = _endpoint()
    request = {
        "name": name,
        "arguments": arguments,
        "workflow_approved": os.environ.get("BORG_AGENT_TOOL_APPROVED") == "1",
    }
    parent = os.environ.get("BORG_TOOL_CALL_ID")
    if parent:
        request["parent"] = parent
    if key[2] is not None:
        request["token"] = key[2]
    payload = _JSON_ENCODE(request).encode("utf-8") + b"\n"
    for attempt in range(2):
        sock, reader, reused = _acquire_connection(key)
        try:
            sock.sendall(payload)
            line = reader.readline()
        except (BrokenPipeError, ConnectionResetError):
            _close_cached()
            if reused and attempt == 0:
                continue
            raise
        except OSError:
            _close_cached()
            raise
        if line:
            break
        _close_cached()
        if reused and attempt == 0:
            continue
        raise BorgError(f"Borg closed the connection without answering {name}")
    try:
        response = json.loads(line)
    except ValueError:
        _close_cached()
        raise
    if not isinstance(response, dict):
        _close_cached()
        raise BorgError(f"Borg returned an invalid response object for {name}")
    if "error" in response:
        raise BorgError(response["error"])
    return response.get("result")


def _request(name, arguments):
    try:
        return _request_unchecked(name, arguments)
    except (OSError, ValueError, TypeError) as error:
        raise BorgError(f"Borg tool {name!r} failed: {error}") from error


def call(name, arguments=None, /, **fields):
    """Call any host tool with a dict and/or keyword fields."""
    if arguments is not None and not isinstance(arguments, dict):
        raise TypeError("Borg tool arguments must be an object")
    return _request(name, {**({} if arguments is None else arguments), **fields})


tool = call


def tools(query=None, limit=10):
    """All available catalog/native/MCP tools, or ranked matches with schemas."""
    arguments: dict = {"workspace_tools": True}
    if query is not None:
        arguments.update(query=query, limit=limit)
    return _request("__borg_tools", arguments)


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    return lambda arguments=None, /, **fields: call(name, arguments, **fields)
