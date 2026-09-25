"""Borg's capabilities as Python functions, for code run through `exec`.

    import borg
    borg.send_message(target="/root/worker", message=report)
    plan = borg.get_plan()
    borg.call("create_goal", objective="...")

Each call goes to the running session over its tool socket, exactly like
`borg call NAME JSON`, and shows in the transcript as a step of the command
that made it. Results are decoded JSON; failures raise `BorgError`.
"""

import json
import os
import socket

__all__ = ["BorgError", "call", "search", "tools"]


class BorgError(Exception):
    """A Borg capability refused or failed the call."""


def _request(name, arguments):
    request = {
        "name": name,
        "arguments": arguments,
        "workflow_approved": os.environ.get("BORG_AGENT_TOOL_APPROVED") == "1",
    }
    parent = os.environ.get("BORG_TOOL_CALL_ID")
    if parent:
        request["parent"] = parent
    path = os.environ.get("BORG_AGENT_TOOL_SOCKET")
    if path:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        address = path
    else:
        address = os.environ.get("BORG_AGENT_TOOL_TCP")
        if not address:
            raise BorgError("not running inside a Borg session: BORG_AGENT_TOOL_SOCKET is unset")
        host, _, port = address.rpartition(":")
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = (host, int(port))
        request["token"] = os.environ.get("BORG_AGENT_TOOL_TOKEN", "")
    with connection:
        connection.connect(address)
        connection.sendall(json.dumps(request).encode() + b"\n")
        reader = connection.makefile("rb")
        line = reader.readline()
    if not line:
        raise BorgError(f"Borg closed the connection without answering {name}")
    response = json.loads(line)
    if "error" in response:
        raise BorgError(response["error"])
    return response.get("result")


def call(name, arguments=None, /, **fields):
    """Call the Borg capability `name` with a dict and/or keyword fields."""
    return _request(name, {**(arguments or {}), **fields})


def tools():
    """Every capability this session offers, with its input schema."""
    return _request("__borg_tools", {})


def search(query, limit=10):
    """Capabilities ranked for `query`, each with a compact signature."""
    return _request("__borg_tools", {"query": query, "limit": limit})


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    return lambda arguments=None, /, **fields: call(name, arguments, **fields)
