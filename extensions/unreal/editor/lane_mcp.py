"""Private MCP client used only by the core service's graceful-stop hook.

Not an interactive editor API: direct clients can bypass core ownership on
backend ports. Public MCP access remains disabled until fencing is integrated.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

PROTOCOL_VERSION = '2025-11-25'
EDITOR_APP = 'EditorToolset.EditorAppToolset'
LANE_TOOLS = 'unreal_lane_tools.UnrealLaneTools'


class Client:
    def __init__(self, url: str, timeout: float = 10) -> None:
        self.url, self.timeout, self.session, self.request_id = url, timeout, None, 0

    def post(self, payload: dict[str, Any]) -> dict:
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream'}
        if self.session:
            headers.update({'Mcp-Session-Id': self.session, 'Mcp-Protocol-Version': PROTOCOL_VERSION})
        req = urllib.request.Request(self.url, json.dumps(payload).encode(), headers)
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            if payload['method'] == 'initialize':
                self.session = response.headers.get('Mcp-Session-Id')
            raw = response.read()
        return json.loads(raw, strict=False) if raw else {}

    def initialize(self) -> None:
        self.post({'jsonrpc': '2.0', 'id': 0, 'method': 'initialize',
                   'params': {'protocolVersion': PROTOCOL_VERSION, 'capabilities': {},
                              'clientInfo': {'name': 'borg-unreal-stop', 'version': '1'}}})
        self.post({'jsonrpc': '2.0', 'method': 'notifications/initialized'})

    def call(self, toolset: str, name: str, args: dict | None = None) -> Any:
        self.request_id += 1
        result = self.post({'jsonrpc': '2.0', 'id': self.request_id, 'method': 'tools/call',
                            'params': {'name': 'call_tool',
                                       'arguments': {'toolset_name': toolset, 'tool_name': name,
                                                     'arguments': args or {}}}})
        if 'error' in result:
            raise RuntimeError(f'editor tool failed: {result["error"]}')
        payload = result.get('result', {})
        text = ''.join(item.get('text', '') for item in payload.get('content', [])
                       if item.get('type') == 'text')
        if payload.get('isError'):
            raise RuntimeError(f'editor tool failed: {text}')
        try:
            value = json.loads(text, strict=False)
            if isinstance(value, dict) and set(value) == {'returnValue'}:
                value = value['returnValue']
            if isinstance(value, str):
                try:
                    value = json.loads(value, strict=False)
                except json.JSONDecodeError:
                    pass
            return value
        except json.JSONDecodeError:
            return text

    def close(self) -> None:
        if self.session:
            req = urllib.request.Request(self.url, method='DELETE', headers={
                'Mcp-Session-Id': self.session, 'Mcp-Protocol-Version': PROTOCOL_VERSION})
            try:
                urllib.request.urlopen(req, timeout=5).close()
            except (urllib.error.URLError, TimeoutError):
                pass
            self.session = None
