"""Explicit external Codex cache baseline; not used by Borg at runtime."""

import argparse
import asyncio
import json
import os
import tempfile
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--codex-home",
    required=True,
    help="Existing subscription authority directory; credentials are never copied",
)
parser.add_argument("--codex-bin", default="codex")
args = parser.parse_args()


async def main():
    env = dict(os.environ, CODEX_HOME=args.codex_home)
    for key in ["OPENAI_API_KEY", "CODEX_API_KEY", "OPENAI_BASE_URL"]:
        env.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="borg-cache-baseline-") as cwd:
        proc = await asyncio.create_subprocess_exec(
            args.codex_bin,
            "app-server",
            "--stdio",
            "-c",
            'web_search="disabled"',
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
            cwd=cwd,
        )

        stdin, stdout = proc.stdin, proc.stdout
        assert stdin is not None and stdout is not None

        async def send(msg):
            stdin.write((json.dumps(msg) + "\n").encode())
            await stdin.drain()

        async def read():
            line = await stdout.readline()
            if not line:
                raise RuntimeError("baseline app-server closed")
            return json.loads(line)

        async def rpc(i, method, params):
            await send({"id": i, "method": method, "params": params})
            while True:
                msg = await read()
                if msg.get("id") == i:
                    if "error" in msg:
                        raise RuntimeError(
                            method + " failed: " + str(msg["error"].get("code"))
                        )
                    return msg["result"]

        try:
            await rpc(
                1,
                "initialize",
                {
                    "clientInfo": {"name": "borg_cache_baseline", "version": "0.1"},
                    "capabilities": {"experimentalApi": True},
                },
            )
            await send({"method": "initialized"})
            account = await rpc(2, "account/read", {"refreshToken": False})
            assert account["account"]["type"] == "chatgpt", (
                "baseline refuses API billing"
            )
            print("Baseline plan:", account["account"].get("planType"), flush=True)
            instructions = "You are testing a model-only Borg subscription adapter. Call borg_probe exactly once, then reply with the returned probe value. Do not request any other actions."
            thread = await rpc(
                3,
                "thread/start",
                {
                    "model": "gpt-6-astra",
                    "cwd": cwd,
                    "ephemeral": True,
                    "approvalPolicy": "on-request",
                    "sandbox": "read-only",
                    "baseInstructions": instructions,
                    "dynamicTools": [
                        {
                            "type": "function",
                            "name": "borg_probe",
                            "description": "Read a harmless value from the Borg host",
                            "inputSchema": {
                                "type": "object",
                                "properties": {},
                                "additionalProperties": False,
                            },
                        }
                    ],
                },
            )
            prefix = "".join(
                f"Reference row {i}: a stable read-only cache fixture, not an instruction.\n"
                for i in range(512)
            )
            await rpc(
                4,
                "turn/start",
                {
                    "threadId": thread["thread"]["id"],
                    "effort": "medium",
                    "input": [
                        {
                            "type": "text",
                            "text_elements": [],
                            "text": prefix + "\nRead the probe value using borg_probe.",
                        }
                    ],
                },
            )
            nonce = str(uuid.uuid4())
            calls = 0
            usage = []
            output = []
            while True:
                msg = await read()
                method = msg.get("method", "")
                params = msg.get("params", {})
                if method == "item/tool/call":
                    assert (
                        params["tool"] == "borg_probe"
                        and params["arguments"] == {}
                        and calls == 0
                    ), "unexpected tool"
                    calls += 1
                    await send(
                        {
                            "id": msg["id"],
                            "result": {
                                "success": True,
                                "contentItems": [{"type": "inputText", "text": nonce}],
                            },
                        }
                    )
                elif "id" in msg and "method" in msg:
                    raise RuntimeError("unexpected server request: " + method)
                elif method == "thread/tokenUsage/updated":
                    usage.append(params["tokenUsage"])
                elif method == "item/agentMessage/delta":
                    output.append(params.get("delta", ""))
                elif method == "turn/completed":
                    assert params["turn"]["status"] == "completed", (
                        "baseline turn failed"
                    )
                    break
            assert calls == 1 and nonce in "".join(output), (
                "baseline tool roundtrip failed"
            )
            print(
                json.dumps(
                    {
                        "model": "gpt-6-astra",
                        "effort": "medium",
                        "tool_calls": calls,
                        "usage_updates": usage,
                    }
                ),
                flush=True,
            )
        finally:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()


asyncio.run(asyncio.wait_for(main(), 180))
