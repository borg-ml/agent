#!/usr/bin/env python3
"""Best-effort graceful quit for Borg's tracked service supervisor."""
import argparse
import sys

from lane_mcp import Client, EDITOR_APP, LANE_TOOLS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()
    client = Client(f'http://127.0.0.1:{args.port}/mcp', timeout=10)
    try:
        client.initialize()
        if client.call(EDITOR_APP, 'IsPIERunning'):
            client.call(EDITOR_APP, 'StopPIE')
        client.call(LANE_TOOLS, 'exec_console', {'command': 'QUIT_EDITOR', 'owner': 'lane-supervisor'})
    finally:
        client.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
