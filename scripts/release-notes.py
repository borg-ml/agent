#!/usr/bin/env python3
"""Validate and promote the curated changelog at a release boundary."""

import re
import sys
from datetime import datetime, timezone
from pathlib import Path


def fail(message: str) -> None:
    raise SystemExit(f"release notes: {message}")


def sections(lines: list[str]) -> list[tuple[int, str]]:
    return [(index, line.strip()) for index, line in enumerate(lines) if line.startswith("## ")]


def find_section(lines: list[str], heading: str) -> tuple[int, int]:
    headings = sections(lines)
    matches = [index for index, (_, title) in enumerate(headings) if title == heading]
    if len(matches) != 1:
        fail(f"expected one '{heading}' section in CHANGELOG.md")
    position = matches[0]
    start = headings[position][0]
    end = headings[position + 1][0] if position + 1 < len(headings) else len(lines)
    if not re.search(r"(?m)^\s*[-*] ", "".join(lines[start + 1 : end])):
        fail(f"'{heading}' has no change entries")
    return start, end


def unreleased(lines: list[str], version: str) -> tuple[int, int]:
    heading = f"## Unreleased (since {version})"
    headings = sections(lines)
    if not headings or headings[0][1] != heading:
        fail(f"CHANGELOG.md must start its release sections with '{heading}'")
    return find_section(lines, heading)


def released(lines: list[str], version: str) -> tuple[int, int]:
    headings = sections(lines)
    if len(headings) < 2 or headings[0][1] != f"## Unreleased (since {version})":
        fail(f"CHANGELOG.md must open with Unreleased since {version}")
    heading = headings[1][1]
    if not re.fullmatch(rf"## {re.escape(version)} \(\d{{4}}-\d{{2}}-\d{{2}}\)", heading):
        fail(f"expected a dated release section for {version} below Unreleased")
    return find_section(lines, heading)


def main() -> None:
    if len(sys.argv) not in (4, 5):
        fail("usage: release-notes.py check|stamp|verify|extract VERSION [TARGET] CHANGELOG.md")
    command = sys.argv[1]
    if command == "stamp":
        if len(sys.argv) != 5:
            fail("stamp requires CURRENT TARGET CHANGELOG.md")
        current, target, filename = sys.argv[2:]
    elif command in ("check", "verify", "extract") and len(sys.argv) == 4:
        current, filename = sys.argv[2:]
    else:
        fail(f"unknown command or arguments: {command}")

    path = Path(filename)
    try:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    except OSError as error:
        fail(f"could not read {filename}: {error}")

    if command == "check":
        unreleased(lines, current)
    elif command == "stamp":
        start, _ = unreleased(lines, current)
        if any(title.startswith(f"## {target} (") for _, title in sections(lines)):
            fail(f"release section {target} already exists")
        date = datetime.now(timezone.utc).date().isoformat()
        lines[start] = f"## Unreleased (since {target})\n\n## {target} ({date})\n"
        path.write_text("".join(lines), encoding="utf-8")
    elif command == "verify":
        released(lines, current)
    else:
        start, end = released(lines, current)
        body = "".join(lines[start + 1 : end]).strip()
        print(f"# Borg Agent {current}\n\n{body}")


if __name__ == "__main__":
    main()
