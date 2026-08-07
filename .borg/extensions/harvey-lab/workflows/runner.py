#!/usr/bin/env python3
"""Run the official Harvey LAB CLI and create Borg-owned receipt inputs.

Harvey LAB owns its result directories. This adapter only creates small,
project-local summaries and copies the official metrics/scores JSON when it is
available. Borg hashes and commits those files after this process returns.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_CAPTURE_BYTES = 64 * 1024
HOST_TIMEOUT_SECONDS = 30 * 60
HOST_TIMEOUT_HEADROOM_SECONDS = 60
MAX_HARVEY_TIMEOUT_SECONDS = HOST_TIMEOUT_SECONDS - HOST_TIMEOUT_HEADROOM_SECONDS
DEFAULT_CONFIG: dict[str, Any] = {
    "lab_root": ".",
    "model": "anthropic/claude-sonnet-4-6",
    "task": "corporate-ma/review-data-room-red-flag-review",
    "max_turns": 200,
    "shell_timeout": 60,
    "timeout_seconds": MAX_HARVEY_TIMEOUT_SECONDS,
    "judge_model": "claude-sonnet-4-6",
    "dual": False,
}


class ConfigError(ValueError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clip_output(value: str | None) -> tuple[str, bool]:
    value = value or ""
    if len(value.encode("utf-8")) <= MAX_CAPTURE_BYTES:
        return value, False
    encoded = value.encode("utf-8")[-MAX_CAPTURE_BYTES:]
    return encoded.decode("utf-8", errors="replace"), True


def atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # The file replacement is still atomic on platforms without a
            # directory fsync operation.
            pass
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def atomic_json(path: Path, value: Any) -> None:
    atomic_bytes(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def copy_json_or_fallback(source: Path, target: Path, fallback: dict[str, Any]) -> tuple[Any | None, bool]:
    try:
        content = source.read_bytes()
        parsed = json.loads(content.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        atomic_json(target, fallback)
        return None, False
    atomic_bytes(target, content)
    return parsed, True


def required_string(config: dict[str, Any], key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    return value.strip()


def positive_int(config: dict[str, Any], key: str, default: int, maximum: int) -> int:
    value = config.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ConfigError(f"{key} must be an integer between 1 and {maximum}")
    return value


def load_config(root: Path, dry_run: bool) -> dict[str, Any]:
    config_path = root / ".borg" / "harvey-lab.json"
    if not config_path.is_file():
        if not dry_run:
            raise ConfigError(
                f"missing {config_path}; copy .borg/extensions/harvey-lab/harvey-lab.example.json"
            )
        config = dict(DEFAULT_CONFIG)
        config["lab_root"] = root
        config["runner"] = "uv"
        return config
    if not (root / ".borg").is_dir():
        raise ConfigError(f"project root does not contain .borg: {root}")
    try:
        loaded = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigError(f"invalid {config_path}: {error}") from error
    if not isinstance(loaded, dict):
        raise ConfigError("Harvey LAB configuration must be a JSON object")
    config = dict(DEFAULT_CONFIG)
    config.update(loaded)
    config["model"] = required_string(config, "model")
    config["task"] = required_string(config, "task")
    config["max_turns"] = positive_int(config, "max_turns", 200, 10_000)
    config["shell_timeout"] = positive_int(config, "shell_timeout", 60, 86_400)
    config["timeout_seconds"] = positive_int(
        config, "timeout_seconds", MAX_HARVEY_TIMEOUT_SECONDS, MAX_HARVEY_TIMEOUT_SECONDS
    )
    config["runner"] = required_string(config, "runner") if "runner" in config else "uv"
    config["judge_model"] = required_string(config, "judge_model")
    if not isinstance(config.get("dual", False), bool):
        raise ConfigError("dual must be a boolean")
    if config.get("reasoning_effort") is not None:
        config["reasoning_effort"] = required_string(config, "reasoning_effort")
    if config.get("skills") is not None:
        skills = config["skills"]
        if not isinstance(skills, list) or not all(isinstance(item, str) and item for item in skills):
            raise ConfigError("skills must be a list of non-empty strings")
    lab_root_value = config.get("lab_root")
    if not isinstance(lab_root_value, str) or not lab_root_value.strip():
        raise ConfigError("lab_root must be a non-empty string")
    lab_root = Path(lab_root_value).expanduser()
    if not lab_root.is_absolute():
        lab_root = root / lab_root
    config["lab_root"] = lab_root.resolve()
    if not dry_run and not config["lab_root"].is_dir():
        raise ConfigError(f"Harvey LAB checkout does not exist: {config['lab_root']}")
    return config


def workflow_uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError as error:
        raise ConfigError(f"workflow-id must be a UUID: {value}") from error


def result_directory(config: dict[str, Any], official_run_id: str) -> Path:
    results_root = Path(config["lab_root"]) / "results"
    candidate = (results_root / official_run_id).resolve()
    try:
        candidate.relative_to(results_root.resolve())
    except ValueError as error:
        raise ConfigError("official run_id escapes the Harvey results directory") from error
    return candidate


def run_command(config: dict[str, Any], official_run_id: str) -> list[str]:
    command = [
        str(config["runner"]),
        "run",
        "python",
        "-m",
        "harness.run",
        "--model",
        config["model"],
        "--task",
        config["task"],
        "--run-id",
        official_run_id,
        "--max-turns",
        str(config["max_turns"]),
        "--shell-timeout",
        str(config["shell_timeout"]),
    ]
    if config.get("reasoning_effort"):
        command.extend(["--reasoning-effort", config["reasoning_effort"]])
    if "temperature" in config:
        command.extend(["--temperature", str(config["temperature"])])
    if config.get("skills") is not None:
        command.append("--skills")
        command.extend(config["skills"])
    return command


def grade_command(config: dict[str, Any], official_run_id: str) -> list[str]:
    command = [
        str(config["runner"]),
        "run",
        "python",
        "-m",
        "evaluation.run_eval",
        "--run-id",
        official_run_id,
        "--task",
        config["task"],
        "--judge-model",
        config["judge_model"],
    ]
    if config.get("dual"):
        command.append("--dual")
    return command


def execute(command: list[str], cwd: Path, timeout_seconds: int, dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return {
            "status": "dry_run",
            "exit_code": None,
            "timed_out": False,
            "stdout": "",
            "stderr": "",
            "output_truncated": False,
        }
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        stdout, stdout_truncated = clip_output(error.stdout if isinstance(error.stdout, str) else None)
        stderr, stderr_truncated = clip_output(error.stderr if isinstance(error.stderr, str) else None)
        return {
            "status": "failed",
            "exit_code": None,
            "timed_out": True,
            "stdout": stdout,
            "stderr": stderr,
            "output_truncated": stdout_truncated or stderr_truncated,
            "error": f"command timed out after {timeout_seconds} seconds",
        }
    except OSError as error:
        return {
            "status": "failed",
            "exit_code": None,
            "timed_out": False,
            "stdout": "",
            "stderr": "",
            "output_truncated": False,
            "error": str(error),
        }
    stdout, stdout_truncated = clip_output(completed.stdout)
    stderr, stderr_truncated = clip_output(completed.stderr)
    return {
        "status": "succeeded" if completed.returncode == 0 else "failed",
        "exit_code": completed.returncode,
        "timed_out": False,
        "stdout": stdout,
        "stderr": stderr,
        "output_truncated": stdout_truncated or stderr_truncated,
    }


def local_record_path(root: Path, phase: str, workflow_id: str) -> Path:
    return root / ".borg" / "harvey-lab" / ("runs" if phase == "run" else "grades") / workflow_id


def run_phase(root: Path, workflow_id: str, dry_run: bool) -> int:
    config = load_config(root, dry_run)
    workflow_id = workflow_uuid(workflow_id)
    official_run_id = f"borg-{workflow_id}"
    command = run_command(config, official_run_id)
    started_at = utc_now()
    outcome = execute(command, Path(config["lab_root"]), config["timeout_seconds"], dry_run)
    finished_at = utc_now()
    results_dir = result_directory(config, official_run_id)
    record_dir = local_record_path(root, "run", workflow_id)
    metrics_path = results_dir / "metrics.json"
    metrics_fallback = {
        "schema_version": 1,
        "benchmark": "harvey-lab",
        "phase": "run",
        "workflow_id": workflow_id,
        "official_run_id": official_run_id,
        "status": outcome["status"],
        "official_metrics_missing": True,
    }
    official_metrics, official_metrics_present = copy_json_or_fallback(
        metrics_path, record_dir / "metrics.json", metrics_fallback
    )
    summary = {
        "schema_version": 1,
        "benchmark": "harvey-lab",
        "phase": "run",
        "workflow_id": workflow_id,
        "official_run_id": official_run_id,
        "task": config["task"],
        "model": config["model"],
        "max_turns": config["max_turns"],
        "command": command,
        "lab_root": str(config["lab_root"]),
        "official_results_dir": str(results_dir),
        "started_at": started_at,
        "finished_at": finished_at,
        "official_metrics": official_metrics,
        "official_metrics_present": official_metrics_present,
        **outcome,
    }
    atomic_json(record_dir / "run.json", summary)
    print(json.dumps({"phase": "run", "status": outcome["status"], "official_run_id": official_run_id}))
    return 0 if outcome["status"] in ("succeeded", "dry_run") else 1


def run_record_for(root: Path, official_run_id: str) -> dict[str, Any] | None:
    for path in (root / ".borg" / "harvey-lab" / "runs").glob("*/run.json"):
        value = read_json(path)
        if isinstance(value, dict) and value.get("official_run_id") == official_run_id:
            return value
    return None


def latest_record(root: Path) -> dict[str, Any]:
    records = []
    for path in (root / ".borg" / "harvey-lab" / "runs").glob("*/run.json"):
        value = read_json(path)
        if (
            isinstance(value, dict)
            and isinstance(value.get("official_run_id"), str)
            and value.get("status") == "succeeded"
        ):
            records.append((path.stat().st_mtime_ns, value))
    if not records:
        raise ConfigError("no Borg-recorded Harvey LAB run exists; set run_id in the config")
    return max(records, key=lambda item: item[0])[1]


def grade_phase(root: Path, workflow_id: str, dry_run: bool) -> int:
    config = load_config(root, dry_run)
    workflow_id = workflow_uuid(workflow_id)
    source_record: dict[str, Any] | None = None
    if config.get("run_id"):
        official_run_id = required_string(config, "run_id")
        if not dry_run:
            source_record = run_record_for(root, official_run_id)
            if source_record is None:
                raise ConfigError(f"configured run_id has no Borg-recorded run: {official_run_id}")
            if source_record.get("status") != "succeeded":
                raise ConfigError(f"configured run_id did not succeed: {official_run_id}")
        task = source_record.get("task", config["task"]) if source_record else config["task"]
    elif dry_run:
        official_run_id = "borg-dry-run-source"
        task = config["task"]
    else:
        source_record = latest_record(root)
        official_run_id = required_string(source_record, "official_run_id")
        task = source_record.get("task", config["task"])
        config["task"] = task
    command = grade_command(config, official_run_id)
    started_at = utc_now()
    outcome = execute(command, Path(config["lab_root"]), config["timeout_seconds"], dry_run)
    finished_at = utc_now()
    results_dir = result_directory(config, official_run_id)
    record_dir = local_record_path(root, "grade", workflow_id)
    scores_path = results_dir / "scores.json"
    scores_fallback = {
        "schema_version": 1,
        "benchmark": "harvey-lab",
        "phase": "grade",
        "workflow_id": workflow_id,
        "official_run_id": official_run_id,
        "task": task,
        "official_scores_missing": True,
    }
    official_scores, official_scores_present = copy_json_or_fallback(
        scores_path, record_dir / "scores.json", scores_fallback
    )
    summary = {
        "schema_version": 1,
        "benchmark": "harvey-lab",
        "phase": "grade",
        "workflow_id": workflow_id,
        "official_run_id": official_run_id,
        "task": task,
        "judge_model": config["judge_model"],
        "command": command,
        "lab_root": str(config["lab_root"]),
        "official_results_dir": str(results_dir),
        "source_run_workflow_id": source_record.get("workflow_id") if source_record else None,
        "source_official_run_id": official_run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "official_scores": official_scores,
        "official_scores_present": official_scores_present,
        **outcome,
    }
    atomic_json(record_dir / "grade.json", summary)
    print(json.dumps({"phase": "grade", "status": outcome["status"], "official_run_id": official_run_id}))
    return 0 if outcome["status"] in ("succeeded", "dry_run") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help=argparse.SUPPRESS)
    subparsers = parser.add_subparsers(dest="phase", required=True)
    for phase in ("run", "grade"):
        subparser = subparsers.add_parser(phase)
        subparser.add_argument("--workflow-id", required=True)
        subparser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.phase == "run":
            return run_phase(root, args.workflow_id, args.dry_run)
        return grade_phase(root, args.workflow_id, args.dry_run)
    except ConfigError as error:
        print(f"harvey-lab: {error}", file=sys.stderr)
        return 2
    except Exception as error:  # pragma: no cover - defensive workflow boundary
        print(f"harvey-lab: unexpected adapter failure: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
