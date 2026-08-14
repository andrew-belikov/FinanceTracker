from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tempfile
import time


MAX_READY_STATE_BYTES = 4096


def _process_stat_fields(pid: int, *, proc_root: Path) -> list[str] | None:
    try:
        raw = (proc_root / str(pid) / "stat").read_text(encoding="utf-8")
        _prefix, separator, tail = raw.rpartition(") ")
        if not separator:
            return None
        return tail.split()
    except (FileNotFoundError, PermissionError, OSError):
        return None


def process_start_ticks(pid: int, *, proc_root: Path = Path("/proc")) -> str | None:
    fields = _process_stat_fields(pid, proc_root=proc_root)
    return fields[19] if fields is not None and len(fields) > 19 else None


def process_state(pid: int, *, proc_root: Path = Path("/proc")) -> str | None:
    fields = _process_stat_fields(pid, proc_root=proc_root)
    return fields[0] if fields else None


def write_ready_state(
    path: str | Path,
    *,
    pid: int | None = None,
    now: float | None = None,
    proc_root: Path = Path("/proc"),
) -> None:
    ready_path = Path(path)
    owner_pid = os.getpid() if pid is None else pid
    start_ticks = process_start_ticks(owner_pid, proc_root=proc_root)
    if start_ticks is None:
        raise RuntimeError("cannot resolve readiness owner process")

    payload = json.dumps(
        {
            "pid": owner_pid,
            "process_start_ticks": start_ticks,
            "updated_at": time.time() if now is None else now,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{ready_path.name}.",
        dir=ready_path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, ready_path)
        os.chmod(ready_path, 0o600)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def clear_ready_state(path: str | Path) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass


def validate_ready_state(
    path: str | Path,
    *,
    expected_pid: int,
    max_age_seconds: float,
    now: float | None = None,
    proc_root: Path = Path("/proc"),
) -> bool:
    ready_path = Path(path)
    try:
        metadata = ready_path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
            return False
        if metadata.st_size <= 0 or metadata.st_size > MAX_READY_STATE_BYTES:
            return False
        payload = json.loads(ready_path.read_text(encoding="utf-8"))
        owner_pid = int(payload["pid"])
        updated_at = float(payload["updated_at"])
        recorded_start_ticks = str(payload["process_start_ticks"])
    except (FileNotFoundError, PermissionError, OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return False

    current_time = time.time() if now is None else now
    age_seconds = current_time - updated_at
    if owner_pid != expected_pid or age_seconds < -5 or age_seconds > max_age_seconds:
        return False
    live_start_ticks = process_start_ticks(expected_pid, proc_root=proc_root)
    return live_start_ticks is not None and live_start_ticks == recorded_start_ticks
