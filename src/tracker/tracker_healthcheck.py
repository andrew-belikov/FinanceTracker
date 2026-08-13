from __future__ import annotations

import os
from pathlib import Path

from common.readiness_state import process_state, validate_ready_state


DEFAULT_READY_FILE = "/tmp/financetracker-tracker.ready"


def get_max_age_seconds() -> int:
    configured = os.getenv("TRACKER_READY_MAX_AGE_SECONDS", "").strip()
    if configured:
        return max(1, int(configured))
    if os.getenv("SNAPSHOT_MODE", "interval").strip().lower() == "cron":
        return 90_000
    interval_minutes = max(1, int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5")))
    return max(300, interval_minutes * 60 * 3)


def main(
    *,
    state_path: str | Path | None = None,
    proc_root: Path = Path("/proc"),
    now: float | None = None,
    max_age_seconds: int | None = None,
) -> int:
    try:
        command_line = (proc_root / "1" / "cmdline").read_bytes().replace(b"\0", b" ")
        owner_state = process_state(1, proc_root=proc_root)
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return 1
    if b"app.py" not in command_line or owner_state in {None, "Z", "X"}:
        return 1
    ready_path = state_path or os.getenv("TRACKER_READY_FILE", DEFAULT_READY_FILE)
    valid = validate_ready_state(
        ready_path,
        expected_pid=1,
        max_age_seconds=max_age_seconds or get_max_age_seconds(),
        now=now,
        proc_root=proc_root,
    )
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
