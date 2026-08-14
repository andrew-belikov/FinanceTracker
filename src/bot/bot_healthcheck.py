from __future__ import annotations

import os
from pathlib import Path

from common.readiness_state import validate_ready_state


DEFAULT_READY_FILE = "/tmp/financetracker-bot.ready"


def iter_process_command_lines(proc_root: Path = Path("/proc")) -> list[str]:
    command_lines: list[str] = []
    for cmdline_path in proc_root.glob("[0-9]*/cmdline"):
        try:
            raw = cmdline_path.read_bytes()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        command_lines.append(raw.replace(b"\0", b" ").decode("utf-8", errors="replace"))
    return command_lines


def main(
    *,
    state_path: str | Path | None = None,
    proc_root: Path = Path("/proc"),
    now: float | None = None,
    max_age_seconds: int | None = None,
) -> int:
    try:
        owner_command = (proc_root / "1" / "cmdline").read_bytes().replace(b"\0", b" ")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return 1
    if b"entrypoint.py" not in owner_command:
        return 1
    ready_path = state_path or os.getenv("BOT_READY_FILE", DEFAULT_READY_FILE)
    valid = validate_ready_state(
        ready_path,
        expected_pid=1,
        max_age_seconds=max_age_seconds
        or max(1, int(os.getenv("BOT_READY_MAX_AGE_SECONDS", "120"))),
        now=now,
        proc_root=proc_root,
    )
    if not valid:
        return 1
    return 0 if any(
        " bot.py" in f" {command_line}"
        for command_line in iter_process_command_lines(proc_root)
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
