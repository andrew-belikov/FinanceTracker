from __future__ import annotations

from pathlib import Path


def iter_process_command_lines() -> list[str]:
    command_lines: list[str] = []
    for cmdline_path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            raw = cmdline_path.read_bytes()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        command_lines.append(raw.replace(b"\0", b" ").decode("utf-8", errors="replace"))
    return command_lines


def main() -> int:
    return 0 if any(" bot.py" in f" {command_line}" for command_line in iter_process_command_lines()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
