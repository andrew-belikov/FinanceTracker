from __future__ import annotations

from pathlib import Path


def main() -> int:
    try:
        command_line = Path("/proc/1/cmdline").read_bytes().replace(b"\0", b" ")
        process_state = Path("/proc/1/stat").read_text(encoding="utf-8").split()[2]
    except (FileNotFoundError, PermissionError, IndexError, ProcessLookupError):
        return 1
    return 0 if b"app.py" in command_line and process_state not in {"Z", "X"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
