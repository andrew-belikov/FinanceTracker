from __future__ import annotations

import os
import stat
import tempfile
import time
from pathlib import Path


DEBUG_PREFIX = "fintracker_debug_"


def _positive_int(name: str, default: int, maximum: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1 or value > maximum:
        raise ValueError(f"{name} is outside the allowed range")
    return value


def protected_debug_directory() -> Path:
    raw_path = os.getenv("REPORT_DEBUG_DIR", "").strip()
    if not raw_path:
        raise ValueError("REPORT_DEBUG_DIR is required when report debug artifacts are enabled")
    directory = Path(raw_path)
    if not directory.is_absolute():
        raise ValueError("REPORT_DEBUG_DIR must be an absolute path")
    if directory.exists() and directory.is_symlink():
        raise ValueError("REPORT_DEBUG_DIR must not be a symlink")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    if stat.S_IMODE(directory.stat().st_mode) != 0o700:
        raise ValueError("REPORT_DEBUG_DIR must have mode 0700")
    return directory


def cleanup_debug_artifacts(directory: Path, *, now: float | None = None) -> None:
    now = time.time() if now is None else now
    max_age = _positive_int("REPORT_DEBUG_MAX_AGE_SECONDS", 86400, 31 * 86400)
    retain = _positive_int("REPORT_DEBUG_MAX_FILES", 10, 100)
    files = sorted(
        (
            path
            for path in directory.iterdir()
            if path.is_file() and not path.is_symlink() and path.name.startswith(DEBUG_PREFIX)
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for index, path in enumerate(files):
        if now - path.stat().st_mtime > max_age or index >= retain:
            path.unlink(missing_ok=True)


def save_debug_text(*, kind: str, suffix: str, text: str) -> str:
    directory = protected_debug_directory()
    cleanup_debug_artifacts(directory)
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f"{DEBUG_PREFIX}{kind}_",
        suffix=suffix,
        dir=directory,
        text=True,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as file_obj:
            descriptor = -1
            file_obj.write(text)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    cleanup_debug_artifacts(directory)
    return raw_path
